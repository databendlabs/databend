// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::collections::HashSet;

use databend_common_exception::Result;
use databend_common_expression::RemoteExpr;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::Join;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::IndexType;
use databend_common_sql::MetadataRef;

use super::utils::scalar_to_remote_expr;
use super::utils::supported_join_type_for_runtime_filter;

#[derive(Clone, Debug)]
pub struct RuntimeFilterTarget {
    pub column_idx: IndexType,
    pub scan_id: usize,
    pub expr: RemoteExpr<String>,
}

#[derive(Clone)]
struct RuntimeFilterColumn {
    column_idx: IndexType,
    scan_id: usize,
    expr: RemoteExpr<String>,
}

#[derive(Clone)]
struct ScanInfo {
    ancestors: Vec<AncestorInfo>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum JoinSide {
    Probe,
    Build,
}

type JoinPtr = usize;

#[derive(Clone, Copy)]
struct AncestorInfo {
    join_ptr: JoinPtr,
    side: JoinSide,
}

struct JoinNodeInfo {
    parent: Option<(JoinPtr, JoinSide)>,
    supported: bool,
}

pub struct RuntimeFilterRouting {
    join_nodes: HashMap<JoinPtr, JoinNodeInfo>,
    scan_infos: HashMap<usize, ScanInfo>,
    column_classes: HashMap<IndexType, IndexType>,
    class_members: HashMap<IndexType, Vec<RuntimeFilterColumn>>,
}

impl RuntimeFilterRouting {
    pub fn build(metadata: &MetadataRef, root: &SExpr) -> Result<Self> {
        let mut builder = RuntimeFilterRoutingBuilder::new(metadata.clone());
        builder.collect(root, None, &mut Vec::new())?;
        builder.finish()
    }

    pub fn find_targets(
        &self,
        join_s_expr: &SExpr,
        column_idx: IndexType,
    ) -> Result<Vec<RuntimeFilterTarget>> {
        let join_ptr = join_s_expr as *const SExpr as JoinPtr;
        let candidate_joins = self.build_chain(join_ptr);
        let class_id = self.column_classes.get(&column_idx).copied().unwrap();

        let mut result = Vec::new();
        if let Some(columns) = self.class_members.get(&class_id) {
            for column in columns {
                if self.scan_in_probe_subtree(column.scan_id, &candidate_joins) {
                    result.push(RuntimeFilterTarget {
                        column_idx: column.column_idx,
                        scan_id: column.scan_id,
                        expr: column.expr.clone(),
                    });
                }
            }
        }

        Ok(result)
    }

    fn scan_in_probe_subtree(&self, scan_id: usize, candidate_set: &HashSet<JoinPtr>) -> bool {
        let Some(info) = self.scan_infos.get(&scan_id) else {
            return false;
        };

        info.ancestors.iter().any(|ancestor| {
            ancestor.side == JoinSide::Probe && candidate_set.contains(&ancestor.join_ptr)
        })
    }

    fn build_chain(&self, join_ptr: JoinPtr) -> HashSet<JoinPtr> {
        let mut chain = HashSet::new();
        let mut current = Some(join_ptr);
        while let Some(ptr) = current {
            chain.insert(ptr);
            let Some(node) = self.join_nodes.get(&ptr) else {
                break;
            };
            match node.parent {
                Some((parent_ptr, JoinSide::Build)) => {
                    if let Some(parent_node) = self.join_nodes.get(&parent_ptr) {
                        if parent_node.supported {
                            current = Some(parent_ptr);
                            continue;
                        }
                    }
                    break;
                }
                _ => break,
            }
        }
        chain
    }
}

struct RuntimeFilterRoutingBuilder {
    metadata: MetadataRef,
    join_nodes: HashMap<JoinPtr, JoinNodeInfo>,
    scan_infos: HashMap<usize, ScanInfo>,
    columns: HashMap<IndexType, RuntimeFilterColumn>,
    union_find: UnionFind,
}

impl RuntimeFilterRoutingBuilder {
    fn new(metadata: MetadataRef) -> Self {
        Self {
            metadata,
            join_nodes: HashMap::new(),
            scan_infos: HashMap::new(),
            columns: HashMap::new(),
            union_find: UnionFind::new(),
        }
    }

    fn collect(
        &mut self,
        s_expr: &SExpr,
        parent: Option<(JoinPtr, JoinSide)>,
        ancestors: &mut Vec<AncestorInfo>,
    ) -> Result<()> {
        match s_expr.plan() {
            RelOperator::Join(join) => {
                let ptr = s_expr as *const SExpr as JoinPtr;
                let supported = supported_join_type_for_runtime_filter(&join.join_type);
                self.join_nodes
                    .insert(ptr, JoinNodeInfo { parent, supported });
                self.process_join(join)?;

                let probe_child = s_expr.child(0)?;
                ancestors.push(AncestorInfo {
                    join_ptr: ptr,
                    side: JoinSide::Probe,
                });
                self.collect(probe_child, Some((ptr, JoinSide::Probe)), ancestors)?;
                ancestors.pop();

                let build_child = s_expr.child(1)?;
                ancestors.push(AncestorInfo {
                    join_ptr: ptr,
                    side: JoinSide::Build,
                });
                self.collect(build_child, Some((ptr, JoinSide::Build)), ancestors)?;
                ancestors.pop();
            }
            RelOperator::Scan(scan) => {
                self.scan_infos.insert(scan.scan_id, ScanInfo {
                    ancestors: ancestors.clone(),
                });
            }
            _ => {
                for child in s_expr.children() {
                    self.collect(child, parent, ancestors)?;
                }
            }
        }
        Ok(())
    }

    fn process_join(&mut self, join: &Join) -> Result<()> {
        for condition in join.equi_conditions.iter() {
            let left = scalar_to_remote_expr(&self.metadata, &condition.left)?;
            let right = scalar_to_remote_expr(&self.metadata, &condition.right)?;
            match (left, right) {
                (Some(left), Some(right)) => {
                    self.union_find.union(left.2, right.2);
                    self.insert_column(left);
                    self.insert_column(right);
                }
                (Some(column), None) | (None, Some(column)) => {
                    self.insert_column(column);
                }
                (None, None) => {}
            };
        }
        Ok(())
    }

    fn insert_column(
        &mut self,
        (expr, scan_id, column_idx): (RemoteExpr<String>, usize, IndexType),
    ) {
        self.columns.insert(column_idx, RuntimeFilterColumn {
            column_idx,
            scan_id,
            expr,
        });
    }

    fn finish(mut self) -> Result<RuntimeFilterRouting> {
        let mut column_classes = HashMap::new();
        let mut class_members: HashMap<IndexType, Vec<RuntimeFilterColumn>> = HashMap::new();
        for (column_idx, column) in self.columns.iter() {
            let class_id = self.union_find.find(*column_idx);
            column_classes.insert(*column_idx, class_id);
            class_members
                .entry(class_id)
                .or_default()
                .push(column.clone());
        }

        Ok(RuntimeFilterRouting {
            join_nodes: self.join_nodes,
            scan_infos: self.scan_infos,
            column_classes,
            class_members,
        })
    }
}

struct UnionFind {
    parent: HashMap<IndexType, IndexType>,
}

impl UnionFind {
    fn new() -> Self {
        Self {
            parent: HashMap::new(),
        }
    }

    fn find(&mut self, x: IndexType) -> IndexType {
        if !self.parent.contains_key(&x) {
            self.parent.insert(x, x);
            return x;
        }

        let parent = *self.parent.get(&x).unwrap();
        if parent != x {
            let root = self.find(parent);
            self.parent.insert(x, root);
        }
        *self.parent.get(&x).unwrap()
    }

    fn union(&mut self, x: IndexType, y: IndexType) {
        let root_x = self.find(x);
        let root_y = self.find(y);
        if root_x != root_y {
            self.parent.insert(root_x, root_y);
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_ast::Span;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_sql::plans::DummyTableScan;
    use databend_common_sql::plans::RelOperator;

    use super::*;

    fn dummy_expr(name: &str) -> RemoteExpr<String> {
        RemoteExpr::ColumnRef {
            span: Span::default(),
            id: name.to_string(),
            data_type: DataType::Number(NumberDataType::Int32),
            display_name: name.to_string(),
        }
    }

    #[test]
    fn test_find_targets_through_ancestors() {
        let join_expr = SExpr::create_leaf(RelOperator::DummyTableScan(DummyTableScan));
        let parent_expr = SExpr::create_leaf(RelOperator::DummyTableScan(DummyTableScan));
        let join_ptr = &join_expr as *const SExpr as JoinPtr;
        let parent_ptr = &parent_expr as *const SExpr as JoinPtr;

        let mut routing = RuntimeFilterRouting {
            join_nodes: HashMap::new(),
            scan_infos: HashMap::new(),
            column_classes: HashMap::new(),
            class_members: HashMap::new(),
        };

        routing.join_nodes.insert(parent_ptr, JoinNodeInfo {
            parent: None,
            supported: true,
        });
        routing.join_nodes.insert(join_ptr, JoinNodeInfo {
            parent: Some((parent_ptr, JoinSide::Build)),
            supported: true,
        });

        routing.scan_infos.insert(10, ScanInfo {
            ancestors: vec![AncestorInfo {
                join_ptr,
                side: JoinSide::Probe,
            }],
        });
        routing.scan_infos.insert(20, ScanInfo {
            ancestors: vec![AncestorInfo {
                join_ptr: parent_ptr,
                side: JoinSide::Probe,
            }],
        });
        routing.scan_infos.insert(30, ScanInfo {
            ancestors: vec![AncestorInfo {
                join_ptr,
                side: JoinSide::Build,
            }],
        });

        routing.column_classes.insert(1, 100);
        routing.column_classes.insert(2, 100);
        routing.column_classes.insert(3, 100);
        routing.class_members.insert(100, vec![
            RuntimeFilterColumn {
                column_idx: 1,
                scan_id: 10,
                expr: dummy_expr("c1"),
            },
            RuntimeFilterColumn {
                column_idx: 2,
                scan_id: 20,
                expr: dummy_expr("c2"),
            },
            RuntimeFilterColumn {
                column_idx: 3,
                scan_id: 30,
                expr: dummy_expr("c3"),
            },
        ]);

        let mut targets = routing.find_targets(&join_expr, 1).unwrap();
        targets.sort_by_key(|t| t.scan_id);
        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].scan_id, 10);
        assert_eq!(targets[1].scan_id, 20);

        let none = routing.find_targets(&join_expr, 99).unwrap();
        assert!(none.is_empty());
    }
}
