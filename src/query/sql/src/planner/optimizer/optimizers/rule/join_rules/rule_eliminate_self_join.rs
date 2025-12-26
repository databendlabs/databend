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
use std::sync::Arc;

use databend_common_exception::Result;

use crate::ColumnEntry;
use crate::Metadata;
use crate::MetadataRef;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::JoinEquiCondition;
use crate::plans::JoinType;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::Scan;

#[derive(Clone, Debug)]
struct RelationWithPath {
    expr: Arc<SExpr>,
    /// Join-tree path from the current inner-join root to this relation.
    path: Vec<usize>,
}

#[derive(Clone, Debug)]
struct MultiJoin {
    relations: Vec<RelationWithPath>,
    equi_conditions: Vec<JoinEquiCondition>,
    non_equi_conditions: Vec<ScalarExpr>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct GroupKeyItemSignature {
    column_id: Option<u32>,
    column_position: Option<usize>,
    column_name: String,
}

#[derive(Clone, Debug)]
struct Candidate {
    table_id: u64,
    group_key: Vec<GroupKeyItemSignature>,
    strict: bool,
    path: Vec<usize>,
}

pub struct RuleEliminateSelfJoin {
    id: RuleID,
    matchers: Vec<Matcher>,
    metadata: MetadataRef,
}

impl RuleEliminateSelfJoin {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::EliminateSelfJoin,
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Join,
                children: vec![Matcher::Leaf, Matcher::Leaf],
            }],
            metadata,
        }
    }

    fn core_candidate_matcher(&self) -> Matcher {
        Matcher::MatchOp {
            op_type: RelOp::Aggregate,
            children: vec![Matcher::MatchOp {
                op_type: RelOp::Aggregate,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::EvalScalar,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Scan,
                        children: vec![],
                    }],
                }],
            }],
        }
    }

    fn extract_multi_join(&self, s_expr: &SExpr) -> Result<Option<MultiJoin>> {
        let mut path = Vec::new();
        self.extract_multi_join_with_path(s_expr, &mut path)
    }

    fn extract_multi_join_with_path(
        &self,
        s_expr: &SExpr,
        current_path: &mut Vec<usize>,
    ) -> Result<Option<MultiJoin>> {
        match s_expr.plan() {
            RelOperator::Join(join) if matches!(join.join_type, JoinType::Inner) => {
                let mut equi_conditions = join.equi_conditions.clone();
                let mut non_equi_conditions = join.non_equi_conditions.clone();
                let mut relations = Vec::new();

                current_path.push(0);
                match self.extract_multi_join_with_path(s_expr.child(0)?, current_path)? {
                    Some(mut left) => {
                        relations.append(&mut left.relations);
                        equi_conditions.append(&mut left.equi_conditions);
                        non_equi_conditions.append(&mut left.non_equi_conditions);
                    }
                    None => {
                        relations.push(RelationWithPath {
                            expr: Arc::new(s_expr.child(0)?.clone()),
                            path: current_path.clone(),
                        });
                    }
                }
                current_path.pop();

                current_path.push(1);
                match self.extract_multi_join_with_path(s_expr.child(1)?, current_path)? {
                    Some(mut right) => {
                        relations.append(&mut right.relations);
                        equi_conditions.append(&mut right.equi_conditions);
                        non_equi_conditions.append(&mut right.non_equi_conditions);
                    }
                    None => {
                        relations.push(RelationWithPath {
                            expr: Arc::new(s_expr.child(1)?.clone()),
                            path: current_path.clone(),
                        });
                    }
                }
                current_path.pop();

                Ok(Some(MultiJoin {
                    relations,
                    equi_conditions,
                    non_equi_conditions,
                }))
            }
            _ => Ok(None),
        }
    }

    fn eliminate_multi_join(&self, multi_join: MultiJoin, original: &SExpr) -> Result<SExpr> {
        let metadata = self.metadata.read();

        let mut candidates = Vec::new();
        for relation in multi_join.relations.iter() {
            if let Some(candidate) =
                self.try_parse_candidate(&relation.expr, &relation.path, &metadata)
            {
                candidates.push(candidate);
            }
        }

        let mut groups: HashMap<(u64, Vec<GroupKeyItemSignature>), Vec<Candidate>> = HashMap::new();
        for candidate in candidates.into_iter() {
            groups
                .entry((candidate.table_id, candidate.group_key.clone()))
                .or_default()
                .push(candidate);
        }

        let mut remove_paths: HashSet<Vec<usize>> = HashSet::new();
        for (_key, group_candidates) in groups.iter() {
            let has_strict = group_candidates.iter().any(|c| c.strict);
            let has_loose = group_candidates.iter().any(|c| !c.strict);
            if !has_strict || !has_loose {
                continue;
            }

            // If there is at least one strict candidate, loose candidates are redundant:
            // both sides are INNER JOIN on unique group keys (Aggregate), so the join acts
            // as a filter, and the strict side already filters stronger than the loose side.
            for c in group_candidates.iter().filter(|c| !c.strict) {
                remove_paths.insert(c.path.clone());
            }
        }

        if remove_paths.is_empty() {
            return Ok(original.clone());
        }

        let mut path = Vec::new();
        let rewritten = self
            .remove_paths_from_inner_join_tree(original, &mut path, &remove_paths)?
            .unwrap_or_else(|| original.clone());

        Ok(rewritten)
    }

    fn try_parse_candidate(
        &self,
        relation: &SExpr,
        path: &[usize],
        metadata: &Metadata,
    ) -> Option<Candidate> {
        let mut strict = false;
        let mut node = relation;

        while matches!(node.plan(), RelOperator::EvalScalar(_)) {
            node = node.child(0).ok()?;
        }

        if matches!(node.plan(), RelOperator::Filter(_)) {
            strict = true;
            node = node.child(0).ok()?;
        }

        if !self.core_candidate_matcher().matches(node) {
            return None;
        }

        let final_agg = match node.plan() {
            RelOperator::Aggregate(agg) => agg,
            _ => return None,
        };

        if final_agg.group_items.is_empty() {
            return None;
        }

        let group_key = Self::group_key_signature(&final_agg.group_items, metadata)?;

        let scan = match node.child(0).ok()?.child(0).ok()?.child(0).ok()?.plan() {
            RelOperator::Scan(scan) => scan,
            _ => return None,
        };

        let table_id = self.table_id_from_scan(scan, metadata)?;

        Some(Candidate {
            table_id,
            group_key,
            strict,
            path: path.to_vec(),
        })
    }

    fn group_key_signature(
        group_items: &[crate::plans::ScalarItem],
        metadata: &Metadata,
    ) -> Option<Vec<GroupKeyItemSignature>> {
        let mut sig = Vec::with_capacity(group_items.len());
        for item in group_items.iter() {
            let used = item.scalar.used_columns();
            if used.len() != 1 {
                return None;
            }
            let col_idx = *used.iter().next()?;
            let ColumnEntry::BaseTableColumn(base_col) = metadata.column(col_idx) else {
                return None;
            };

            sig.push(GroupKeyItemSignature {
                column_id: base_col.column_id,
                column_position: base_col.column_position,
                column_name: base_col.column_name.clone(),
            });
        }
        Some(sig)
    }

    fn table_id_from_scan(&self, scan: &Scan, metadata: &Metadata) -> Option<u64> {
        let table_entry = metadata.table(scan.table_index);
        Some(table_entry.table().get_table_info().ident.table_id)
    }

    fn remove_paths_from_inner_join_tree(
        &self,
        s_expr: &SExpr,
        current_path: &mut Vec<usize>,
        remove_paths: &HashSet<Vec<usize>>,
    ) -> Result<Option<SExpr>> {
        if remove_paths.contains(current_path) {
            return Ok(None);
        }

        match s_expr.plan() {
            RelOperator::Join(join) if matches!(join.join_type, JoinType::Inner) => {
                current_path.push(0);
                let left = self.remove_paths_from_inner_join_tree(
                    s_expr.child(0)?,
                    current_path,
                    remove_paths,
                )?;
                current_path.pop();

                current_path.push(1);
                let right = self.remove_paths_from_inner_join_tree(
                    s_expr.child(1)?,
                    current_path,
                    remove_paths,
                )?;
                current_path.pop();

                match (left, right) {
                    (None, None) => Ok(None),
                    (Some(keep), None) | (None, Some(keep)) => Ok(Some(keep)),
                    (Some(left), Some(right)) => {
                        Ok(Some(SExpr::create_binary(s_expr.plan.clone(), left, right)))
                    }
                }
            }
            _ => Ok(Some(s_expr.clone())),
        }
    }
}

impl Rule for RuleEliminateSelfJoin {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        if let Some(multi_join) = self.extract_multi_join(s_expr)? {
            let result = self.eliminate_multi_join(multi_join, s_expr)?;
            if result.ne(s_expr) {
                let mut result = result;
                result.set_applied_rule(&self.id);
                state.add_result(result);
            }
        }

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
