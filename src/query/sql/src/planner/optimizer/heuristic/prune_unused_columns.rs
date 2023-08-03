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
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use itertools::Itertools;

use crate::optimizer::ColumnSet;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::CteScan;
use crate::plans::DummyTableScan;
use crate::plans::EvalScalar;
use crate::plans::Lambda;
use crate::plans::MaterializedCte;
use crate::plans::ProjectSet;
use crate::plans::RelOperator;
use crate::ColumnBinding;
use crate::ColumnEntry;
use crate::IndexType;
use crate::MetadataRef;

pub struct UnusedColumnPruner {
    metadata: MetadataRef,

    /// If exclude lazy columns
    apply_lazy: bool,
    /// Record cte_idx and the cte's output columns
    cte_output_columns: HashMap<IndexType, Vec<ColumnBinding>>,
}

impl UnusedColumnPruner {
    pub fn new(metadata: MetadataRef, apply_lazy: bool) -> Self {
        Self {
            metadata,
            apply_lazy,
            cte_output_columns: Default::default(),
        }
    }

    pub fn remove_unused_columns(
        &mut self,
        expr: &SExpr,
        require_columns: ColumnSet,
    ) -> Result<SExpr> {
        let mut s_expr = self.keep_required_columns(expr, require_columns)?;
        s_expr.applied_rules = expr.applied_rules.clone();
        Ok(s_expr)
    }

    /// Keep columns referenced by parent plan node.
    /// `required` contains columns referenced by its ancestors. When a node has multiple children,
    /// the required columns for each child could be different and we may include columns not needed
    /// by a specific child. Columns should be skipped once we found it not exist in the subtree as we
    /// visit a plan node.
    fn keep_required_columns(&mut self, expr: &SExpr, mut required: ColumnSet) -> Result<SExpr> {
        match expr.plan() {
            RelOperator::Scan(p) => {
                // add virtual columns to scan
                let mut virtual_columns = ColumnSet::new();
                for column in self
                    .metadata
                    .read()
                    .virtual_columns_by_table_index(p.table_index)
                    .iter()
                {
                    match column {
                        ColumnEntry::VirtualColumn(virtual_column) => {
                            virtual_columns.insert(virtual_column.column_index);
                        }
                        _ => unreachable!(),
                    }
                }

                // Some table may not have any column,
                // e.g. `system.sync_crash_me`
                if p.columns.is_empty() && virtual_columns.is_empty() {
                    return Ok(expr.clone());
                }
                let columns = p.columns.union(&virtual_columns).cloned().collect();
                let mut prewhere = p.prewhere.clone();
                let mut used: ColumnSet = required.intersection(&columns).cloned().collect();
                if let Some(ref mut pw) = prewhere {
                    debug_assert!(
                        pw.prewhere_columns.is_subset(&columns),
                        "prewhere columns should be a subset of scan columns"
                    );
                    pw.output_columns = used.clone();
                    // `prune_columns` is after `prewhere_optimize`,
                    // so we need to add prewhere columns to scan columns.
                    used = used.union(&pw.prewhere_columns).cloned().collect();
                }

                Ok(SExpr::create_leaf(Arc::new(RelOperator::Scan(
                    p.prune_columns(used, prewhere),
                ))))
            }
            RelOperator::Join(p) => {
                // Include columns referenced in left conditions
                let left = p.left_conditions.iter().fold(required.clone(), |acc, v| {
                    acc.union(&v.used_columns()).cloned().collect()
                });
                // Include columns referenced in right conditions
                let right = p.right_conditions.iter().fold(required.clone(), |acc, v| {
                    acc.union(&v.used_columns()).cloned().collect()
                });

                let others = p.non_equi_conditions.iter().fold(required, |acc, v| {
                    acc.union(&v.used_columns()).cloned().collect()
                });

                Ok(SExpr::create_binary(
                    Arc::new(RelOperator::Join(p.clone())),
                    Arc::new(self.keep_required_columns(
                        expr.child(0)?,
                        left.union(&others).cloned().collect(),
                    )?),
                    Arc::new(self.keep_required_columns(
                        expr.child(1)?,
                        right.union(&others).cloned().collect(),
                    )?),
                ))
            }

            RelOperator::EvalScalar(p) => {
                let mut used = vec![];
                // Only keep columns needed by parent plan.
                for s in p.items.iter() {
                    if !required.contains(&s.index) {
                        continue;
                    }
                    used.push(s.clone());
                    s.scalar.used_columns().iter().for_each(|c| {
                        required.insert(*c);
                    })
                }
                if used.is_empty() {
                    // Eliminate unnecessary `EvalScalar`
                    self.keep_required_columns(expr.child(0)?, required)
                } else {
                    Ok(SExpr::create_unary(
                        Arc::new(RelOperator::EvalScalar(EvalScalar { items: used })),
                        Arc::new(self.keep_required_columns(expr.child(0)?, required)?),
                    ))
                }
            }
            RelOperator::Filter(p) => {
                let used = p.predicates.iter().fold(required, |acc, v| {
                    acc.union(&v.used_columns()).cloned().collect()
                });
                Ok(SExpr::create_unary(
                    Arc::new(RelOperator::Filter(p.clone())),
                    Arc::new(self.keep_required_columns(expr.child(0)?, used)?),
                ))
            }
            RelOperator::Aggregate(p) => {
                let mut used = vec![];
                for item in &p.aggregate_functions {
                    if required.contains(&item.index) {
                        required.extend(item.scalar.used_columns());
                        used.push(item.clone());
                    }
                }

                p.group_items.iter().for_each(|i| {
                    // If the group item comes from a complex expression, we only include the final
                    // column index here. The used columns will be included in its EvalScalar child.
                    required.insert(i.index);
                });

                // If the aggregate is empty, we remove the aggregate operator and replace it with
                // a DummyTableScan which returns exactly one row.
                if p.group_items.is_empty() && used.is_empty() {
                    Ok(SExpr::create_leaf(Arc::new(RelOperator::DummyTableScan(
                        DummyTableScan,
                    ))))
                } else {
                    Ok(SExpr::create_unary(
                        Arc::new(RelOperator::Aggregate(Aggregate {
                            group_items: p.group_items.clone(),
                            aggregate_functions: used,
                            from_distinct: p.from_distinct,
                            mode: p.mode,
                            limit: p.limit,
                            grouping_id_index: p.grouping_id_index,
                            grouping_sets: p.grouping_sets.clone(),
                        })),
                        Arc::new(self.keep_required_columns(expr.child(0)?, required)?),
                    ))
                }
            }
            RelOperator::Window(p) => {
                if required.contains(&p.index) {
                    // The scalar items in window function is not replaced yet.
                    // The will be replaced in physical plan builder.
                    p.arguments.iter().for_each(|item| {
                        required.extend(item.scalar.used_columns());
                    });
                    p.partition_by.iter().for_each(|item| {
                        required.extend(item.scalar.used_columns());
                    });
                    p.order_by.iter().for_each(|item| {
                        required.extend(item.order_by_item.scalar.used_columns());
                    });
                }

                Ok(SExpr::create_unary(
                    Arc::new(RelOperator::Window(p.clone())),
                    Arc::new(self.keep_required_columns(expr.child(0)?, required)?),
                ))
            }
            RelOperator::Sort(p) => {
                let mut p = p.clone();
                p.items.iter().for_each(|s| {
                    required.insert(s.index);
                });

                // If the query will be optimized by lazy reading, we don't need to do pre-projection.
                if self.metadata.read().lazy_columns().is_empty() {
                    p.pre_projection = Some(required.iter().sorted().copied().collect());
                }

                Ok(SExpr::create_unary(
                    Arc::new(RelOperator::Sort(p)),
                    Arc::new(self.keep_required_columns(expr.child(0)?, required)?),
                ))
            }
            RelOperator::Limit(p) => {
                if self.apply_lazy {
                    let metadata = self.metadata.read().clone();
                    let lazy_columns = metadata.lazy_columns();
                    required = required
                        .difference(lazy_columns)
                        .cloned()
                        .collect::<ColumnSet>();
                    required.extend(metadata.row_id_indexes());
                }

                Ok(SExpr::create_unary(
                    Arc::new(RelOperator::Limit(p.clone())),
                    Arc::new(self.keep_required_columns(expr.child(0)?, required)?),
                ))
            }
            RelOperator::UnionAll(p) => {
                let left_used = p.pairs.iter().fold(required.clone(), |mut acc, v| {
                    acc.insert(v.0);
                    acc
                });
                let right_used = p.pairs.iter().fold(required, |mut acc, v| {
                    acc.insert(v.1);
                    acc
                });
                Ok(SExpr::create_binary(
                    Arc::new(RelOperator::UnionAll(p.clone())),
                    Arc::new(self.keep_required_columns(expr.child(0)?, left_used)?),
                    Arc::new(self.keep_required_columns(expr.child(1)?, right_used)?),
                ))
            }

            RelOperator::ProjectSet(op) => {
                let parent_required = required.clone();
                for s in op.srfs.iter() {
                    required.extend(s.scalar.used_columns().iter().copied());
                }
                // Columns that are not used by the parent plan don't need to be kept.
                // Repeating the rows to as many as the results of SRF may result in an OOM.
                let unused = required.difference(&parent_required);
                let unused_columns = unused.into_iter().copied().collect::<Vec<_>>();
                let project_set = ProjectSet {
                    srfs: op.srfs.clone(),
                    unused_columns: Some(unused_columns),
                };

                Ok(SExpr::create_unary(
                    Arc::new(RelOperator::ProjectSet(project_set)),
                    Arc::new(self.keep_required_columns(expr.child(0)?, required)?),
                ))
            }

            RelOperator::CteScan(scan) => {
                let mut used_columns = scan.used_columns()?;
                used_columns = required.intersection(&used_columns).cloned().collect();
                let mut pruned_fields = vec![];
                let mut pruned_offsets = vec![];
                let cte_output_columns = self.cte_output_columns.get(&scan.cte_idx.0).unwrap();
                for field in scan.fields.iter() {
                    if used_columns.contains(&field.name().parse()?) {
                        pruned_fields.push(field.clone());
                    }
                }
                for field in pruned_fields.iter() {
                    for (offset, col) in cte_output_columns.iter().enumerate() {
                        if col.index.eq(&field.name().parse::<IndexType>()?) {
                            pruned_offsets.push(offset);
                            break;
                        }
                    }
                }
                Ok(SExpr::create_leaf(Arc::new(RelOperator::CteScan(
                    CteScan {
                        cte_idx: scan.cte_idx,
                        fields: pruned_fields,
                        offsets: pruned_offsets,
                        stat: scan.stat.clone(),
                    },
                ))))
            }

            RelOperator::MaterializedCte(cte) => {
                if self.apply_lazy {
                    return Ok(expr.clone());
                }
                let left_output_column = RelExpr::with_s_expr(expr)
                    .derive_relational_prop_child(0)?
                    .output_columns
                    .clone();
                let right_used_column = RelExpr::with_s_expr(expr)
                    .derive_relational_prop_child(1)?
                    .used_columns
                    .clone();
                // Get the intersection of `left_used_column` and `right_used_column`
                let left_required = left_output_column
                    .intersection(&right_used_column)
                    .cloned()
                    .collect::<ColumnSet>();

                let mut required_output_columns = vec![];
                for column in cte.left_output_columns.iter() {
                    if left_required.contains(&column.index) {
                        required_output_columns.push(column.clone());
                    }
                }
                self.cte_output_columns
                    .insert(cte.cte_idx, required_output_columns.clone());
                Ok(SExpr::create_binary(
                    Arc::new(RelOperator::MaterializedCte(MaterializedCte {
                        left_output_columns: required_output_columns,
                        cte_idx: cte.cte_idx,
                    })),
                    Arc::new(self.keep_required_columns(expr.child(0)?, left_required)?),
                    Arc::new(self.keep_required_columns(expr.child(1)?, required)?),
                ))
            }

            RelOperator::Lambda(p) => {
                let mut used = vec![];
                // Keep all columns, as some lambda functions may be arguments to other lambda functions.
                for s in p.items.iter() {
                    used.push(s.clone());
                    s.scalar.used_columns().iter().for_each(|c| {
                        required.insert(*c);
                    })
                }
                if used.is_empty() {
                    self.keep_required_columns(expr.child(0)?, required)
                } else {
                    Ok(SExpr::create_unary(
                        Arc::new(RelOperator::Lambda(Lambda { items: used })),
                        Arc::new(self.keep_required_columns(expr.child(0)?, required)?),
                    ))
                }
            }

            RelOperator::DummyTableScan(_) => Ok(expr.clone()),

            _ => Err(ErrorCode::Internal(
                "Attempting to prune columns of a physical plan is not allowed",
            )),
        }
    }
}
