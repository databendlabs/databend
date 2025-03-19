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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::binder::ColumnBindingBuilder;
use crate::binder::Visibility;
use crate::optimizer::decorrelate::subquery_rewriter::FlattenInfo;
use crate::optimizer::ColumnSet;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::optimizer::SubqueryRewriter;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::DummyTableScan;
use crate::plans::EvalScalar;
use crate::plans::ExpressionScan;
use crate::plans::Filter;
use crate::plans::Join;
use crate::plans::JoinEquiCondition;
use crate::plans::Operator;
use crate::plans::ProjectSet;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::Scan;
use crate::plans::Sort;
use crate::plans::UnionAll;
use crate::plans::Window;
use crate::ColumnEntry;
use crate::IndexType;
use crate::Metadata;

impl SubqueryRewriter {
    #[recursive::recursive]
    pub fn flatten_plan(
        &mut self,
        left: Option<&SExpr>,
        plan: &SExpr,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        need_cross_join: bool,
    ) -> Result<SExpr> {
        let prop = plan.derive_relational_prop()?;
        if prop.outer_columns.is_empty() {
            if !need_cross_join {
                return Ok(plan.clone());
            }
            return self.flatten_left(left, plan, correlated_columns);
        }

        match plan.plan() {
            RelOperator::EvalScalar(eval_scalar) => self.flatten_right_eval_scalar(
                left,
                plan,
                eval_scalar,
                correlated_columns,
                flatten_info,
                need_cross_join,
            ),
            RelOperator::ProjectSet(project_set) => self.flatten_right_project_set(
                left,
                plan,
                project_set,
                correlated_columns,
                flatten_info,
                need_cross_join,
            ),
            RelOperator::Filter(filter) => self.flatten_right_filter(
                left,
                filter,
                plan,
                correlated_columns,
                flatten_info,
                need_cross_join,
            ),
            RelOperator::Join(join) => {
                self.flatten_right_join(left, join, plan, correlated_columns, flatten_info)
            }
            RelOperator::Aggregate(aggregate) => self.flatten_right_aggregate(
                left,
                aggregate,
                plan,
                correlated_columns,
                flatten_info,
                need_cross_join,
            ),
            RelOperator::Sort(sort) => self.flatten_right_sort(
                left,
                plan,
                sort,
                correlated_columns,
                flatten_info,
                need_cross_join,
            ),
            RelOperator::Limit(_) => self.flatten_right_limit(
                left,
                plan,
                correlated_columns,
                flatten_info,
                need_cross_join,
            ),
            RelOperator::UnionAll(op) => self.flatten_right_union_all(
                left,
                op,
                plan,
                correlated_columns,
                flatten_info,
                need_cross_join,
            ),
            RelOperator::Window(op) => {
                self.flatten_right_window(left, plan, op, correlated_columns, flatten_info)
            }
            RelOperator::ExpressionScan(scan) => {
                self.flatten_right_expression_scan(plan, scan, correlated_columns)
            }
            _ => Err(ErrorCode::SemanticError(
                "Invalid plan type for flattening subquery",
            )),
        }
    }

    fn flatten_right_eval_scalar(
        &mut self,
        left: Option<&SExpr>,
        plan: &SExpr,
        eval_scalar: &EvalScalar,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        mut need_cross_join: bool,
    ) -> Result<SExpr> {
        if !eval_scalar.used_columns()?.is_disjoint(correlated_columns) {
            need_cross_join = true;
        }
        let flatten_plan = self.flatten_plan(
            left,
            plan.child(0)?,
            correlated_columns,
            flatten_info,
            need_cross_join,
        )?;
        let mut items = Vec::with_capacity(eval_scalar.items.len() + correlated_columns.len());
        for item in eval_scalar.items.iter() {
            let new_item = ScalarItem {
                scalar: self.flatten_scalar(&item.scalar, correlated_columns)?,
                index: item.index,
            };
            items.push(new_item);
        }
        let metadata = self.metadata.read();
        items.extend(sortd_iter(correlated_columns).map(|old| {
            let index = *self.derived_columns.get(&old).unwrap();
            self.scalar_item_from_index(index, "correlated.", &metadata)
        }));
        Ok(SExpr::create_unary(
            Arc::new(EvalScalar { items }.into()),
            Arc::new(flatten_plan),
        ))
    }

    fn flatten_right_project_set(
        &mut self,
        left: Option<&SExpr>,
        plan: &SExpr,
        project_set: &ProjectSet,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        mut need_cross_join: bool,
    ) -> Result<SExpr> {
        if !project_set
            .srfs
            .iter()
            .map(|srf| srf.scalar.used_columns())
            .fold(ColumnSet::new(), |mut acc, v| {
                acc.extend(v);
                acc
            })
            .is_disjoint(correlated_columns)
        {
            need_cross_join = true;
        }
        let flatten_plan = self.flatten_plan(
            left,
            plan.child(0)?,
            correlated_columns,
            flatten_info,
            need_cross_join,
        )?;
        let mut srfs = Vec::with_capacity(project_set.srfs.len());
        for item in project_set.srfs.iter() {
            let new_item = ScalarItem {
                scalar: self.flatten_scalar(&item.scalar, correlated_columns)?,
                index: item.index,
            };
            srfs.push(new_item);
        }
        let metadata = self.metadata.read();
        let scalar_items = self
            .derived_columns
            .values()
            .map(|index| self.scalar_item_from_index(*index, "correlated.", &metadata))
            .collect();
        Ok(SExpr::create_unary(
            Arc::new(ProjectSet { srfs }.into()),
            Arc::new(SExpr::create_unary(
                Arc::new(
                    EvalScalar {
                        items: scalar_items,
                    }
                    .into(),
                ),
                Arc::new(flatten_plan),
            )),
        ))
    }

    fn flatten_right_filter(
        &mut self,
        left: Option<&SExpr>,
        filter: &Filter,
        plan: &SExpr,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        mut need_cross_join: bool,
    ) -> Result<SExpr> {
        let mut predicates = Vec::with_capacity(filter.predicates.len());
        if !need_cross_join {
            need_cross_join = self.join_outer_inner_table(filter, correlated_columns)?;
            if need_cross_join {
                self.derived_columns.clear();
            }
        }
        let flatten_plan = self.flatten_plan(
            left,
            plan.child(0)?,
            correlated_columns,
            flatten_info,
            need_cross_join,
        )?;
        for predicate in filter.predicates.iter() {
            predicates.push(self.flatten_scalar(predicate, correlated_columns)?);
        }

        let filter_plan = Filter { predicates }.into();
        Ok(SExpr::create_unary(
            Arc::new(filter_plan),
            Arc::new(flatten_plan),
        ))
    }

    fn flatten_right_join(
        &mut self,
        left: Option<&SExpr>,
        join: &Join,
        plan: &SExpr,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
    ) -> Result<SExpr> {
        // Helper function to check if conditions need a cross join
        fn needs_cross_join(
            conditions: &[JoinEquiCondition],
            correlated_columns: &HashSet<IndexType>,
            left_side: bool,
        ) -> bool {
            conditions.iter().any(|condition| {
                let condition = if left_side {
                    &condition.left
                } else {
                    &condition.right
                };
                !condition.used_columns().is_disjoint(correlated_columns)
            })
        }

        // Helper function to process conditions
        fn process_conditions(
            conditions: &[ScalarExpr],
            correlated_columns: &HashSet<IndexType>,
            derived_columns: &HashMap<IndexType, IndexType>,
            need_cross_join: bool,
        ) -> Result<Vec<ScalarExpr>> {
            if need_cross_join {
                conditions
                    .iter()
                    .map(|condition| {
                        let mut new_condition = condition.clone();
                        for col in condition.used_columns() {
                            if correlated_columns.contains(&col) {
                                let new_col = derived_columns.get(&col).ok_or_else(|| {
                                    ErrorCode::Internal("Missing derived columns")
                                })?;
                                new_condition.replace_column(col, *new_col)?;
                            }
                        }
                        Ok(new_condition)
                    })
                    .collect()
            } else {
                Ok(conditions.to_vec())
            }
        }

        let mut left_need_cross_join =
            needs_cross_join(&join.equi_conditions, correlated_columns, true);
        let mut right_need_cross_join =
            needs_cross_join(&join.equi_conditions, correlated_columns, false);

        let join_rel_expr = RelExpr::with_s_expr(plan);
        let left_prop = join_rel_expr.derive_relational_prop_child(0)?;
        let right_prop = join_rel_expr.derive_relational_prop_child(1)?;

        for condition in join.non_equi_conditions.iter() {
            for col in condition.used_columns() {
                if correlated_columns.contains(&col) {
                    if left_prop.output_columns.contains(&col) {
                        left_need_cross_join = true;
                    } else if right_prop.output_columns.contains(&col) {
                        right_need_cross_join = true;
                    }
                }
            }
        }

        let left_flatten_plan = self.flatten_plan(
            left,
            plan.child(0)?,
            correlated_columns,
            flatten_info,
            left_need_cross_join,
        )?;
        let right_flatten_plan = self.flatten_plan(
            left,
            plan.child(1)?,
            correlated_columns,
            flatten_info,
            right_need_cross_join,
        )?;

        let left_conditions = join
            .equi_conditions
            .iter()
            .map(|condition| condition.left.clone())
            .collect::<Vec<_>>();
        let left_conditions = process_conditions(
            &left_conditions,
            correlated_columns,
            &self.derived_columns,
            left_need_cross_join,
        )?;
        let right_conditions = join
            .equi_conditions
            .iter()
            .map(|condition| condition.right.clone())
            .collect::<Vec<_>>();
        let right_conditions = process_conditions(
            &right_conditions,
            correlated_columns,
            &self.derived_columns,
            right_need_cross_join,
        )?;
        let non_equi_conditions = process_conditions(
            &join.non_equi_conditions,
            correlated_columns,
            &self.derived_columns,
            true,
        )?;

        Ok(SExpr::create_binary(
            Arc::new(
                Join {
                    equi_conditions: JoinEquiCondition::new_conditions(
                        left_conditions,
                        right_conditions,
                        vec![],
                    ),
                    non_equi_conditions,
                    join_type: join.join_type.clone(),
                    marker_index: join.marker_index,
                    from_correlated_subquery: false,
                    need_hold_hash_table: false,
                    is_lateral: false,
                    single_to_inner: None,
                    build_side_cache_info: None,
                }
                .into(),
            ),
            Arc::new(left_flatten_plan),
            Arc::new(right_flatten_plan),
        ))
    }

    fn flatten_right_aggregate(
        &mut self,
        left: Option<&SExpr>,
        aggregate: &Aggregate,
        plan: &SExpr,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        mut need_cross_join: bool,
    ) -> Result<SExpr> {
        if !aggregate.used_columns()?.is_disjoint(correlated_columns) {
            need_cross_join = true;
        }
        let flatten_plan = self.flatten_plan(
            left,
            plan.child(0)?,
            correlated_columns,
            flatten_info,
            need_cross_join,
        )?;
        let mut group_items =
            Vec::with_capacity(aggregate.group_items.len() + correlated_columns.len());
        for item in aggregate.group_items.iter() {
            let scalar = self.flatten_scalar(&item.scalar, correlated_columns)?;
            group_items.push(ScalarItem {
                scalar,
                index: item.index,
            })
        }

        let metadata = self.metadata.read();
        group_items.extend(sortd_iter(correlated_columns).map(|old| {
            let index = *self.derived_columns.get(&old).unwrap();
            self.scalar_item_from_index(index, "correlated.", &metadata)
        }));
        drop(metadata);

        let mut agg_items = Vec::with_capacity(aggregate.aggregate_functions.len());
        for item in aggregate.aggregate_functions.iter() {
            let scalar = self.flatten_scalar(&item.scalar, correlated_columns)?;
            if let ScalarExpr::AggregateFunction(AggregateFunction { func_name, .. }) = &scalar {
                // For scalar subquery, we'll convert it to single join.
                // Single join is similar to left outer join, if there isn't matched row in the right side, we'll add NULL value for the right side.
                // But for count aggregation function, NULL values should be 0.
                if aggregate.aggregate_functions.len() == 1
                    && (func_name.eq_ignore_ascii_case("count") || func_name.eq("count_distinct"))
                {
                    flatten_info.from_count_func = true;
                }
            }
            agg_items.push(ScalarItem {
                scalar,
                index: item.index,
            })
        }
        Ok(SExpr::create_unary(
            Arc::new(
                Aggregate {
                    mode: AggregateMode::Initial,
                    group_items,
                    aggregate_functions: agg_items,
                    from_distinct: aggregate.from_distinct,
                    rank_limit: aggregate.rank_limit.clone(),
                    grouping_sets: aggregate.grouping_sets.clone(),
                }
                .into(),
            ),
            Arc::new(flatten_plan),
        ))
    }

    fn flatten_right_sort(
        &mut self,
        left: Option<&SExpr>,
        plan: &SExpr,
        sort: &Sort,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        need_cross_join: bool,
    ) -> Result<SExpr> {
        // Currently, we don't support sort contain subquery.
        let flatten_plan = self.flatten_plan(
            left,
            plan.child(0)?,
            correlated_columns,
            flatten_info,
            need_cross_join,
        )?;
        // Check if sort contains `count() or distinct count()`.
        if sort.items.iter().any(|item| {
            let metadata = self.metadata.read();
            let col = metadata.column(item.index);
            if let ColumnEntry::DerivedColumn(derived_col) = col {
                // A little tricky here, we'll check if a sort item is a count aggregation function later.
                derived_col.alias.to_lowercase().starts_with("count")
            } else {
                false
            }
        }) {
            flatten_info.from_count_func = false;
        }
        Ok(SExpr::create_unary(
            Arc::new(plan.plan().clone()),
            Arc::new(flatten_plan),
        ))
    }

    fn flatten_right_limit(
        &mut self,
        left: Option<&SExpr>,
        plan: &SExpr,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        need_cross_join: bool,
    ) -> Result<SExpr> {
        // Currently, we don't support limit contain subquery.
        let flatten_plan = self.flatten_plan(
            left,
            plan.child(0)?,
            correlated_columns,
            flatten_info,
            need_cross_join,
        )?;
        Ok(SExpr::create_unary(
            Arc::new(plan.plan().clone()),
            Arc::new(flatten_plan),
        ))
    }

    fn flatten_right_window(
        &mut self,
        left: Option<&SExpr>,
        plan: &SExpr,
        window: &Window,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
    ) -> Result<SExpr> {
        if !window.used_columns()?.is_disjoint(correlated_columns) {
            return Err(ErrorCode::SemanticError(
                "correlated columns in window functions not supported",
            ));
        }
        let flatten_plan =
            self.flatten_plan(left, plan.child(0)?, correlated_columns, flatten_info, true)?;
        let mut partition_by = window.partition_by.clone();
        let metadata = self.metadata.read();
        partition_by.extend(sortd_iter(correlated_columns).map(|old| {
            let index = *self.derived_columns.get(&old).unwrap();
            self.scalar_item_from_index(index, "correlated.", &metadata)
        }));
        drop(metadata);

        Ok(SExpr::create_unary(
            Arc::new(
                Window {
                    span: window.span,
                    index: window.index,
                    function: window.function.clone(),
                    arguments: window.arguments.clone(),
                    partition_by,
                    order_by: window.order_by.clone(),
                    frame: window.frame.clone(),
                    limit: window.limit,
                }
                .into(),
            ),
            Arc::new(flatten_plan),
        ))
    }

    fn flatten_right_union_all(
        &mut self,
        left: Option<&SExpr>,
        op: &UnionAll,
        plan: &SExpr,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        mut need_cross_join: bool,
    ) -> Result<SExpr> {
        if !op.used_columns()?.is_disjoint(correlated_columns) {
            need_cross_join = true;
        }
        let left_flatten_plan = self.flatten_plan(
            left,
            plan.child(0)?,
            correlated_columns,
            flatten_info,
            need_cross_join,
        )?;
        let right_flatten_plan = self.flatten_plan(
            left,
            plan.child(1)?,
            correlated_columns,
            flatten_info,
            need_cross_join,
        )?;
        self.derived_columns
            .retain(|_, derived_column| op.output_indexes.contains(derived_column));

        Ok(SExpr::create_binary(
            Arc::new(op.clone().into()),
            Arc::new(left_flatten_plan),
            Arc::new(right_flatten_plan),
        ))
    }

    fn flatten_right_expression_scan(
        &mut self,
        plan: &SExpr,
        scan: &ExpressionScan,
        correlated_columns: &ColumnSet,
    ) -> Result<SExpr> {
        let binder = self.binder.as_ref().unwrap();
        for correlated_column in correlated_columns.iter() {
            let derived_column_index = binder
                .expression_scan_context
                .get_derived_column(scan.expression_scan_index, *correlated_column);
            self.derived_columns
                .insert(*correlated_column, derived_column_index);
        }
        Ok(plan.clone())
    }

    fn flatten_left(
        &mut self,
        left: Option<&SExpr>,
        plan: &SExpr,
        correlated_columns: &ColumnSet,
    ) -> Result<SExpr> {
        let left = self.flatten_left_recursive(left.unwrap())?;

        // Wrap logical get with distinct to eliminate duplicates rows.
        let metadata = self.metadata.read();
        let group_items = sortd_iter(correlated_columns)
            .map(|old| {
                let index = *self.derived_columns.get(&old).unwrap();
                self.scalar_item_from_index(index, "correlated.", &metadata)
            })
            .collect();

        let aggr = SExpr::create_unary(
            Arc::new(
                Aggregate {
                    mode: AggregateMode::Initial,
                    group_items,
                    ..Default::default()
                }
                .into(),
            ),
            Arc::new(left),
        );

        Ok(SExpr::create_binary(
            Arc::new(Join::default().into()),
            Arc::new(aggr),
            Arc::new(plan.clone()),
        ))
    }

    fn flatten_left_recursive(&mut self, expr: &SExpr) -> Result<SExpr> {
        let children = expr
            .children
            .iter()
            .map(|child| Ok(self.flatten_left_recursive(child)?.into()))
            .collect::<Result<_>>()?;

        Ok(SExpr {
            plan: Arc::new(self.flatten_left_plan(&expr.plan)?),
            children,
            original_group: None,
            rel_prop: Default::default(),
            stat_info: Default::default(),
            applied_rules: Default::default(),
        })
    }

    fn flatten_left_plan(&mut self, plan: &RelOperator) -> Result<RelOperator> {
        let op = match plan {
            RelOperator::DummyTableScan(_) => DummyTableScan.into(),
            RelOperator::Scan(scan) => self.flatten_left_scan(scan),
            RelOperator::EvalScalar(eval) => {
                let mut metadata = self.metadata.write();
                let items = eval
                    .items
                    .iter()
                    .map(|ScalarItem { scalar, index: old }| {
                        let mut scalar = scalar.clone();
                        for old in scalar.used_columns() {
                            let Some(new) = self.derived_columns.get(&old) else {
                                continue;
                            };
                            scalar.replace_column(old, *new).unwrap();
                        }
                        let column_entry = metadata.column(*old);
                        let name = column_entry.name();
                        let data_type = column_entry.data_type();
                        let index =
                            metadata.add_derived_column(name, data_type, Some(scalar.clone()));
                        self.derived_columns.insert(*old, index);
                        ScalarItem { scalar, index }
                    })
                    .collect();
                EvalScalar { items }.into()
            }
            RelOperator::Limit(limit) => limit.clone().into(),
            RelOperator::Sort(sort) => {
                let mut sort = sort.clone();
                for old in sort.used_columns() {
                    let Some(new) = self.derived_columns.get(&old) else {
                        continue;
                    };
                    sort.replace_column(old, *new);
                }
                sort.into()
            }
            RelOperator::Filter(filter) => {
                let mut filter = filter.clone();
                for predicate in &mut filter.predicates {
                    for old in predicate.used_columns() {
                        let Some(new) = self.derived_columns.get(&old) else {
                            continue;
                        };
                        predicate.replace_column(old, *new).unwrap();
                    }
                }
                filter.into()
            }
            RelOperator::Join(join) => {
                let mut join = join.clone();
                for old in join.used_columns().unwrap() {
                    let Some(new) = self.derived_columns.get(&old) else {
                        continue;
                    };
                    join.replace_column(old, *new).unwrap();
                }
                join.into()
            }
            _ => return Err(ErrorCode::Unimplemented(format!("{:?}", plan.rel_op()))),
        };
        Ok(op)
    }

    fn flatten_left_scan(&mut self, scan: &Scan) -> RelOperator {
        let mut metadata = self.metadata.write();
        let columns = scan
            .columns
            .iter()
            .copied()
            .map(|col| {
                let column_entry = metadata.column(col).clone();
                let derived_index = metadata.add_derived_column(
                    column_entry.name(),
                    column_entry.data_type(),
                    None,
                );
                self.derived_columns.insert(col, derived_index);
                derived_index
            })
            .collect();
        Scan {
            table_index: scan.table_index,
            columns,
            scan_id: metadata.next_scan_id(),
            ..Default::default()
        }
        .into()
    }

    fn scalar_item_from_index(
        &self,
        index: IndexType,
        name_prefix: &str,
        metadata: &Metadata,
    ) -> ScalarItem {
        let column_entry = metadata.column(index);
        let column = ColumnBindingBuilder::new(
            format!("{name_prefix}{}", column_entry.name()),
            index,
            Box::from(column_entry.data_type()),
            Visibility::Visible,
        )
        .build();
        ScalarItem {
            scalar: ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column }),
            index,
        }
    }
}

fn sortd_iter(columns: &ColumnSet) -> impl Iterator<Item = IndexType> {
    let mut v = Vec::from_iter(columns.iter().copied());
    v.sort();
    v.into_iter()
}
