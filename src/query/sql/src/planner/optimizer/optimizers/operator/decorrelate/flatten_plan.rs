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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;

use super::DerivedColumnMap;
use super::FlattenPlanResult;
use crate::ColumnEntry;
use crate::ColumnSet;
use crate::Metadata;
use crate::Symbol;
use crate::binder::ColumnBindingBuilder;
use crate::binder::Visibility;
use crate::binder::WindowOrderByInfo;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::operator::FlattenInfo;
use crate::optimizer::optimizers::operator::SubqueryDecorrelatorOptimizer;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::ConstantExpr;
use crate::plans::ConstantTableScan;
use crate::plans::EvalScalar;
use crate::plans::ExpressionScan;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinEquiCondition;
use crate::plans::Limit;
use crate::plans::Operator;
use crate::plans::ProjectSet;
use crate::plans::RecursiveCteScan;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::Scan;
use crate::plans::Sequence;
use crate::plans::Sort;
use crate::plans::SortItem;
use crate::plans::UnionAll;
use crate::plans::Window;
use crate::plans::WindowFuncFrame;
use crate::plans::WindowFuncFrameBound;
use crate::plans::WindowFuncFrameUnits;
use crate::plans::WindowFuncType;
use crate::plans::WindowPartition;

fn merge_derived_columns(
    mut derived_columns: DerivedColumnMap,
    updates: DerivedColumnMap,
) -> DerivedColumnMap {
    derived_columns.extend(updates);
    derived_columns
}

impl SubqueryDecorrelatorOptimizer {
    #[recursive::recursive]
    pub fn flatten_plan(
        &mut self,
        outer: &SExpr,
        subquery: &SExpr,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        need_cross_join: bool,
        available_derived_columns: &DerivedColumnMap,
    ) -> Result<FlattenPlanResult> {
        let prop = subquery.derive_relational_prop()?;
        if prop.outer_columns.is_empty() {
            return if need_cross_join {
                self.rewrite_to_join_then_aggr(
                    outer,
                    subquery,
                    correlated_columns,
                    available_derived_columns,
                )
            } else {
                Ok((subquery.clone(), available_derived_columns.clone()))
            };
        }

        match subquery.plan() {
            RelOperator::EvalScalar(eval_scalar) => self.flatten_sub_eval_scalar(
                outer,
                subquery,
                eval_scalar,
                correlated_columns,
                flatten_info,
                need_cross_join,
                available_derived_columns,
            ),
            RelOperator::ProjectSet(project_set) => self.flatten_sub_project_set(
                outer,
                subquery,
                project_set,
                correlated_columns,
                flatten_info,
                need_cross_join,
                available_derived_columns,
            ),
            RelOperator::Filter(filter) => self.flatten_sub_filter(
                outer,
                subquery,
                filter,
                correlated_columns,
                flatten_info,
                need_cross_join,
                available_derived_columns,
            ),
            RelOperator::Join(join) => self.flatten_sub_join(
                outer,
                subquery,
                join,
                correlated_columns,
                flatten_info,
                available_derived_columns,
            ),
            RelOperator::Aggregate(aggregate) => self.flatten_sub_aggregate(
                outer,
                subquery,
                aggregate,
                correlated_columns,
                flatten_info,
                need_cross_join,
                available_derived_columns,
            ),
            RelOperator::Sort(sort) => self.flatten_sub_sort(
                outer,
                subquery,
                sort,
                correlated_columns,
                flatten_info,
                need_cross_join,
                available_derived_columns,
            ),
            RelOperator::Limit(limit) => self.flatten_sub_limit(
                outer,
                subquery,
                limit,
                correlated_columns,
                flatten_info,
                need_cross_join,
                available_derived_columns,
            ),
            RelOperator::UnionAll(op) => self.flatten_sub_union_all(
                outer,
                subquery,
                op,
                correlated_columns,
                flatten_info,
                need_cross_join,
                available_derived_columns,
            ),
            RelOperator::Window(op) => self.flatten_sub_window(
                outer,
                subquery,
                op,
                correlated_columns,
                flatten_info,
                available_derived_columns,
            ),
            RelOperator::ExpressionScan(scan) => self.flatten_sub_expression_scan(
                subquery,
                scan,
                correlated_columns,
                available_derived_columns,
            ),
            _ => Err(ErrorCode::SemanticError(
                "Invalid plan type for flattening subquery",
            )),
        }
    }

    fn flatten_sub_eval_scalar(
        &mut self,
        outer: &SExpr,
        subquery: &SExpr,
        eval_scalar: &EvalScalar,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        mut need_cross_join: bool,
        available_derived_columns: &DerivedColumnMap,
    ) -> Result<FlattenPlanResult> {
        if !eval_scalar.used_columns()?.is_disjoint(correlated_columns) {
            need_cross_join = true;
        }

        let (flatten_plan, derived_columns) = self.flatten_plan(
            outer,
            subquery.unary_child(),
            correlated_columns,
            flatten_info,
            need_cross_join,
            available_derived_columns,
        )?;

        let metadata = self.metadata.clone();
        let metadata = metadata.read();
        let items: Vec<ScalarItem> = eval_scalar
            .items
            .iter()
            .filter(|item| !correlated_columns.contains(&item.index))
            .map(Item::Scalar)
            .chain(correlated_columns.iter().copied().map(Item::Index))
            .map(|item| match item {
                Item::Scalar(item) => Ok(ScalarItem {
                    scalar: self.flatten_scalar(
                        &item.scalar,
                        correlated_columns,
                        &derived_columns,
                    )?,
                    index: item.index,
                }),
                Item::Index(old) => Ok(Self::scalar_item_from_index(
                    Self::get_derived(&derived_columns, old)?,
                    "outer.",
                    &metadata,
                )),
            })
            .collect::<Result<_>>()?;

        // Eg1. SELECT c_id, (SELECT count() FROM o WHERE o.c_id=c.c_id) FROM c ORDER BY c_id;
        // Eg2. SELECT
        //   (
        //     SELECT
        //       IF(COUNT(0) = 0, '0', '1')
        //     FROM
        //       property_records pr
        //     WHERE
        //       th.property_id = pr.property_id
        //       AND th.owner_id = pr.owner_id
        //   ) AS ownership_status,
        //   (
        //     SELECT
        //       IF(COUNT(0) = 0, '0', '1')
        //     FROM
        //       mortgage_records mr
        //     WHERE
        //       th.property_id = mr.property_id
        //   ) AS mortgage_status
        // FROM
        //   transaction_history th;

        if flatten_info.from_count_func {
            flatten_info.from_count_func = items.iter().any(|x| {
                if let ScalarExpr::BoundColumnRef(cf) = &x.scalar {
                    matches!(
                        cf.column.data_type.as_ref(),
                        &DataType::Number(NumberDataType::UInt64),
                    )
                } else {
                    false
                }
            });
        }

        Ok((
            flatten_plan.build_unary(EvalScalar { items }),
            derived_columns,
        ))
    }

    fn flatten_sub_project_set(
        &mut self,
        outer: &SExpr,
        subquery: &SExpr,
        project_set: &ProjectSet,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        mut need_cross_join: bool,
        available_derived_columns: &DerivedColumnMap,
    ) -> Result<FlattenPlanResult> {
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
        let (flatten_plan, derived_columns) = self.flatten_plan(
            outer,
            subquery.unary_child(),
            correlated_columns,
            flatten_info,
            need_cross_join,
            available_derived_columns,
        )?;
        let mut srfs = Vec::with_capacity(project_set.srfs.len());
        for item in project_set.srfs.iter() {
            let new_item = ScalarItem {
                scalar: self.flatten_scalar(&item.scalar, correlated_columns, &derived_columns)?,
                index: item.index,
            };
            srfs.push(new_item);
        }
        let metadata = self.metadata.read();
        let scalar_items = derived_columns
            .values()
            .map(|index| Self::scalar_item_from_index(*index, "outer.", &metadata))
            .collect();
        Ok((
            flatten_plan
                .build_unary(EvalScalar {
                    items: scalar_items,
                })
                .build_unary(ProjectSet { srfs }),
            derived_columns,
        ))
    }

    fn flatten_sub_filter(
        &mut self,
        outer: &SExpr,
        subquery: &SExpr,
        filter: &Filter,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        need_cross_join: bool,
        available_derived_columns: &DerivedColumnMap,
    ) -> Result<FlattenPlanResult> {
        let (join_outer_inner, derived_columns) =
            self.join_outer_inner_table(filter, correlated_columns)?;
        let need_cross_join = need_cross_join || join_outer_inner;
        let child_available_derived_columns = if join_outer_inner {
            DerivedColumnMap::new()
        } else {
            merge_derived_columns(available_derived_columns.clone(), derived_columns)
        };
        let (flatten_plan, derived_columns) = self.flatten_plan(
            outer,
            subquery.unary_child(),
            correlated_columns,
            flatten_info,
            need_cross_join,
            &child_available_derived_columns,
        )?;

        let predicates = filter
            .predicates
            .iter()
            .map(|predicate| self.flatten_scalar(predicate, correlated_columns, &derived_columns))
            .collect::<Result<_>>()?;

        Ok((
            flatten_plan.build_unary(Filter { predicates }),
            derived_columns,
        ))
    }

    fn flatten_sub_join(
        &mut self,
        outer: &SExpr,
        subquery: &SExpr,
        join: &Join,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        available_derived_columns: &DerivedColumnMap,
    ) -> Result<FlattenPlanResult> {
        // Helper function to check if conditions need a cross join
        fn needs_cross_join(
            conditions: &[JoinEquiCondition],
            correlated_columns: &ColumnSet,
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
            correlated_columns: &ColumnSet,
            derived_columns: &HashMap<Symbol, Symbol>,
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
                                    ErrorCode::Internal(format!("Missing derived column {col}"))
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

        let join_rel_expr = RelExpr::with_s_expr(subquery);
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

        let (left_flatten_plan, left_derived_columns) = self.flatten_plan(
            outer,
            subquery.left_child(),
            correlated_columns,
            flatten_info,
            left_need_cross_join,
            available_derived_columns,
        )?;
        let (right_flatten_plan, right_derived_columns) = self.flatten_plan(
            outer,
            subquery.right_child(),
            correlated_columns,
            flatten_info,
            right_need_cross_join,
            available_derived_columns,
        )?;

        let left_conditions = join
            .equi_conditions
            .iter()
            .map(|condition| condition.left.clone())
            .collect::<Vec<_>>();
        let left_conditions = process_conditions(
            &left_conditions,
            correlated_columns,
            &left_derived_columns,
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
            &right_derived_columns,
            right_need_cross_join,
        )?;
        let derived_columns = merge_derived_columns(left_derived_columns, right_derived_columns);
        let non_equi_conditions = process_conditions(
            &join.non_equi_conditions,
            correlated_columns,
            &derived_columns,
            true,
        )?;

        Ok((
            SExpr::create_binary(
                Join {
                    equi_conditions: JoinEquiCondition::new_conditions(
                        left_conditions,
                        right_conditions,
                        vec![],
                    ),
                    non_equi_conditions,
                    join_type: join.join_type,
                    marker_index: join.marker_index,
                    from_correlated_subquery: false,
                    need_hold_hash_table: false,
                    is_lateral: false,
                    single_to_inner: None,
                    build_side_cache_info: None,
                },
                left_flatten_plan,
                right_flatten_plan,
            ),
            derived_columns,
        ))
    }

    fn flatten_sub_aggregate(
        &mut self,
        outer: &SExpr,
        subquery: &SExpr,
        aggregate: &Aggregate,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        mut need_cross_join: bool,
        available_derived_columns: &DerivedColumnMap,
    ) -> Result<FlattenPlanResult> {
        if !aggregate.used_columns()?.is_disjoint(correlated_columns) {
            need_cross_join = true;
        }
        let (flatten_plan, derived_columns) = self.flatten_plan(
            outer,
            subquery.unary_child(),
            correlated_columns,
            flatten_info,
            need_cross_join,
            available_derived_columns,
        )?;

        let metadata = self.metadata.clone();
        let metadata = metadata.read();
        let group_items = aggregate
            .group_items
            .iter()
            .map(Item::Scalar)
            .chain(correlated_columns.iter().copied().map(Item::Index))
            .map(|item| match item {
                Item::Scalar(item) => {
                    let scalar =
                        self.flatten_scalar(&item.scalar, correlated_columns, &derived_columns)?;
                    Ok(ScalarItem {
                        scalar,
                        index: item.index,
                    })
                }
                Item::Index(old) => Ok(Self::scalar_item_from_index(
                    Self::get_derived(&derived_columns, old)?,
                    "outer.",
                    &metadata,
                )),
            })
            .collect::<Result<_>>()?;
        drop(metadata);

        let mut agg_items = Vec::with_capacity(aggregate.aggregate_functions.len());
        for item in aggregate.aggregate_functions.iter() {
            let scalar = self.flatten_scalar(&item.scalar, correlated_columns, &derived_columns)?;
            if let ScalarExpr::AggregateFunction(AggregateFunction { func_name, .. }) = &scalar {
                // For scalar subquery, we'll convert it to single join.
                // Single join is similar to left outer join, if there isn't matched row in the right side, we'll add NULL value for the right side.
                // But for count aggregation function, NULL values should be 0.
                if aggregate.aggregate_functions.len() == 1
                    && (func_name.eq_ignore_ascii_case("count")
                        || func_name.eq_ignore_ascii_case("count_distinct"))
                {
                    flatten_info.from_count_func = true;
                }
            }
            agg_items.push(ScalarItem {
                scalar,
                index: item.index,
            })
        }
        Ok((
            flatten_plan.build_unary(Aggregate {
                mode: AggregateMode::Initial,
                group_items,
                aggregate_functions: agg_items,
                from_distinct: aggregate.from_distinct,
                rank_limit: aggregate.rank_limit.clone(),
                grouping_sets: aggregate.grouping_sets.clone(),
            }),
            derived_columns,
        ))
    }

    fn flatten_sub_sort(
        &mut self,
        outer: &SExpr,
        subquery: &SExpr,
        sort: &Sort,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        need_cross_join: bool,
        available_derived_columns: &DerivedColumnMap,
    ) -> Result<FlattenPlanResult> {
        // Currently, we don't support sort contain subquery.
        let (flatten_plan, derived_columns) = self.flatten_plan(
            outer,
            subquery.unary_child(),
            correlated_columns,
            flatten_info,
            need_cross_join,
            available_derived_columns,
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
        Ok((
            flatten_plan.build_unary(subquery.plan.clone()),
            derived_columns,
        ))
    }

    fn flatten_sub_limit(
        &mut self,
        outer: &SExpr,
        subquery: &SExpr,
        limit: &Limit,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        need_cross_join: bool,
        available_derived_columns: &DerivedColumnMap,
    ) -> Result<FlattenPlanResult> {
        let (flatten_plan, derived_columns, order_by) = match subquery.unary_child().plan() {
            RelOperator::Sort(sort) => {
                let (flatten_plan, derived_columns) = self.flatten_plan(
                    outer,
                    subquery.unary_child().unary_child(),
                    correlated_columns,
                    flatten_info,
                    need_cross_join,
                    available_derived_columns,
                )?;

                if sort.items.iter().any(|item| {
                    let metadata = self.metadata.read();
                    let col = metadata.column(item.index);
                    if let ColumnEntry::DerivedColumn(derived_col) = col {
                        derived_col.alias.to_lowercase().starts_with("count")
                    } else {
                        false
                    }
                }) {
                    flatten_info.from_count_func = false;
                }

                let metadata = self.metadata.read();
                let order_by = sort
                    .items
                    .iter()
                    .map(|item| {
                        let index = derived_columns
                            .get(&item.index)
                            .copied()
                            .unwrap_or(item.index);
                        Ok(WindowOrderByInfo {
                            order_by_item: Self::scalar_item_from_index(index, "", &metadata),
                            asc: Some(item.asc),
                            nulls_first: Some(item.nulls_first),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                (flatten_plan, derived_columns, order_by)
            }
            _ => {
                let (flatten_plan, derived_columns) = self.flatten_plan(
                    outer,
                    subquery.unary_child(),
                    correlated_columns,
                    flatten_info,
                    need_cross_join,
                    available_derived_columns,
                )?;
                (flatten_plan, derived_columns, vec![])
            }
        };

        if limit.limit.is_none() && limit.offset == 0 {
            return Ok((flatten_plan, derived_columns));
        }

        let metadata = self.metadata.read();
        let partition_by = correlated_columns
            .iter()
            .copied()
            .map(|old| {
                Ok(Self::scalar_item_from_index(
                    Self::get_derived(&derived_columns, old)?,
                    "outer.",
                    &metadata,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        drop(metadata);

        let row_number_type = DataType::Number(NumberDataType::UInt64);
        let row_number_index = self.metadata.write().add_derived_column(
            "correlated_limit_row_number".to_string(),
            row_number_type.clone(),
        );

        let sort_settings = self.ctx.get_settings();
        let default_nulls_first = sort_settings.get_nulls_first();

        let mut sort_items = Vec::with_capacity(partition_by.len() + order_by.len());
        for part in &partition_by {
            sort_items.push(SortItem {
                index: part.index,
                asc: true,
                nulls_first: default_nulls_first(true),
            });
        }
        for order in &order_by {
            let asc = order.asc.unwrap_or(true);
            sort_items.push(SortItem {
                index: order.order_by_item.index,
                asc,
                nulls_first: order
                    .nulls_first
                    .unwrap_or_else(|| default_nulls_first(asc)),
            });
        }

        let window_plan = Window {
            span: None,
            index: row_number_index,
            function: WindowFuncType::RowNumber,
            arguments: vec![],
            partition_by: partition_by.clone(),
            order_by: order_by.clone(),
            frame: WindowFuncFrame {
                units: WindowFuncFrameUnits::Rows,
                start_bound: WindowFuncFrameBound::Preceding(None),
                end_bound: WindowFuncFrameBound::CurrentRow,
            },
            limit: None,
        };

        let window_child = if sort_items.is_empty() {
            flatten_plan
        } else {
            flatten_plan.build_unary(Sort {
                items: sort_items,
                limit: None,
                after_exchange: None,
                pre_projection: None,
                window_partition: if partition_by.is_empty() {
                    None
                } else {
                    Some(WindowPartition {
                        partition_by: partition_by.clone(),
                        top: None,
                        func: WindowFuncType::RowNumber,
                    })
                },
            })
        };

        let row_number_column = ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                "correlated_limit_row_number".to_string(),
                row_number_index,
                Box::new(row_number_type),
                Visibility::Visible,
            )
            .build(),
        });

        let mut predicates = Vec::new();
        if let Some(row_limit) = limit.limit {
            predicates.push(ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "lte".to_string(),
                params: vec![],
                arguments: vec![
                    row_number_column.clone(),
                    ScalarExpr::ConstantExpr(ConstantExpr {
                        span: None,
                        value: Scalar::Number(NumberScalar::UInt64(
                            row_limit.saturating_add(limit.offset) as u64,
                        )),
                    }),
                ],
            }));
        }

        if limit.offset > 0 {
            predicates.push(ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "gt".to_string(),
                params: vec![],
                arguments: vec![
                    row_number_column,
                    ScalarExpr::ConstantExpr(ConstantExpr {
                        span: None,
                        value: Scalar::Number(NumberScalar::UInt64(limit.offset as u64)),
                    }),
                ],
            }));
        }

        Ok((
            window_child
                .build_unary(window_plan)
                .build_unary(Filter { predicates }),
            derived_columns,
        ))
    }

    fn flatten_sub_window(
        &mut self,
        outer: &SExpr,
        subquery: &SExpr,
        window: &Window,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        available_derived_columns: &DerivedColumnMap,
    ) -> Result<FlattenPlanResult> {
        if !window.used_columns()?.is_disjoint(correlated_columns) {
            return Err(ErrorCode::SemanticError(
                "correlated columns in window functions not supported",
            ));
        }
        let (flatten_plan, derived_columns) = self.flatten_plan(
            outer,
            subquery.unary_child(),
            correlated_columns,
            flatten_info,
            true,
            available_derived_columns,
        )?;
        let metadata = self.metadata.read();
        let partition_by = window
            .partition_by
            .iter()
            .cloned()
            .map(Ok)
            .chain(correlated_columns.iter().copied().map(|old| {
                Ok(Self::scalar_item_from_index(
                    Self::get_derived(&derived_columns, old)?,
                    "outer.",
                    &metadata,
                ))
            }))
            .collect::<Result<_>>()?;
        drop(metadata);

        Ok((
            flatten_plan.build_unary(Window {
                span: window.span,
                index: window.index,
                function: window.function.clone(),
                arguments: window.arguments.clone(),
                partition_by,
                order_by: window.order_by.clone(),
                frame: window.frame.clone(),
                limit: window.limit,
            }),
            derived_columns,
        ))
    }

    fn flatten_sub_union_all(
        &mut self,
        outer: &SExpr,
        subquery: &SExpr,
        union_all: &UnionAll,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        mut need_cross_join: bool,
        available_derived_columns: &DerivedColumnMap,
    ) -> Result<FlattenPlanResult> {
        if !union_all.used_columns()?.is_disjoint(correlated_columns) {
            need_cross_join = true;
        }

        let rel_expr = RelExpr::with_s_expr(subquery);
        let left_prop = rel_expr.derive_relational_prop_child(0)?;
        let right_prop = rel_expr.derive_relational_prop_child(1)?;
        let left_need_cross_join =
            need_cross_join || !correlated_columns.is_subset(&left_prop.outer_columns);
        let right_need_cross_join =
            need_cross_join || !correlated_columns.is_subset(&right_prop.outer_columns);

        let mut union_all = union_all.clone();
        let (left_flatten_plan, left_derived) = self.flatten_plan(
            outer,
            subquery.left_child(),
            correlated_columns,
            flatten_info,
            left_need_cross_join,
            available_derived_columns,
        )?;
        Self::rewrite_union_branch_outputs(
            &mut union_all.left_outputs,
            correlated_columns,
            &left_derived,
        )?;

        let (right_flatten_plan, right_derived) = self.flatten_plan(
            outer,
            subquery.right_child(),
            correlated_columns,
            flatten_info,
            right_need_cross_join,
            available_derived_columns,
        )?;
        Self::rewrite_union_branch_outputs(
            &mut union_all.right_outputs,
            correlated_columns,
            &right_derived,
        )?;

        let mut metadata = self.metadata.write();
        let mut derived_columns = DerivedColumnMap::new();
        union_all
            .output_indexes
            .extend(correlated_columns.iter().copied().map(|old| {
                let column_entry = metadata.column(old);
                let name = column_entry.name();
                let data_type = column_entry.data_type();
                let new = metadata.add_derived_column(name, data_type);
                derived_columns.insert(old, new);
                new
            }));

        Ok((
            SExpr::create_binary(
                Arc::new(union_all.clone().into()),
                Arc::new(left_flatten_plan),
                Arc::new(right_flatten_plan),
            ),
            derived_columns,
        ))
    }

    fn rewrite_union_branch_outputs(
        branch_outputs: &mut Vec<(Symbol, Option<ScalarExpr>)>,
        correlated_columns: &ColumnSet,
        derived: &DerivedColumnMap,
    ) -> Result<()> {
        *branch_outputs = branch_outputs
            .drain(..)
            .map(|(old, mut expr)| {
                let Some(&new) = derived.get(&old) else {
                    return Ok((old, expr));
                };
                if let Some(expr) = &mut expr {
                    expr.replace_column(old, new)?;
                };
                Ok((new, expr))
            })
            .chain(correlated_columns.iter().copied().map(|old| {
                let new = derived
                    .get(&old)
                    .copied()
                    .ok_or_else(|| ErrorCode::Internal(format!("Missing derived column {old}")))?;
                Ok((new, None))
            }))
            .collect::<Result<_>>()?;
        Ok(())
    }

    fn flatten_sub_expression_scan(
        &mut self,
        subquery: &SExpr,
        scan: &ExpressionScan,
        correlated_columns: &ColumnSet,
        available_derived_columns: &DerivedColumnMap,
    ) -> Result<FlattenPlanResult> {
        let binder = self.binder.as_ref().unwrap();
        let derived_columns = correlated_columns
            .iter()
            .copied()
            .map(|correlated_column| {
                let derived_column_index = binder
                    .expression_scan_context
                    .get_derived_column(scan.expression_scan_index, correlated_column);
                (correlated_column, derived_column_index)
            })
            .collect();
        Ok((
            subquery.clone(),
            merge_derived_columns(available_derived_columns.clone(), derived_columns),
        ))
    }

    fn rewrite_to_join_then_aggr(
        &mut self,
        outer: &SExpr,
        subquery: &SExpr,
        correlated_columns: &ColumnSet,
        available_derived_columns: &DerivedColumnMap,
    ) -> Result<FlattenPlanResult> {
        let mut derived_columns = available_derived_columns.clone();
        let outer = self.clone_outer_recursive(outer, &mut derived_columns)?;

        // Wrap logical get with distinct to eliminate duplicates rows.
        let metadata = self.metadata.read();
        let group_items = correlated_columns
            .iter()
            .copied()
            .map(|old| {
                Ok(Self::scalar_item_from_index(
                    Self::get_derived(&derived_columns, old)?,
                    "",
                    &metadata,
                ))
            })
            .collect::<Result<_>>()?;

        let aggr = outer.build_unary(Aggregate {
            mode: AggregateMode::Initial,
            group_items,
            ..Default::default()
        });

        Ok((
            SExpr::create_binary(
                Arc::new(Join::default().into()),
                Arc::new(aggr),
                Arc::new(subquery.clone()),
            ),
            derived_columns,
        ))
    }

    #[recursive::recursive]
    fn clone_outer_recursive(
        &mut self,
        outer: &SExpr,
        derived_columns: &mut DerivedColumnMap,
    ) -> Result<SExpr> {
        let children = outer
            .children
            .iter()
            .map(|child| Ok(self.clone_outer_recursive(child, derived_columns)?.into()))
            .collect::<Result<_>>()?;

        Ok(SExpr::create(
            self.clone_outer_plan(outer.plan(), derived_columns)?,
            children,
            None,
            None,
            None,
        ))
    }

    fn clone_outer_plan(
        &mut self,
        plan: &RelOperator,
        derived_columns: &mut DerivedColumnMap,
    ) -> Result<RelOperator> {
        let op = match plan {
            RelOperator::DummyTableScan(scan) => scan.clone().into(),
            RelOperator::ConstantTableScan(scan) => {
                self.clone_outer_constant_table_scan(scan, derived_columns)?
            }
            RelOperator::Scan(scan) => self.clone_outer_scan(scan, derived_columns),
            RelOperator::RecursiveCteScan(scan) => {
                self.clone_outer_recursive_cte_scan(scan, derived_columns)?
            }
            RelOperator::UnionAll(union_all) => {
                self.clone_outer_union_all(union_all, derived_columns)?
            }
            RelOperator::Sequence(sequence) => self.clone_outer_sequence(sequence),
            RelOperator::EvalScalar(eval) => self.clone_outer_eval_scalar(eval, derived_columns)?,
            RelOperator::Limit(limit) => limit.clone().into(),
            RelOperator::Sort(sort) => {
                let mut sort = sort.clone();
                for old in sort.used_columns() {
                    sort.replace_column(old, Self::get_derived(derived_columns, old)?);
                }
                sort.into()
            }
            RelOperator::Filter(filter) => {
                let mut filter = filter.clone();
                for predicate in &mut filter.predicates {
                    for old in predicate.used_columns() {
                        predicate.replace_column(old, Self::get_derived(derived_columns, old)?)?;
                    }
                }
                filter.into()
            }
            RelOperator::Join(join) => {
                let mut join = join.clone();
                for old in join.used_columns()? {
                    join.replace_column(old, Self::get_derived(derived_columns, old)?)?;
                }
                if let Some(mark) = &mut join.marker_index {
                    let mut metadata = self.metadata.write();
                    let column_entry = metadata.column(*mark);
                    let name = column_entry.name();
                    let data_type = column_entry.data_type();
                    let new_mark = metadata.add_derived_column(name, data_type);
                    derived_columns.insert(*mark, new_mark);
                    *mark = new_mark;
                }
                join.into()
            }
            RelOperator::Aggregate(aggregate) => {
                let mut aggregate = aggregate.clone();
                let metadata = self.metadata.clone();
                let mut metadata = metadata.write();
                for item in &mut aggregate.group_items {
                    *item = self.clone_outer_scalar_item(item, &mut metadata, derived_columns)?;
                }
                for func in &mut aggregate.aggregate_functions {
                    *func = self.clone_outer_scalar_item(func, &mut metadata, derived_columns)?;
                }
                aggregate.rank_limit = None;
                if aggregate.grouping_sets.is_some() {
                    return Err(ErrorCode::Unimplemented(
                        "join left plan can't contain aggregate with GROUPING SETS to dcorrelated join right plan",
                    ));
                }
                aggregate.into()
            }
            _ => {
                return Err(ErrorCode::Unimplemented(format!(
                    "join left plan can't contain {:?} to dcorrelated join right plan",
                    plan.rel_op()
                )));
            }
        };
        Ok(op)
    }

    fn clone_outer_constant_table_scan(
        &mut self,
        scan: &ConstantTableScan,
        derived_columns: &mut DerivedColumnMap,
    ) -> Result<RelOperator> {
        let mut metadata = self.metadata.write();
        let ((values, fields), columns) = scan
            .columns
            .iter()
            .copied()
            .map(|index| {
                let (value, field) = scan.value(index)?;
                let name = metadata.column(index).name();
                let derived_index = metadata.add_derived_column(name, field.data_type().clone());

                let field = DataField::new(&derived_index.to_string(), field.data_type().clone());
                derived_columns.insert(index, derived_index);
                Ok(((value, field), derived_index))
            })
            .collect::<Result<((Vec<_>, Vec<_>), ColumnSet)>>()?;

        Ok(ConstantTableScan {
            values,
            num_rows: scan.num_rows,
            schema: Arc::new(DataSchema::new(fields)),
            columns,
        }
        .into())
    }

    fn clone_outer_scan(
        &mut self,
        scan: &Scan,
        derived_columns: &mut DerivedColumnMap,
    ) -> RelOperator {
        let mut metadata = self.metadata.write();
        let columns = scan
            .columns
            .iter()
            .copied()
            .map(|col| {
                let column_entry = metadata.column(col).clone();
                let derived_index =
                    metadata.add_derived_column(column_entry.name(), column_entry.data_type());
                derived_columns.insert(col, derived_index);
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

    fn clone_outer_recursive_cte_scan(
        &mut self,
        scan: &RecursiveCteScan,
        derived_columns: &mut DerivedColumnMap,
    ) -> Result<RelOperator> {
        let mut metadata = self.metadata.write();
        let fields = scan
            .fields
            .iter()
            .map(|field| {
                let index = field.name().parse()?;
                let column_entry = metadata.column(index).clone();
                let derived_index =
                    metadata.add_derived_column(column_entry.name(), column_entry.data_type());
                derived_columns.insert(index, derived_index);
                Ok(DataField::new(
                    &derived_index.to_string(),
                    field.data_type().clone(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(RecursiveCteScan {
            fields,
            table_name: scan.table_name.clone(),
            logical_recursive_cte_id: scan.logical_recursive_cte_id,
        }
        .into())
    }

    fn clone_outer_union_all(
        &mut self,
        union_all: &UnionAll,
        derived_columns: &mut DerivedColumnMap,
    ) -> Result<RelOperator> {
        let mut union_all = union_all.clone();
        union_all.left_outputs = union_all
            .left_outputs
            .drain(..)
            .map(|(old, mut expr)| {
                let Some(&new) = derived_columns.get(&old) else {
                    return Ok((old, expr));
                };
                if let Some(expr) = &mut expr {
                    for used_column in expr.used_columns() {
                        expr.replace_column(
                            used_column,
                            Self::get_derived(derived_columns, used_column)?,
                        )?;
                    }
                }
                Ok((new, expr))
            })
            .collect::<Result<_>>()?;
        union_all.right_outputs = union_all
            .right_outputs
            .drain(..)
            .map(|(old, mut expr)| {
                let Some(&new) = derived_columns.get(&old) else {
                    return Ok((old, expr));
                };
                if let Some(expr) = &mut expr {
                    for used_column in expr.used_columns() {
                        expr.replace_column(
                            used_column,
                            Self::get_derived(derived_columns, used_column)?,
                        )?;
                    }
                }
                Ok((new, expr))
            })
            .collect::<Result<_>>()?;

        let mut metadata = self.metadata.write();
        union_all.output_indexes = union_all
            .output_indexes
            .iter()
            .copied()
            .map(|old| {
                let column_entry = metadata.column(old);
                let name = column_entry.name().to_string();
                let data_type = column_entry.data_type();
                let new = metadata.add_derived_column(name, data_type);
                derived_columns.insert(old, new);
                new
            })
            .collect();

        Ok(union_all.into())
    }

    fn clone_outer_sequence(&mut self, sequence: &Sequence) -> RelOperator {
        sequence.clone().into()
    }

    fn clone_outer_eval_scalar(
        &mut self,
        eval: &EvalScalar,
        derived_columns: &mut DerivedColumnMap,
    ) -> Result<RelOperator> {
        let metadata = self.metadata.clone();
        let mut metadata = metadata.write();
        let items = eval
            .items
            .iter()
            .map(|item| self.clone_outer_scalar_item(item, &mut metadata, derived_columns))
            .collect::<Result<_>>()?;
        Ok(EvalScalar { items }.into())
    }

    fn clone_outer_scalar_item(
        &mut self,
        ScalarItem { scalar, index }: &ScalarItem,
        metadata: &mut Metadata,
        derived_columns: &mut DerivedColumnMap,
    ) -> Result<ScalarItem> {
        let mut scalar = scalar.clone();
        let index = *index;
        match scalar {
            ScalarExpr::BoundColumnRef(ref mut column_ref) if column_ref.column.index == index => {
                let new_index = Self::get_derived(derived_columns, index)?;
                column_ref.column.index = new_index;
                Ok(ScalarItem {
                    scalar,
                    index: new_index,
                })
            }
            _ => {
                for old in scalar.used_columns() {
                    scalar.replace_column(old, Self::get_derived(derived_columns, old)?)?;
                }
                let column_entry = metadata.column(index);
                let name = column_entry.name();
                let data_type = column_entry.data_type();
                let old = index;
                let index = metadata.add_derived_column(name, data_type);
                derived_columns.insert(old, index);
                Ok(ScalarItem { scalar, index })
            }
        }
    }

    fn scalar_item_from_index(index: Symbol, name_prefix: &str, metadata: &Metadata) -> ScalarItem {
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

    pub(crate) fn get_derived(derived_columns: &DerivedColumnMap, old: Symbol) -> Result<Symbol> {
        derived_columns
            .get(&old)
            .copied()
            .ok_or_else(|| ErrorCode::Internal(format!("Missing derived column {old}")))
    }
}

enum Item<'a> {
    Scalar(&'a ScalarItem),
    Index(Symbol),
}
