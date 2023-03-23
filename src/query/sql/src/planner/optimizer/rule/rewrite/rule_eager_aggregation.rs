// Copyright 2023 Datafuse Labs.
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

use common_expression::types::number::NumberDataType;
use common_expression::types::DataType;
use common_functions::aggregates::AggregateFunctionFactory;

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::ColumnSet;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::EvalScalar;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::ColumnBinding;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::Visibility;

/// Rule to push aggregation past a join to reduces the number of input rows to the join.
/// Specific details can be found in `Eager Aggregation and Lazy Aggregation`.
///
/// eager group by:
/// input:        Expression
///                   |   
///            Aggregate(Final)
///                   |
///           Aggregate(Partial)
///                   |
///                  Join
///                 /    \
///                *      *
///
/// output:       Expression
///                   |
///            Aggregate(Final)
///                   |
///           Aggregate(Partial)
///                   |
///                  Join
///                 /    \
///                *      Aggregate(Final)
///                        \
///                         Aggregate(Partial)

pub struct RuleEagerAggregation {
    id: RuleID,
    pattern: SExpr,
    metadata: MetadataRef,
}

impl RuleEagerAggregation {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::EagerAggregation,
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::EvalScalar,
                }
                .into(),
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::Aggregate,
                    }
                    .into(),
                    SExpr::create_unary(
                        PatternPlan {
                            plan_type: RelOp::Aggregate,
                        }
                        .into(),
                        SExpr::create_binary(
                            PatternPlan {
                                plan_type: RelOp::Join,
                            }
                            .into(),
                            SExpr::create_leaf(
                                PatternPlan {
                                    plan_type: RelOp::Pattern,
                                }
                                .into(),
                            ),
                            SExpr::create_leaf(
                                PatternPlan {
                                    plan_type: RelOp::Pattern,
                                }
                                .into(),
                            ),
                        ),
                    ),
                ),
            ),
            metadata,
        }
    }
}

impl Rule for RuleEagerAggregation {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, a_expr: &SExpr, state: &mut TransformResult) -> common_exception::Result<()> {
        let eval_scalar_expr = a_expr;
        let agg_final_expr = eval_scalar_expr.child(0)?;
        let agg_partial_expr = agg_final_expr.child(0)?;
        let join_expr = agg_partial_expr.child(0)?;
        let join: Join = join_expr.plan().clone().try_into()?;

        // Only supports inner join and cross join.
        if !matches!(join.join_type, JoinType::Inner | JoinType::Cross) {
            return Ok(());
        }

        let mut eval_scalar: EvalScalar = eval_scalar_expr.plan().clone().try_into()?;
        let mut agg_final: Aggregate = agg_final_expr.plan().clone().try_into()?;
        let mut agg_partial: Aggregate = agg_partial_expr.plan().clone().try_into()?;

        // Get the original column set from the left child and right child of join.
        let mut columns_set = vec![ColumnSet::new(), ColumnSet::new()];
        get_columns_set(&join_expr.children[0], &mut columns_set[0]);
        get_columns_set(&join_expr.children[1], &mut columns_set[1]);

        let eval_scalar_items: HashMap<usize, usize> = eval_scalar
            .items
            .iter()
            .enumerate()
            .filter_map(|(index, item)| match &item.scalar {
                ScalarExpr::BoundColumnRef(column) => Some((column.column.index, index)),
                _ => None,
            })
            .collect();

        let function_factory = AggregateFunctionFactory::instance();

        // Get valid aggregate functions.
        let mut aggregations = vec![
            get_valid_aggregation_functions(
                &agg_final,
                &columns_set[0],
                &eval_scalar_items,
                function_factory,
            ),
            get_valid_aggregation_functions(
                &agg_final,
                &columns_set[1],
                &eval_scalar_items,
                function_factory,
            ),
        ];

        if aggregations[0].is_empty() && aggregations[1].is_empty() {
            return Ok(());
        }

        let mut group_columns_set = vec![ColumnSet::new(), ColumnSet::new()];
        for item in agg_final.group_columns()?.iter() {
            for idx in 0..2 {
                if columns_set[idx].contains(item) {
                    group_columns_set[idx].insert(*item);
                }
            }
        }

        let mut is_eager = [true, true];
        for cond in join.left_conditions.iter() {
            for c in cond.used_columns().iter() {
                if !group_columns_set[0].contains(c) {
                    is_eager[0] = false;
                }
            }
        }
        for cond in join.right_conditions.iter() {
            for c in cond.used_columns().iter() {
                if !group_columns_set[1].contains(c) {
                    is_eager[1] = false;
                }
            }
        }

        let metadata = self.metadata.clone();
        let mut metadata = metadata.write();
        let mut column_index = 0;

        let d = if aggregations[0].is_empty() { 1 } else { 0 };
        if is_eager[d] && is_eager[d ^ 1] {
            // TODO(dousir9):
            // if aggregations[d ^ 1].is_empty() {
            //     apply double eager on both.
            // } else {
            //     apply eager split on both.
            //     apply eager groupby-count on d.
            // }
        } else if is_eager[d] && !is_eager[d ^ 1] {
            if aggregations[d ^ 1].is_empty() {
                // Apply eager group by on d
                let mut success = false;
                // Check if there is an AVG aggregate function, if so, convert it to SUM and add a COUNT aggregate function.
                // TODO(dousir9): add comments in the future.
                let mut avg_components = HashMap::new();
                let mut avg_count_aggregations = Vec::new();
                for (index, func_name) in aggregations[d].iter_mut() {
                    if func_name == "avg" {
                        *func_name = "sum".to_string();
                        agg_partial
                            .aggregate_functions
                            .push(agg_partial.aggregate_functions[*index].clone());
                        agg_final
                            .aggregate_functions
                            .push(agg_final.aggregate_functions[*index].clone());

                        let sum_aggregation_functions = vec![
                            &mut agg_partial.aggregate_functions[*index],
                            &mut agg_final.aggregate_functions[*index],
                        ];
                        let sum_index = sum_aggregation_functions[0].index;
                        for aggregate_function in sum_aggregation_functions {
                            if let ScalarExpr::AggregateFunction(agg) =
                                &mut aggregate_function.scalar
                            {
                                agg.func_name = "sum".to_string();
                                if let ScalarExpr::BoundColumnRef(column) = &agg.args[0] {
                                    metadata.change_derived_column_alias(
                                        aggregate_function.index,
                                        format!(
                                            "{}({}.{})",
                                            agg.func_name.clone(),
                                            &column
                                                .column
                                                .table_name
                                                .clone()
                                                .unwrap_or("".to_string()),
                                            &column.column.column_name.clone(),
                                        ),
                                    );
                                }
                                // // TODO(dousir9): add comments in the future.
                                agg.return_type = Box::new(DataType::Nullable(Box::new(
                                    DataType::Number(NumberDataType::Int64),
                                )))
                            }
                        }

                        let last_index = agg_partial.aggregate_functions.len() - 1;
                        let count_aggregation_functions = vec![
                            &mut agg_partial.aggregate_functions[last_index],
                            &mut agg_final.aggregate_functions[last_index],
                        ];
                        let new_index = metadata.add_derived_column(
                            format!("_{}_eager_{}", &func_name, column_index),
                            count_aggregation_functions[0].scalar.data_type()?,
                        );
                        column_index += 1;
                        for aggregate_function in count_aggregation_functions {
                            if let ScalarExpr::AggregateFunction(agg) =
                                &mut aggregate_function.scalar
                            {
                                agg.func_name = "count".to_string();
                                agg.return_type =
                                    Box::new(DataType::Number(NumberDataType::UInt64));
                            }
                            aggregate_function.index = new_index;
                        }

                        avg_components.insert(sum_index, new_index);
                        avg_count_aggregations.push((last_index, "count".to_string()));
                    }
                }

                let eager_agg_final = agg_final.clone();
                let eager_agg_partial = agg_partial.clone();
                aggregations[d].extend(avg_count_aggregations);
                let mut old_to_new = HashMap::new();

                // The aggregations[d] here only contains MIN, MAX, SUM and COUNT,
                // and the AVG has been transformed into SUM and COUNT.
                for (index, func_name) in aggregations[d].iter() {
                    let aggregation_functions = vec![
                        &mut agg_partial.aggregate_functions[*index],
                        &mut agg_final.aggregate_functions[*index],
                    ];
                    let old_index = aggregation_functions[0].index;
                    let new_index = metadata.add_derived_column(
                        format!("_{}_eager_{}", &func_name, column_index),
                        aggregation_functions[0].scalar.data_type()?,
                    );
                    column_index += 1;
                    old_to_new.insert(old_index, new_index);
                    for aggregate_function in aggregation_functions {
                        if let ScalarExpr::AggregateFunction(agg) = &mut aggregate_function.scalar {
                            modify_aggregate_function(agg, old_index);
                            aggregate_function.index = new_index;
                        }
                    }

                    // TODO(dousir9): add comments in the future.
                    if let Some(idx) = eval_scalar_items.get(&old_index) && !avg_components.contains_key(&old_index) {
                        let eval_scalar_item = &mut eval_scalar.items[*idx];
                        if let ScalarExpr::BoundColumnRef(column) = &mut eval_scalar_item.scalar {
                            let mut column_binding = &mut column.column;
                            if column_binding.index == old_index {
                                column_binding.index = new_index;
                                if func_name == "count" {
                                    column_binding.data_type = Box::new(DataType::Nullable(Box::new(
                                        DataType::Number(NumberDataType::UInt64),
                                    )));
                                    eval_scalar_item.scalar = ScalarExpr::CastExpr(CastExpr {
                                        span: None,
                                        is_try: false,
                                        argument: Box::new(eval_scalar_item.scalar.clone()),
                                        target_type: Box::new(DataType::Number(NumberDataType::UInt64)),
                                    });
                                }
                            }
                            success = true;
                        }
                    }
                }

                // AVG(C) = SUM(C) / COUNT(NOTNULL C)
                for (sum_index, count_index) in avg_components.iter() {
                    if let Some(idx) = eval_scalar_items.get(sum_index) {
                        let eval_scalar_item = &mut eval_scalar.items[*idx];
                        eval_scalar_item.scalar = ScalarExpr::FunctionCall(FunctionCall {
                            span: None,
                            func_name: "divide".to_string(),
                            params: vec![],
                            arguments: vec![
                                ScalarExpr::BoundColumnRef(BoundColumnRef {
                                    span: None,
                                    column: ColumnBinding {
                                        database_name: None,
                                        table_name: None,
                                        column_name: "_eager".to_string(),
                                        index: old_to_new[sum_index],
                                        data_type: Box::new(DataType::Number(
                                            NumberDataType::Float64,
                                        )),
                                        visibility: Visibility::Visible,
                                    },
                                }),
                                ScalarExpr::CastExpr(CastExpr {
                                    span: None,
                                    is_try: false,
                                    argument: Box::new(ScalarExpr::BoundColumnRef(
                                        BoundColumnRef {
                                            span: None,
                                            column: ColumnBinding {
                                                database_name: None,
                                                table_name: None,
                                                column_name: "_eager".to_string(),
                                                index: old_to_new[count_index],
                                                data_type: Box::new(DataType::Nullable(Box::new(
                                                    DataType::Number(NumberDataType::UInt64),
                                                ))),
                                                visibility: Visibility::Visible,
                                            },
                                        },
                                    )),
                                    target_type: Box::new(DataType::Number(NumberDataType::UInt64)),
                                }),
                            ],
                        });
                        success = true;
                    }
                }

                if !success {
                    return Ok(());
                }

                let join_expr = if d == 0 {
                    join_expr.replace_children(vec![
                        SExpr::create_unary(
                            RelOperator::Aggregate(eager_agg_final),
                            SExpr::create_unary(
                                RelOperator::Aggregate(eager_agg_partial),
                                join_expr.child(0)?.clone(),
                            ),
                        ),
                        join_expr.child(1)?.clone(),
                    ])
                } else {
                    join_expr.replace_children(vec![
                        join_expr.child(0)?.clone(),
                        SExpr::create_unary(
                            RelOperator::Aggregate(eager_agg_final),
                            SExpr::create_unary(
                                RelOperator::Aggregate(eager_agg_partial),
                                join_expr.child(1)?.clone(),
                            ),
                        ),
                    ])
                };

                let mut result = eval_scalar_expr
                    .replace_children(vec![
                        agg_final_expr
                            .replace_children(vec![
                                agg_partial_expr
                                    .replace_children(vec![join_expr])
                                    .replace_plan(agg_partial.try_into()?),
                            ])
                            .replace_plan(agg_final.try_into()?),
                    ])
                    .replace_plan(eval_scalar.try_into()?);

                result.set_applied_rule(&self.id);
                state.add_result(result);
            } else {
                // TODO(dousir9): apply eager groupby-count on d
            }
        } else if !is_eager[d] && is_eager[d ^ 1] {
            // TODO(dousir9):
            // if aggregations[d ^ 1].is_empty() {
            //     apply eager count on d^1.
            // } else {
            //     apply eager groupby-count on d^1.
            // }
        }
        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}

fn get_columns_set(s_expr: &SExpr, columns_set: &mut ColumnSet) {
    match &s_expr.plan {
        RelOperator::Scan(scan) => {
            columns_set.extend(scan.columns.clone());
        }
        RelOperator::Join(_) => {
            get_columns_set(&s_expr.children[0], columns_set);
            get_columns_set(&s_expr.children[1], columns_set);
        }
        RelOperator::UnionAll(union_all) => {
            columns_set.extend(union_all.used_columns().unwrap());
        }
        _ => {
            if !s_expr.children().is_empty() {
                get_columns_set(&s_expr.children[0], columns_set);
            }
        }
    }
}

fn get_valid_aggregation_functions(
    agg_final: &Aggregate,
    columns_set: &ColumnSet,
    eval_scalar_items: &HashMap<usize, usize>,
    function_factory: &AggregateFunctionFactory,
) -> Vec<(usize, String)> {
    agg_final
        .aggregate_functions
        .iter()
        .enumerate()
        .filter_map(|(index, aggregate_item)| match &aggregate_item.scalar {
            ScalarExpr::AggregateFunction(aggregate_function)
                if aggregate_function.args.len() == 1
                    && function_factory.is_decomposable(&aggregate_function.func_name)
                    && eval_scalar_items.contains_key(&aggregate_item.index) =>
            {
                match &aggregate_function.args[0] {
                    ScalarExpr::BoundColumnRef(column) => match &*column.column.data_type {
                        DataType::Number(_) if columns_set.contains(&column.column.index) => {
                            Some((index, aggregate_function.func_name.clone()))
                        }
                        DataType::Nullable(ty) => match **ty {
                            DataType::Number(_) if columns_set.contains(&column.column.index) => {
                                Some((index, aggregate_function.func_name.clone()))
                            }
                            _ => None,
                        },
                        _ => None,
                    },
                    _ => None,
                }
            }
            _ => None,
        })
        .collect::<Vec<_>>()
}

fn modify_aggregate_function(agg: &mut AggregateFunction, args_index: usize) {
    agg.args[0] = ScalarExpr::BoundColumnRef(BoundColumnRef {
        span: None,
        column: ColumnBinding {
            database_name: None,
            table_name: None,
            column_name: "_eager".to_string(),
            index: args_index,
            data_type: agg.return_type.clone(),
            visibility: Visibility::Visible,
        },
    });

    if agg.func_name.as_str() == "count" {
        agg.func_name = "sum".to_string();
        agg.return_type = Box::new(DataType::Nullable(Box::new(DataType::Number(
            NumberDataType::UInt64,
        ))));
    }
}
