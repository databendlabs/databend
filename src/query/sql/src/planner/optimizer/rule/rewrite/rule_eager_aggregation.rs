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

use common_functions::aggregates::AggregateFunctionFactory;
use common_expression::types::number::NumberDataType;
use common_expression::types::DataType;

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::ColumnSet;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::ColumnBinding;
use crate::IndexType;
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
}

impl RuleEagerAggregation {
    pub fn new() -> Self {
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

        // dbg!("columns_set = {:?}", &columns_set);

        // Divide the aggregate functions into left and right based on the columns in the aggregate function.
        let aggregations = vec![
            get_aggregation_functions(&agg_final, &columns_set[0]),
            get_aggregation_functions(&agg_final, &columns_set[1]),
        ];

        // dbg!("aggregations = {:?}", &aggregations);

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

        // Obtain the columns that exist in both the group by column and the join condition columns.
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

        // dbg!("group_columns_set = {:?}", &group_columns_set);

        let factory = AggregateFunctionFactory::instance();
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
                let eager_agg_final = agg_final.clone();
                let eager_agg_partial = agg_partial.clone();
                for (index, aggregation_function_index, func_name) in aggregations[d].iter() {
                    if !factory.is_decomposable(func_name) {
                        continue;
                    }

                    let aggregation_functions = vec![&mut agg_partial.aggregate_functions[*index], &mut agg_final.aggregate_functions[*index]];
                    for aggregate_function in aggregation_functions {
                        if let ScalarExpr::AggregateFunction(agg) = &mut aggregate_function.scalar {
                            let data_type = match agg.func_name.as_str() {
                                "count" => Box::new(DataType::Number(NumberDataType::UInt64)),
                                "sum" => Box::new(DataType::Number(NumberDataType::Int64)),
                                _ => Box::new(agg.args[0].data_type()?.clone()),
                            };
                            agg.args[0] = ScalarExpr::BoundColumnRef(BoundColumnRef {
                                span: None,
                                column: ColumnBinding {
                                    database_name: None,
                                    table_name: None,
                                    column_name: "eager_aggregation".to_string(),
                                    index: *aggregation_function_index,
                                    data_type: data_type,
                                    visibility: Visibility::Visible,
                                },
                            });
                            if agg.func_name.as_str() == "count" {
                                agg.func_name = "sum".to_string();
                                agg.return_type = Box::new(DataType::Nullable(Box::new(
                                    DataType::Number(NumberDataType::UInt64),
                                )));
                                for item in &mut eval_scalar.items {
                                    if let ScalarExpr::BoundColumnRef(column) = &mut item.scalar {
                                        let column_binding = &mut column.column;
                                        if column_binding.index == aggregate_function.index {
                                            column_binding.data_type = Box::new(DataType::Nullable(Box::new(
                                                DataType::Number(NumberDataType::UInt64),
                                            )));
                                        }
                                    }
                                }
                            }
                        }
                    }
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

fn get_aggregation_functions(
    agg_final: & Aggregate,
    columns_set: &ColumnSet,
) -> Vec<(usize, IndexType, String)> {
    agg_final
        .aggregate_functions
        .iter()
        .enumerate()
        .filter_map(|(index, aggregate_item)| match &aggregate_item.scalar {
            ScalarExpr::AggregateFunction(aggregate_function)
                if aggregate_function.args.len() == 1 =>
            {
                match &aggregate_function.args[0] {
                    ScalarExpr::BoundColumnRef(column)
                        if matches!(*column.column.data_type, DataType::Number(_))
                            && columns_set.contains(&column.column.index) =>
                    {
                        Some((index, aggregate_item.index, aggregate_function.func_name.clone()))
                    }
                    _ => None,
                }
            }
            _ => None,
        })
        .collect::<Vec<_>>()
}
