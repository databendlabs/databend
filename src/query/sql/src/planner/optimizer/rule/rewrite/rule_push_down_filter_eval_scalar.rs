// Copyright 2022 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::RuleID;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::ColumnSet;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::plans::AggregateFunction;
use crate::plans::AndExpr;
use crate::plans::CastExpr;
use crate::plans::ComparisonExpr;
use crate::plans::EvalScalar;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::NotExpr;
use crate::plans::OrExpr;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::WindowFunc;

pub struct RulePushDownFilterEvalScalar {
    id: RuleID,
    pattern: SExpr,
}

impl RulePushDownFilterEvalScalar {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownFilterEvalScalar,
            // Filter
            //  \
            //   EvalScalar
            //    \
            //     *
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Filter,
                }
                .into(),
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::EvalScalar,
                    }
                    .into(),
                    SExpr::create_leaf(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ),
                ),
            ),
        }
    }

    // Replace predicate with children scalar items
    fn replace_predicate(
        predicate: &ScalarExpr,
        items: &[ScalarItem],
        eval_scalar_columns: &ColumnSet,
        eval_scalar_child_columns: &ColumnSet,
    ) -> Result<ScalarExpr> {
        if !predicate
            .used_columns()
            .is_subset(eval_scalar_child_columns)
            && predicate.used_columns().is_subset(eval_scalar_columns)
        {
            match predicate {
                ScalarExpr::BoundColumnRef(column) => {
                    for item in items {
                        if item.index == column.column.index {
                            return Ok(item.scalar.clone());
                        }
                    }
                    Err(ErrorCode::UnknownColumn(format!(
                        "Cannot find column to replace `{}`(#{})",
                        column.column.column_name, column.column.index
                    )))
                }
                ScalarExpr::AndExpr(scalar) => {
                    let left = Self::replace_predicate(
                        &scalar.left,
                        items,
                        eval_scalar_columns,
                        eval_scalar_child_columns,
                    )?;
                    let right = Self::replace_predicate(
                        &scalar.right,
                        items,
                        eval_scalar_columns,
                        eval_scalar_child_columns,
                    )?;
                    Ok(ScalarExpr::AndExpr(AndExpr {
                        left: Box::new(left),
                        right: Box::new(right),
                    }))
                }
                ScalarExpr::OrExpr(scalar) => {
                    let left = Self::replace_predicate(
                        &scalar.left,
                        items,
                        eval_scalar_columns,
                        eval_scalar_child_columns,
                    )?;
                    let right = Self::replace_predicate(
                        &scalar.right,
                        items,
                        eval_scalar_columns,
                        eval_scalar_child_columns,
                    )?;
                    Ok(ScalarExpr::OrExpr(OrExpr {
                        left: Box::new(left),
                        right: Box::new(right),
                    }))
                }
                ScalarExpr::NotExpr(scalar) => {
                    let argument = Self::replace_predicate(
                        &scalar.argument,
                        items,
                        eval_scalar_columns,
                        eval_scalar_child_columns,
                    )?;
                    Ok(ScalarExpr::NotExpr(NotExpr {
                        argument: Box::new(argument),
                    }))
                }
                ScalarExpr::ComparisonExpr(scalar) => {
                    let left = Self::replace_predicate(
                        &scalar.left,
                        items,
                        eval_scalar_columns,
                        eval_scalar_child_columns,
                    )?;
                    let right = Self::replace_predicate(
                        &scalar.right,
                        items,
                        eval_scalar_columns,
                        eval_scalar_child_columns,
                    )?;
                    Ok(ScalarExpr::ComparisonExpr(ComparisonExpr {
                        op: scalar.op.clone(),
                        left: Box::new(left),
                        right: Box::new(right),
                    }))
                }
                ScalarExpr::WindowFunction(window) => {
                    let args = window
                        .agg_func
                        .args
                        .iter()
                        .map(|arg| {
                            Self::replace_predicate(
                                arg,
                                items,
                                eval_scalar_columns,
                                eval_scalar_child_columns,
                            )
                        })
                        .collect::<Result<Vec<ScalarExpr>>>()?;

                    let agg_func = AggregateFunction {
                        func_name: window.agg_func.func_name.clone(),
                        distinct: window.agg_func.distinct,
                        params: window.agg_func.params.clone(),
                        args,
                        return_type: window.agg_func.return_type.clone(),
                        display_name: window.agg_func.display_name.clone(),
                    };

                    Ok(ScalarExpr::WindowFunction(WindowFunc {
                        agg_func,
                        partition_by: window.partition_by.clone(),
                        order_by: window.order_by.clone(),
                        frame: window.frame.clone(),
                    }))
                }
                ScalarExpr::AggregateFunction(agg_func) => {
                    let args = agg_func
                        .args
                        .iter()
                        .map(|arg| {
                            Self::replace_predicate(
                                arg,
                                items,
                                eval_scalar_columns,
                                eval_scalar_child_columns,
                            )
                        })
                        .collect::<Result<Vec<ScalarExpr>>>()?;

                    Ok(ScalarExpr::AggregateFunction(AggregateFunction {
                        func_name: agg_func.func_name.clone(),
                        distinct: agg_func.distinct,
                        params: agg_func.params.clone(),
                        args,
                        return_type: agg_func.return_type.clone(),
                        display_name: agg_func.display_name.clone(),
                    }))
                }
                ScalarExpr::FunctionCall(func) => {
                    let arguments = func
                        .arguments
                        .iter()
                        .map(|arg| {
                            Self::replace_predicate(
                                arg,
                                items,
                                eval_scalar_columns,
                                eval_scalar_child_columns,
                            )
                        })
                        .collect::<Result<Vec<ScalarExpr>>>()?;

                    Ok(ScalarExpr::FunctionCall(FunctionCall {
                        span: func.span,
                        params: func.params.clone(),
                        arguments,
                        func_name: func.func_name.clone(),
                    }))
                }
                ScalarExpr::CastExpr(cast) => {
                    let arg = Self::replace_predicate(
                        &cast.argument,
                        items,
                        eval_scalar_columns,
                        eval_scalar_child_columns,
                    )?;
                    Ok(ScalarExpr::CastExpr(CastExpr {
                        span: cast.span,
                        is_try: cast.is_try,
                        argument: Box::new(arg),
                        target_type: cast.target_type.clone(),
                    }))
                }
                _ => Ok(predicate.clone()),
            }
        } else {
            Ok(predicate.clone())
        }
    }
}

impl Rule for RulePushDownFilterEvalScalar {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let mut filter: Filter = s_expr.plan().clone().try_into()?;

        let mut used_columns = ColumnSet::new();
        for pred in filter.predicates.iter() {
            used_columns = used_columns.union(&pred.used_columns()).cloned().collect();
        }

        let input = s_expr.child(0)?;
        let eval_scalar: EvalScalar = s_expr.child(0)?.plan().clone().try_into()?;

        let rel_expr = RelExpr::with_s_expr(input);
        let eval_scalar_child_prop = rel_expr.derive_relational_prop_child(0)?;

        let scalar_rel_expr = RelExpr::with_s_expr(s_expr);
        let eval_scalar_prop = scalar_rel_expr.derive_relational_prop_child(0)?;

        // Replacing `DerivedColumn` in `Filter` with the column expression defined in the view.
        // This allows us to eliminate the `EvalScalar` and push the filter down to the `Scan`.
        if used_columns.is_subset(&eval_scalar_prop.output_columns)
            && !used_columns.is_subset(&eval_scalar_child_prop.output_columns)
        {
            let new_predicates = &filter
                .predicates
                .iter()
                .map(|predicate| {
                    Self::replace_predicate(
                        predicate,
                        &eval_scalar.items,
                        &eval_scalar_prop.output_columns,
                        &eval_scalar_child_prop.output_columns,
                    )
                })
                .collect::<Result<Vec<ScalarExpr>>>()?;

            filter.predicates = new_predicates.to_vec();

            used_columns.clear();
            for pred in filter.predicates.iter() {
                used_columns = used_columns.union(&pred.used_columns()).cloned().collect();
            }
        }

        // Check if `Filter` can be satisfied by children of `EvalScalar`
        if used_columns.is_subset(&eval_scalar_child_prop.output_columns) {
            // TODO(leiysky): partial push down conjunctions
            // For example, `select a from (select a, a+1 as b from t) where a = 1 and b = 2`
            // can be optimized as `select a from (select a, a+1 as b from t where a = 1) where b = 2`
            let new_expr = SExpr::create_unary(
                eval_scalar.into(),
                SExpr::create_unary(filter.into(), input.child(0)?.clone()),
            );
            state.add_result(new_expr);
        }

        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
