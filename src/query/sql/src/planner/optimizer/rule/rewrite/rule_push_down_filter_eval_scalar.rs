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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::RuleID;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::ColumnSet;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::plans::AggregateFunction;
use crate::plans::CastExpr;
use crate::plans::EvalScalar;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::WindowFunc;
use crate::plans::WindowFuncType;
use crate::plans::WindowOrderBy;
use crate::MetadataRef;

pub struct RulePushDownFilterEvalScalar {
    id: RuleID,
    patterns: Vec<SExpr>,
    metadata: MetadataRef,
}

impl RulePushDownFilterEvalScalar {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::PushDownFilterEvalScalar,
            // Filter
            //  \
            //   EvalScalar
            //    \
            //     *
            patterns: vec![SExpr::create_unary(
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
            )],
            metadata,
        }
    }

    // Replace predicate with children scalar items
    fn replace_predicate(predicate: &ScalarExpr, items: &[ScalarItem]) -> Result<ScalarExpr> {
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
            ScalarExpr::WindowFunction(window) => {
                let func = match &window.func {
                    WindowFuncType::Aggregate(agg) => {
                        let args = agg
                            .args
                            .iter()
                            .map(|arg| Self::replace_predicate(arg, items))
                            .collect::<Result<Vec<ScalarExpr>>>()?;

                        WindowFuncType::Aggregate(AggregateFunction {
                            func_name: agg.func_name.clone(),
                            distinct: agg.distinct,
                            params: agg.params.clone(),
                            args,
                            return_type: agg.return_type.clone(),
                            display_name: agg.display_name.clone(),
                        })
                    }
                    func => func.clone(),
                };

                let partition_by = window
                    .partition_by
                    .iter()
                    .map(|arg| Self::replace_predicate(arg, items))
                    .collect::<Result<Vec<ScalarExpr>>>()?;

                let order_by = window
                    .order_by
                    .iter()
                    .map(|arg| {
                        Ok(WindowOrderBy {
                            asc: arg.asc,
                            nulls_first: arg.nulls_first,
                            expr: Self::replace_predicate(&arg.expr, items)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(ScalarExpr::WindowFunction(WindowFunc {
                    display_name: window.display_name.clone(),
                    func,
                    partition_by,
                    order_by,
                    frame: window.frame.clone(),
                }))
            }
            ScalarExpr::AggregateFunction(agg_func) => {
                let args = agg_func
                    .args
                    .iter()
                    .map(|arg| Self::replace_predicate(arg, items))
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
                    .map(|arg| Self::replace_predicate(arg, items))
                    .collect::<Result<Vec<ScalarExpr>>>()?;

                Ok(ScalarExpr::FunctionCall(FunctionCall {
                    span: func.span,
                    params: func.params.clone(),
                    arguments,
                    func_name: func.func_name.clone(),
                }))
            }
            ScalarExpr::CastExpr(cast) => {
                let arg = Self::replace_predicate(&cast.argument, items)?;
                Ok(ScalarExpr::CastExpr(CastExpr {
                    span: cast.span,
                    is_try: cast.is_try,
                    argument: Box::new(arg),
                    target_type: cast.target_type.clone(),
                }))
            }
            _ => Ok(predicate.clone()),
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

        let metadata = self.metadata.read();
        let table_entries = metadata.tables();
        let is_source_of_view = table_entries.iter().any(|t| t.is_source_of_view());

        // Replacing `DerivedColumn` in `Filter` with the column expression defined in the view.
        // This allows us to eliminate the `EvalScalar` and push the filter down to the `Scan`.
        if (used_columns.is_subset(&eval_scalar_prop.output_columns)
            && !used_columns.is_subset(&eval_scalar_child_prop.output_columns))
            || is_source_of_view
        {
            let new_predicates = &filter
                .predicates
                .iter()
                .map(|predicate| Self::replace_predicate(predicate, &eval_scalar.items))
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

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}
