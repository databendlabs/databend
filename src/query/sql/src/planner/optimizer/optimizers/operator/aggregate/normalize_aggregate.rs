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

use std::sync::Arc;

use databend_common_exception::Result;

use crate::ColumnBinding;
use crate::Visibility;
use crate::optimizer::Optimizer;
use crate::optimizer::ir::SExpr;
use crate::plans::Aggregate;
use crate::plans::BoundColumnRef;
use crate::plans::ConstantExpr;
use crate::plans::EvalScalar;
use crate::plans::FunctionCall;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;

pub struct RuleNormalizeAggregateOptimizer {}

impl RuleNormalizeAggregateOptimizer {
    pub fn new() -> Self {
        RuleNormalizeAggregateOptimizer {}
    }

    #[recursive::recursive]
    pub fn optimize_sync(&self, s_expr: &SExpr) -> Result<SExpr> {
        let mut children = Vec::with_capacity(s_expr.arity());
        for child in s_expr.children() {
            let child = self.optimize_sync(child)?;
            children.push(Arc::new(child));
        }
        let s_expr = s_expr.replace_children(children);
        if let RelOperator::Aggregate(_) = s_expr.plan.as_ref() {
            self.normalize_aggregate(&s_expr)
        } else {
            Ok(s_expr)
        }
    }

    fn normalize_aggregate(&self, s_expr: &SExpr) -> Result<SExpr> {
        let aggregate: Aggregate = s_expr.plan().clone().try_into()?;
        let mut work_expr = None;
        let mut alias_functions_index = vec![];
        let mut new_aggregate_functions = Vec::with_capacity(aggregate.aggregate_functions.len());
        let mut post_aggregate_scalars = Vec::new();

        let mut rewritten = false;

        for aggregate_function in &aggregate.aggregate_functions {
            if let ScalarExpr::AggregateFunction(function) = &aggregate_function.scalar {
                if !function.distinct
                    && function.func_name == "count"
                    && (function.args.is_empty()
                        || !function.args[0].data_type()?.is_nullable_or_null())
                {
                    rewritten = true;
                    if work_expr.is_none() {
                        let mut new_function = function.clone();
                        new_function.args = vec![];

                        work_expr = Some((aggregate_function.index, function.clone()));
                        new_aggregate_functions.push(ScalarItem {
                            index: aggregate_function.index,
                            scalar: ScalarExpr::AggregateFunction(new_function),
                        });
                    }

                    alias_functions_index.push((aggregate_function.index, function.clone()));
                    continue;
                }

                // rewrite count(distinct item)/uniq/count_distinct on grouping key to 1 (or 0 if null)
                let distinct_on_group_key = ((function.distinct && function.func_name == "count")
                    || function.func_name == "uniq"
                    || function.func_name == "count_distinct")
                    && function.args.len() == 1
                    && function.args.iter().all(|expr| {
                        if let ScalarExpr::BoundColumnRef(r) = expr {
                            aggregate
                                .group_items
                                .iter()
                                .any(|item| item.index == r.column.index)
                        } else {
                            false
                        }
                    });

                if distinct_on_group_key {
                    rewritten = true;

                    let nullable = function.args[0].data_type()?.is_nullable_or_null();

                    let scalar = if nullable {
                        let not_null_check = ScalarExpr::FunctionCall(FunctionCall {
                            span: None,
                            func_name: "is_not_null".to_string(),
                            params: vec![],
                            arguments: vec![function.args[0].clone()],
                        });

                        ScalarExpr::FunctionCall(FunctionCall {
                            span: None,
                            func_name: "if".to_string(),
                            params: vec![],
                            arguments: vec![
                                not_null_check,
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: None,
                                    value: 1u64.into(),
                                }),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: None,
                                    value: 0u64.into(),
                                }),
                            ],
                        })
                    } else {
                        ScalarExpr::ConstantExpr(ConstantExpr {
                            span: None,
                            value: 1u64.into(),
                        })
                    };

                    post_aggregate_scalars.push(ScalarItem {
                        index: aggregate_function.index,
                        scalar,
                    });

                    continue;
                }
            }

            new_aggregate_functions.push(aggregate_function.clone());
        }

        if !rewritten {
            return Ok(s_expr.clone());
        }

        let new_aggregate = Aggregate {
            mode: aggregate.mode,
            group_items: aggregate.group_items,
            aggregate_functions: new_aggregate_functions,
            from_distinct: aggregate.from_distinct,
            rank_limit: aggregate.rank_limit,
            grouping_sets: aggregate.grouping_sets,
        };

        let mut new_aggregate = SExpr::create_unary(
            Arc::new(new_aggregate.into()),
            Arc::new(s_expr.child(0)?.clone()),
        );

        let mut scalar_items = Vec::new();

        if let Some((work_index, work_c)) = work_expr {
            if alias_functions_index.len() >= 2 {
                for (alias_function_index, _alias_function) in alias_functions_index {
                    scalar_items.push(ScalarItem {
                        index: alias_function_index,
                        scalar: ScalarExpr::BoundColumnRef(BoundColumnRef {
                            span: None,
                            column: ColumnBinding {
                                table_name: None,
                                table_index: None,
                                database_name: None,
                                column_position: None,
                                index: work_index,
                                virtual_expr: None,
                                data_type: work_c.return_type.clone(),
                                visibility: Visibility::Visible,
                                column_name: work_c.display_name.clone(),
                                is_srf: false,
                            },
                        }),
                    })
                }
            }
        }

        scalar_items.extend(post_aggregate_scalars);

        if !scalar_items.is_empty() {
            new_aggregate = SExpr::create_unary(
                Arc::new(
                    EvalScalar {
                        items: scalar_items,
                    }
                    .into(),
                ),
                Arc::new(new_aggregate),
            );
        }

        Ok(new_aggregate)
    }
}

impl Default for RuleNormalizeAggregateOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Optimizer for RuleNormalizeAggregateOptimizer {
    fn name(&self) -> String {
        "RuleNormalizeAggregateOptimizer".to_string()
    }

    async fn optimize(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        self.optimize_sync(s_expr)
    }
}
