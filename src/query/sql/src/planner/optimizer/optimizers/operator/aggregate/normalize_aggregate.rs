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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::Result;

use crate::ColumnBinding;
use crate::Visibility;
use crate::optimizer::Optimizer;
use crate::optimizer::ir::SExpr;
use crate::plans::Aggregate;
use crate::plans::AggregateMode;
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

    #[stacksafe::stacksafe]
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
        if let Some(rewritten) = self.rewrite_distinct_count(&aggregate, s_expr)? {
            return Ok(rewritten);
        }
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
                    && aggregate.grouping_sets.is_none()
                    && function.args.iter().all(|expr| {
                        if let ScalarExpr::BoundColumnRef(r) = expr {
                            aggregate
                                .group_items
                                .iter()
                                .any(|item| item.index == r.column.index)
                        } else {
                            false
                        }
                    })
                    && function.args.iter().all(|expr| {
                        !expr
                            .data_type()
                            .map(|t| t.is_nullable_or_null())
                            .unwrap_or(true)
                    });

                if distinct_on_group_key {
                    rewritten = true;

                    // Grouping sets rewrite will wrap grouping keys into nullable and inject NULLs
                    // for sets where the key is absent, so treat them as nullable even if the
                    // original column type is non-nullable.
                    let mut nullable = function.args[0].data_type()?.is_nullable_or_null();
                    if !nullable {
                        if let Some(grouping_sets) = &aggregate.grouping_sets {
                            if !grouping_sets.sets.is_empty() {
                                nullable = true;
                            }
                        }
                    }

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

    // Rewrite `count(distinct a, b, ..)` to `count(*)` over an inner `GROUP BY`
    // that deduplicates the distinct arguments.
    fn rewrite_distinct_count(
        &self,
        aggregate: &Aggregate,
        s_expr: &SExpr,
    ) -> Result<Option<SExpr>> {
        if aggregate.mode != AggregateMode::Initial
            || aggregate.grouping_sets.is_some()
            || aggregate.aggregate_functions.len() != 1
            || s_expr.arity() != 1
            || !aggregate.group_items.is_empty()
        {
            return Ok(None);
        }

        let agg_item = &aggregate.aggregate_functions[0];
        let ScalarExpr::AggregateFunction(function) = &agg_item.scalar else {
            return Ok(None);
        };

        let is_distinct_count = (function.func_name == "count" && function.distinct)
            || function.func_name == "count_distinct";
        if !is_distinct_count || function.args.len() <= 1 {
            return Ok(None);
        }

        // Skip rewrite when any argument is nullable or Null, to preserve NULL-skipping semantics.
        if function.args.iter().any(|expr| {
            expr.data_type()
                .map(|t| t.is_nullable_or_null())
                .unwrap_or(true)
        }) {
            return Ok(None);
        }

        let mut new_group_items = aggregate.group_items.clone();
        let mut seen = HashSet::with_capacity(new_group_items.len() + function.args.len());
        for item in &new_group_items {
            seen.insert(item.index);
        }

        for arg in &function.args {
            let ScalarExpr::BoundColumnRef(column) = arg else {
                return Ok(None);
            };
            if seen.insert(column.column.index) {
                new_group_items.push(ScalarItem {
                    index: column.column.index,
                    scalar: arg.clone(),
                });
            }
        }

        let inner_aggregate = Aggregate {
            mode: AggregateMode::Initial,
            group_items: new_group_items,
            aggregate_functions: vec![],
            from_distinct: aggregate.from_distinct,
            rank_limit: None,
            grouping_sets: None,
        };

        let mut new_function = function.clone();
        new_function.func_name = "count".to_string();
        new_function.distinct = false;
        new_function.args = vec![];
        new_function.sort_descs = vec![];

        let outer_aggregate = Aggregate {
            mode: aggregate.mode,
            group_items: aggregate.group_items.clone(),
            aggregate_functions: vec![ScalarItem {
                index: agg_item.index,
                scalar: ScalarExpr::AggregateFunction(new_function),
            }],
            from_distinct: aggregate.from_distinct,
            rank_limit: aggregate.rank_limit.clone(),
            grouping_sets: aggregate.grouping_sets.clone(),
        };

        Ok(Some(SExpr::create_unary(
            Arc::new(outer_aggregate.into()),
            Arc::new(SExpr::create_unary(
                Arc::new(inner_aggregate.into()),
                Arc::new(s_expr.child(0)?.clone()),
            )),
        )))
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
