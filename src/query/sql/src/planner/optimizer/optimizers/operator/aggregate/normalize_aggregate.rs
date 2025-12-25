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
use crate::plans::EvalScalar;
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
        if let Some(rewritten) = self.rewrite_distinct_count(&aggregate, s_expr)? {
            return Ok(rewritten);
        }
        let mut work_expr = None;
        let mut alias_functions_index = vec![];
        let mut new_aggregate_functions = Vec::with_capacity(aggregate.aggregate_functions.len());

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

                // rewrite count(distinct items) to count() if items in group by
                let distinct_eliminated = ((function.distinct && function.func_name == "count")
                    || function.func_name == "uniq"
                    || function.func_name == "count_distinct")
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

                if distinct_eliminated {
                    rewritten = true;
                    let mut new_function = function.clone();
                    new_function.args = vec![];
                    new_function.func_name = "count".to_string();

                    new_aggregate_functions.push(ScalarItem {
                        index: aggregate_function.index,
                        scalar: ScalarExpr::AggregateFunction(new_function),
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

        if let Some((work_index, work_c)) = work_expr {
            if alias_functions_index.len() < 2 {
                return Ok(new_aggregate);
            }
            if !alias_functions_index.is_empty() {
                let mut scalar_items = Vec::with_capacity(alias_functions_index.len());
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
        } else {
            Ok(new_aggregate)
        }
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

        let args_in_group = function.args.iter().all(|expr| {
            if let ScalarExpr::BoundColumnRef(r) = expr {
                aggregate
                    .group_items
                    .iter()
                    .any(|item| item.index == r.column.index)
            } else {
                false
            }
        });
        if args_in_group {
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
