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

use common_exception::Result;

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::ColumnBinding;
use crate::Visibility;

pub struct RuleNormalizeAggregate {
    id: RuleID,
    patterns: Vec<SExpr>,
}

impl RuleNormalizeAggregate {
    pub fn new() -> Self {
        Self {
            id: RuleID::NormalizeAggregate,
            // Aggregate
            //  \
            //   *
            patterns: vec![SExpr::create_unary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Aggregate,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_leaf(Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Pattern,
                    }
                    .into(),
                ))),
            )],
        }
    }
}

impl Rule for RuleNormalizeAggregate {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let aggregate: Aggregate = s_expr.plan().clone().try_into()?;

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
            return Ok(());
        }

        let new_aggregate = Aggregate {
            mode: aggregate.mode,
            group_items: aggregate.group_items,
            aggregate_functions: new_aggregate_functions,
            from_distinct: aggregate.from_distinct,
            limit: aggregate.limit,
            grouping_sets: aggregate.grouping_sets,
        };

        let mut new_aggregate = SExpr::create_unary(
            Arc::new(new_aggregate.into()),
            Arc::new(s_expr.child(0)?.clone()),
        );
        new_aggregate.set_applied_rule(&self.id);

        if let Some((work_index, work_c)) = work_expr {
            if alias_functions_index.len() < 2 {
                state.add_result(new_aggregate);
                return Ok(());
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
                                virtual_computed_expr: None,
                                data_type: work_c.return_type.clone(),
                                visibility: Visibility::Visible,
                                column_name: work_c.display_name.clone(),
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

            new_aggregate.set_applied_rule(&self.id);
            state.add_result(new_aggregate);
            Ok(())
        } else {
            state.add_result(new_aggregate);
            Ok(())
        }
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}
