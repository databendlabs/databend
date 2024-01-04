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

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Filter;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOp::Pattern;
use crate::plans::Window;

/// Input:   Filter
///           \
///            Window
///             \
///              *
///
/// Output:
/// (1)      Window
///           \
///            Filter
///             \
///              *
///
/// (2)
///          Filter(remaining)
///           \
///            Window
///             \
///              Filter(pushed down)
///               \
///                *
///
/// note that only push down filter used in `Window.partition_by` columns
pub struct RulePushDownFilterWindow {
    id: RuleID,
    patterns: Vec<SExpr>,
}

impl RulePushDownFilterWindow {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownFilterWindow,
            patterns: vec![SExpr::create_unary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Filter,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Window,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan { plan_type: Pattern }.into(),
                    ))),
                )),
            )],
        }
    }
}

impl Rule for RulePushDownFilterWindow {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut TransformResult,
    ) -> databend_common_exception::Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let window_expr = s_expr.child(0)?;
        let window: Window = window_expr.plan().clone().try_into()?;
        let partition_by_columns = window.partition_by_columns()?;

        let mut pushed_down_predicates = vec![];
        let mut remaining_predicates = vec![];
        for predicate in filter.predicates.into_iter() {
            let predicate_used_columns = predicate.used_columns();
            if predicate_used_columns.is_subset(&partition_by_columns) {
                pushed_down_predicates.push(predicate);
            } else {
                remaining_predicates.push(predicate)
            }
        }

        if !pushed_down_predicates.is_empty() {
            let pushed_down_filter = Filter {
                predicates: pushed_down_predicates,
            };
            let result = if remaining_predicates.is_empty() {
                SExpr::create_unary(
                    Arc::new(window.into()),
                    Arc::new(SExpr::create_unary(
                        Arc::new(pushed_down_filter.into()),
                        Arc::new(window_expr.child(0)?.clone()),
                    )),
                )
            } else {
                let remaining_filter = Filter {
                    predicates: remaining_predicates,
                };
                let mut s_expr = SExpr::create_unary(
                    Arc::new(remaining_filter.into()),
                    Arc::new(SExpr::create_unary(
                        Arc::new(window.into()),
                        Arc::new(SExpr::create_unary(
                            Arc::new(pushed_down_filter.into()),
                            Arc::new(window_expr.child(0)?.clone()),
                        )),
                    )),
                );
                s_expr.set_applied_rule(&self.id);
                s_expr
            };
            state.add_result(result);
        }

        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}
