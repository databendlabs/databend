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
use crate::optimizer::RelExpr;
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
pub struct RulePushDownFilterWindows {
    id: RuleID,
    patterns: Vec<SExpr>,
}

impl RulePushDownFilterWindows {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownFilterWindows,
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

impl Rule for RulePushDownFilterWindows {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut TransformResult,
    ) -> databend_common_exception::Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let window: Window = s_expr.child(0)?.plan().clone().try_into()?;
        let window_rel_expr = RelExpr::with_s_expr(s_expr);
        let window_prop = window_rel_expr.derive_relational_prop_child(0)?;
        let window_group_columns = window.group_columns()?;

        let mut remaining_predicates = vec![];
        let mut pushed_down_predicates = vec![];

        for predicate in filter.predicates.into_iter() {
            let predicate_used_columns = predicate.used_columns();
            if predicate_used_columns.is_subset(&window_prop.output_columns)
                && predicate_used_columns.is_subset(&window_group_columns)
            {
                pushed_down_predicates.push(predicate);
            } else {
                remaining_predicates.push(predicate)
            }
        }

        let mut result = s_expr.child(0)?.child(0)?.clone();

        if !pushed_down_predicates.is_empty() {
            let pushed_down_filter = Filter {
                predicates: pushed_down_predicates,
            };
            result = SExpr::create_unary(Arc::new(pushed_down_filter.into()), Arc::new(result));
        }

        result = SExpr::create_unary(Arc::new(window.into()), Arc::new(result));

        if !remaining_predicates.is_empty() {
            let remaining_filter = Filter {
                predicates: remaining_predicates,
            };
            result = SExpr::create_unary(Arc::new(remaining_filter.into()), Arc::new(result));
            result.set_applied_rule(&self.id);
        }

        state.add_result(result);

        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}
