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

use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::partition_filter::WindowFilterPushDown;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Filter;
use crate::plans::RelOp;

/// Push down predicates that have one value per window partition, so the
/// filter removes whole partitions. See `WindowFilterPushDown`.
pub struct RulePushDownFilterWindow {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RulePushDownFilterWindow {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownFilterWindow,
            matchers: vec![
                Matcher::MatchOp {
                    op_type: RelOp::Filter,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Window,
                        children: vec![Matcher::Leaf],
                    }],
                },
                Matcher::MatchOp {
                    op_type: RelOp::Filter,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::WindowGroup,
                        children: vec![Matcher::Leaf],
                    }],
                },
            ],
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
        let Filter { predicates } = s_expr.plan().clone().try_into()?;
        let window_expr = s_expr.unary_child();
        let Some(push_down) = WindowFilterPushDown::try_new(window_expr)? else {
            return Ok(());
        };
        let mut pushed_down = vec![];
        let mut remaining = vec![];
        for predicate in predicates {
            if push_down.can_push_down(&predicate)? {
                pushed_down.push(predicate);
            } else {
                remaining.push(predicate);
            }
        }
        if pushed_down.is_empty() {
            return Ok(());
        }

        let window = window_expr
            .unary_child_arc()
            .ref_build_unary(Filter {
                predicates: pushed_down,
            })
            .build_unary(window_expr.plan.clone());
        let result = if remaining.is_empty() {
            window
        } else {
            let mut expr = window.build_unary(Filter {
                predicates: remaining,
            });
            expr.set_applied_rule(&self.id);
            expr
        };
        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RulePushDownFilterWindow {
    fn default() -> Self {
        Self::new()
    }
}
