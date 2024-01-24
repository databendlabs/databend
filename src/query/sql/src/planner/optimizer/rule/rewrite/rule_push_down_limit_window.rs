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

use std::cmp;
use std::sync::Arc;

use databend_common_exception::Result;

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Limit;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOp::Pattern;
use crate::plans::RelOp::Window;
use crate::plans::RelOperator;
use crate::plans::Window as LogicalWindow;
use crate::plans::WindowFuncFrame;
use crate::plans::WindowFuncFrameUnits;

/// Input:  Limit
///           \
///          Window
///             \
///              *
///
/// Output: Limit
///           \
///          Window(padding limit)
///             \
///               *
pub struct RulePushDownLimitWindow {
    id: RuleID,
    patterns: Vec<SExpr>,
}

impl RulePushDownLimitWindow {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownLimitSort,
            patterns: vec![SExpr::create_unary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Limit,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_unary(
                    Arc::new(PatternPlan { plan_type: Window }.into()),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan { plan_type: Pattern }.into(),
                    ))),
                )),
            )],
        }
    }
}

impl Rule for RulePushDownLimitWindow {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let limit: Limit = s_expr.plan().clone().try_into()?;
        if let Some(mut count) = limit.limit {
            count += limit.offset;
            let window = s_expr.child(0)?;
            let mut window_limit: LogicalWindow = window.plan().clone().try_into()?;
            if is_range_frame(&window_limit.frame) || child_has_window(window.child(0)?)? {
                return Ok(());
            }

            window_limit.limit = Some(window_limit.limit.map_or(count, |c| cmp::max(c, count)));
            let sort = SExpr::create_unary(
                Arc::new(RelOperator::Window(window_limit)),
                Arc::new(window.child(0)?.clone()),
            );

            let mut result = s_expr.replace_children(vec![Arc::new(sort)]);
            result.set_applied_rule(&self.id);
            state.add_result(result);
        }
        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}

fn is_range_frame(frame: &WindowFuncFrame) -> bool {
    matches!(frame.units, WindowFuncFrameUnits::Range)
}

fn child_has_window(child: &SExpr) -> Result<bool> {
    match child.plan() {
        RelOperator::Window(_) => Ok(true),
        RelOperator::Scan(_) => Ok(false), // finish recursion
        _ => child_has_window(child.child(0)?),
    }
}
