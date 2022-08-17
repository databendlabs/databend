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

use std::cmp;

use common_exception::Result;

use crate::sql::optimizer::rule::Rule;
use crate::sql::optimizer::rule::TransformState;
use crate::sql::optimizer::RuleID;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::Limit;
use crate::sql::plans::LogicalGet;
use crate::sql::plans::PatternPlan;
use crate::sql::plans::RelOp;
use crate::sql::plans::RelOperator;

/// Input:  Limit
///           \
///          LogicalGet
///             \
///              *
///
/// Output: Limit
///             \
///           LogicalGet
///               \
///                *

pub struct RulePushDownLimitScan {
    id: RuleID,
    pattern: SExpr,
}

impl RulePushDownLimitScan {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownLimitScan,
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Limit,
                }
                .into(),
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::LogicalGet,
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
}

impl Rule for RulePushDownLimitScan {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformState) -> Result<()> {
        let limit: Limit = s_expr.plan().clone().try_into()?;
        if limit.limit.is_none() {
            // nothing happened
            return Ok(());
        }
        let mut scan: LogicalGet = s_expr.child(0)?.plan().clone().try_into()?;
        let limited = limit.limit.unwrap() + limit.offset;
        if let Some(cnt) = scan.limit {
            // already have limit node been pushed down
            scan.limit = Some(cmp::min(limited, cnt));
        } else {
            // first limit
            scan.limit = Some(limited);
        }
        if limit.offset == 0 {
            // no offset, just tells it how many rows to be scanned
            state.add_result(SExpr::create_leaf(RelOperator::LogicalGet(scan)));
        } else {
            // replace scan with limit pushed down
            state.add_result(
                s_expr.replace_children(vec![SExpr::create_leaf(RelOperator::LogicalGet(scan))]),
            )
        }
        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
