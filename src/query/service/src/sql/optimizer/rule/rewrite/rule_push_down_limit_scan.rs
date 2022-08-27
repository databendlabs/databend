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
///
/// Output:
///         Limit
///           \
///           LogicalGet(padding limit)

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
                SExpr::create_leaf(
                    PatternPlan {
                        plan_type: RelOp::LogicalGet,
                    }
                    .into(),
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
        if let Some(mut count) = limit.limit {
            let child = s_expr.child(0)?;
            let mut get: LogicalGet = child.plan().clone().try_into()?;
            count += limit.offset;
            get.limit = Some(get.limit.map_or(count, |c| cmp::max(c, count)));
            let get = SExpr::create_leaf(RelOperator::LogicalGet(get));
            state.add_result(s_expr.replace_children(vec![get]));
        }
        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
