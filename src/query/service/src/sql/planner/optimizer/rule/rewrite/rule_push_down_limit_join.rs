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

use crate::sql::optimizer::rule::Rule;
use crate::sql::optimizer::rule::TransformState;
use crate::sql::optimizer::RuleID;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::JoinType;
use crate::sql::plans::Limit;
use crate::sql::plans::LogicalInnerJoin;
use crate::sql::plans::PatternPlan;
use crate::sql::plans::RelOp;
use crate::sql::plans::RelOperator;

/// Input:      Limit
///               |
///        Left Outer Join
///             /    \
///            *      *
///
/// Output:
///             Limit
///               |
///        Left Outer Join
///             /     \
///           Limit   Limit
///           /         \
///          *           *
pub struct RulePushDownLimitOuterJoin {
    id: RuleID,
    pattern: SExpr,
}

impl RulePushDownLimitOuterJoin {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownLimitOuterJoin,
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Limit,
                }
                .into(),
                SExpr::create_binary(
                    PatternPlan {
                        plan_type: RelOp::LogicalInnerJoin,
                    }
                    .into(),
                    SExpr::create_leaf(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ),
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

impl Rule for RulePushDownLimitOuterJoin {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformState) -> common_exception::Result<()> {
        let limit: Limit = s_expr.plan().clone().try_into()?;
        if limit.limit.is_some() {
            let child = s_expr.child(0)?;
            let join: LogicalInnerJoin = child.plan().clone().try_into()?;
            match join.join_type {
                JoinType::Left | JoinType::Full => {
                    state.add_result(s_expr.replace_children(vec![child.replace_children(vec![
                        SExpr::create_unary(
                            RelOperator::Limit(limit.clone()),
                            child.child(0)?.clone(),
                        ),
                        SExpr::create_unary(RelOperator::Limit(limit), child.child(1)?.clone()),
                    ])]))
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
