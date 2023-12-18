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
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::Limit;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOperator;

/// Input:      Limit
///               |
///        Left Outer Join
///             /    \
///            *      *
///
/// Output:
///             Limit(offset removed)
///               |
///        Left Outer Join
///             /     \
///           Limit    *
///           /
///          *
pub struct RulePushDownLimitOuterJoin {
    id: RuleID,
    patterns: Vec<SExpr>,
}

impl RulePushDownLimitOuterJoin {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownLimitOuterJoin,
            patterns: vec![SExpr::create_unary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Limit,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_binary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Join,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ))),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ))),
                )),
            )],
        }
    }
}

impl Rule for RulePushDownLimitOuterJoin {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut TransformResult,
    ) -> databend_common_exception::Result<()> {
        let limit: Limit = s_expr.plan().clone().try_into()?;
        if limit.limit.is_some() {
            let child = s_expr.child(0)?;
            let join: Join = child.plan().clone().try_into()?;
            match join.join_type {
                JoinType::Left => {
                    let child = child.replace_children(vec![
                        Arc::new(SExpr::create_unary(
                            Arc::new(RelOperator::Limit(limit.clone())),
                            Arc::new(child.child(0)?.clone()),
                        )),
                        Arc::new(child.child(1)?.clone()),
                    ]);
                    let mut result = SExpr::create_unary(
                        Arc::new(RelOperator::Limit(Limit {
                            before_exchange: limit.before_exchange,
                            limit: limit.limit,
                            offset: 0,
                        })),
                        Arc::new(child),
                    );
                    result.set_applied_rule(&self.id);
                    state.add_result(result)
                }
                JoinType::Right => {
                    let child = Arc::new(child.replace_children(vec![
                        Arc::new(child.child(0)?.clone()),
                        Arc::new(SExpr::create_unary(
                            Arc::new(RelOperator::Limit(limit.clone())),
                            Arc::new(child.child(1)?.clone()),
                        )),
                    ]));
                    let mut result = SExpr::create_unary(
                        Arc::new(RelOperator::Limit(Limit {
                            before_exchange: limit.before_exchange,
                            limit: limit.limit,
                            offset: 0,
                        })),
                        child,
                    );
                    result.set_applied_rule(&self.id);
                    state.add_result(result)
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}
