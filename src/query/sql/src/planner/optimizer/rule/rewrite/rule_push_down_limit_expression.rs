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

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::EvalScalar;
use crate::plans::Limit;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOp::Pattern;
use crate::plans::RelOperator;

/// Input:  Limit
///           \
///          Expression
///             \
///              *
///
/// Output: Expression
///           \
///          limit
///             \
///               *
pub struct RulePushDownLimitExpression {
    id: RuleID,
    patterns: Vec<SExpr>,
}

impl RulePushDownLimitExpression {
    pub fn new() -> Self {
        Self {
            id: RuleID::RulePushDownLimitExpression,
            patterns: vec![SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Limit,
                }
                .into(),
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::EvalScalar,
                    }
                    .into(),
                    SExpr::create_leaf(PatternPlan { plan_type: Pattern }.into()),
                ),
            )],
        }
    }
}

impl Rule for RulePushDownLimitExpression {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> common_exception::Result<()> {
        let limit: Limit = s_expr.plan().clone().try_into()?;
        let eval_plan = s_expr.child(0)?;
        let eval_scalar: EvalScalar = eval_plan.plan().clone().try_into()?;

        let limit_expr =
            SExpr::create_unary(RelOperator::Limit(limit), eval_plan.child(0)?.clone());
        let mut result = SExpr::create_unary(RelOperator::EvalScalar(eval_scalar), limit_expr);

        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}
