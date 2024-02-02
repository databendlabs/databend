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

use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::EvalScalar;
use crate::plans::Limit;
use crate::plans::RelOp;
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
pub struct RulePushDownLimitEvalScalar {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RulePushDownLimitEvalScalar {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownLimitEvalScalar,
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Limit,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::EvalScalar,
                    children: vec![Matcher::Leaf],
                }],
            }],
        }
    }
}

impl Rule for RulePushDownLimitEvalScalar {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut TransformResult,
    ) -> databend_common_exception::Result<()> {
        let limit: Limit = s_expr.plan().clone().try_into()?;
        let eval_plan = s_expr.child(0)?;
        let eval_scalar: EvalScalar = eval_plan.plan().clone().try_into()?;

        let limit_expr = SExpr::create_unary(
            Arc::new(RelOperator::Limit(limit)),
            Arc::new(eval_plan.child(0)?.clone()),
        );
        let mut result = SExpr::create_unary(
            Arc::new(RelOperator::EvalScalar(eval_scalar)),
            Arc::new(limit_expr),
        );

        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
