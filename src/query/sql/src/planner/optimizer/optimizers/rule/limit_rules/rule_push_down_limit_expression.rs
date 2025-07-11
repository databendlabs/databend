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

use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::EvalScalar;
use crate::plans::Limit;
use crate::plans::Operator;
use crate::plans::RelOp;

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
        let limit = s_expr
            .plan()
            .as_any()
            .downcast_ref::<Limit>()
            .unwrap()
            .clone();
        let eval_plan = s_expr.child(0)?;
        let eval_scalar = eval_plan
            .plan()
            .as_any()
            .downcast_ref::<EvalScalar>()
            .unwrap()
            .clone();

        let limit_expr = SExpr::create_unary(limit, eval_plan.child(0)?.clone());
        let mut result = SExpr::create_unary(eval_scalar, limit_expr);

        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RulePushDownLimitEvalScalar {
    fn default() -> Self {
        Self::new()
    }
}
