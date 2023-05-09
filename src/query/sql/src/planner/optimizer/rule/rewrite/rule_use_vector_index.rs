// Copyright 2023 Datafuse Labs
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

use common_exception::ErrorCode;

use crate::optimizer::rule::Rule;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::PatternPlan;
use crate::plans::RelOp;

pub struct RuleUseVectorIndex {
    id: RuleID,
    patterns: Vec<SExpr>,
}

impl RuleUseVectorIndex {
    pub fn new() -> Self {
        Self {
            id: RuleID::UseVectorIndex,
            // Limit
            //   \
            //   Sort(cosine_distance)
            //     \
            //     EvalScalar
            patterns: vec![SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Limit,
                }
                .into(),
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::Sort,
                    }
                    .into(),
                    SExpr::create_leaf(
                        PatternPlan {
                            plan_type: RelOp::EvalScalar,
                        }
                        .into(),
                    ),
                ),
            )],
        }
    }
}

impl Rule for RuleUseVectorIndex {
    fn id(&self) -> crate::optimizer::RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &crate::optimizer::SExpr,
        state: &mut crate::optimizer::rule::TransformResult,
    ) -> common_exception::Result<()> {
        let sort_expr = s_expr.children.get(0).unwrap();
        let sort_operator = s_expr.plan.as_sort().unwrap();
        let eval_scalar_operator = sort_expr
            .children
            .get(0)
            .unwrap()
            .plan
            .as_eval_scalar()
            .unwrap();
        if sort_operator.items.len() != 1 || sort_operator.items[0].asc != true {
            state.add_result(s_expr.clone());
            return Ok(());
        }
        let sort_by = eval_scalar_operator
            .items
            .get(sort_operator.items[0].index)
            .unwrap();
        match &sort_by.scalar {
            crate::ScalarExpr::FunctionCall(func) if func.func_name == "cosine_distance" => {
                
            }
            _ => state.add_result(s_expr.clone()),
        }
        Ok(())
    }

    fn patterns(&self) -> &Vec<crate::optimizer::SExpr> {
        &self.patterns
    }
}
