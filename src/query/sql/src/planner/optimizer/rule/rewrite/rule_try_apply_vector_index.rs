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
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::ScalarExpr;

pub struct RuleTryApplyVectorIndex {
    id: RuleID,
    patterns: Vec<SExpr>,
}

impl RuleTryApplyVectorIndex {
    pub fn new() -> Self {
        Self {
            id: RuleID::TryApplyVectorIndex,
            // Input:
            //   Limit
            //     \
            //    Sort
            //      \
            //      EvalScalar
            //        \
            //         Scan
            //
            // Output:
            //   Scan(order_by, limit, similarity)
            patterns: vec![SExpr::create_unary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Limit,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Sort,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_unary(
                        Arc::new(
                            PatternPlan {
                                plan_type: RelOp::EvalScalar,
                            }
                            .into(),
                        ),
                        Arc::new(SExpr::create_leaf(Arc::new(
                            PatternPlan {
                                plan_type: RelOp::Scan,
                            }
                            .into(),
                        ))),
                    )),
                )),
            )],
        }
    }
}

impl Rule for RuleTryApplyVectorIndex {
    fn id(&self) -> crate::optimizer::RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &crate::optimizer::SExpr,
        state: &mut crate::optimizer::rule::TransformResult,
    ) -> common_exception::Result<()> {
        let limit = s_expr.plan().as_limit().unwrap();
        let sort = s_expr.walk_down(1).plan().as_sort().unwrap();
        let eval_scalar = s_expr.walk_down(2).plan().as_eval_scalar().unwrap();

        if limit.offset != 0 || limit.limit.is_none() || sort.items.len() != 1 || !sort.items[0].asc
        {
            state.add_result(s_expr.clone());
            return Ok(());
        }

        let sort_by_idx = eval_scalar
            .items
            .iter()
            .position(|item| item.index == sort.items[0].index)
            .unwrap();
        let sort_by_item = &eval_scalar.items[sort_by_idx];
        match &sort_by_item.scalar {
            ScalarExpr::FunctionCall(func) if func.func_name == "cosine_distance" => {
                let mut new_scan = s_expr.walk_down(3).clone();
                new_scan.plan.as_scan_mut().unwrap().order_by = Some(sort.items.clone());
                new_scan.plan.as_scan_mut().unwrap().limit = Some(limit.limit.unwrap());
                new_scan.plan.as_scan_mut().unwrap().similarity = Some(Box::new(func.clone()));
                new_scan.set_applied_rule(&self.id);
                state.add_result(new_scan);
            }
            _ => state.add_result(s_expr.clone()),
        }
        Ok(())
    }

    fn patterns(&self) -> &Vec<crate::optimizer::SExpr> {
        &self.patterns
    }
}
