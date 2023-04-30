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

use common_exception::Result;

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Limit;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::UnionAll;

pub struct RulePushDownLimitUnion {
    id: RuleID,
    patterns: Vec<SExpr>,
}

impl RulePushDownLimitUnion {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownLimitUnion,
            // Limit
            //  \
            //   UnionAll
            //     /  \
            //   ...   ...
            patterns: vec![SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Limit,
                }
                .into(),
                SExpr::create_binary(
                    PatternPlan {
                        plan_type: RelOp::UnionAll,
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
            )],
        }
    }
}

impl Rule for RulePushDownLimitUnion {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let limit: Limit = s_expr.plan().clone().try_into()?;
        let union_s_expr = s_expr.child(0)?;
        let union: UnionAll = union_s_expr.plan().clone().try_into()?;

        if limit.limit.is_none() {
            return Ok(());
        }
        // Create limit which will be pushed down
        let limit_offset = limit.limit.unwrap() + limit.offset;
        let new_limit = Limit {
            limit: limit
                .limit
                .map(|origin_limit| cmp::max(origin_limit, limit_offset)),
            offset: 0,
        };

        // Push down new_limit to union children
        let mut union_left_child = union_s_expr.child(0)?.clone();
        let mut union_right_child = union_s_expr.child(1)?.clone();

        // Add limit to union children
        union_left_child = SExpr::create_unary(new_limit.clone().into(), union_left_child);
        union_right_child = SExpr::create_unary(new_limit.into(), union_right_child);

        let mut result = SExpr::create_binary(union.into(), union_left_child, union_right_child);

        // Add original limit to top
        result = s_expr.replace_children(vec![result]);
        result.set_applied_rule(&self.id);
        state.add_result(result);

        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}
