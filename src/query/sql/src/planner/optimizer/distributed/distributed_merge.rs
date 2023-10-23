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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::optimizer::SExpr;
use crate::plans::Exchange::Broadcast;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOperator;
pub struct MergeSourceOptimizer {
    merge_source_pattern: SExpr,
}

impl MergeSourceOptimizer {
    pub fn create() -> Self {
        Self {
            merge_source_pattern: Self::merge_source_pattern(),
        }
    }

    pub fn optimize(&self, s_expr: &SExpr) -> Result<SExpr> {
        if !s_expr.match_pattern(&self.merge_source_pattern) {
            return Err(ErrorCode::BadArguments(format!(
                "pattern not match for dirstributed merge source"
            )));
        } else {
            let join_s_expr = s_expr.child(0)?;

            let left_exchange = join_s_expr.child(0)?;
            assert!(left_exchange.children.len() == 1);
            let left_exchange_input = left_exchange.child(0)?;

            let right_exchange = join_s_expr.child(1)?;
            assert!(right_exchange.children.len() == 1);
            let right_exchange_input = join_s_expr.child(0)?;

            let new_join_children = vec![
                Arc::new(left_exchange_input.clone()),
                Arc::new(SExpr::create_unary(
                    Arc::new(RelOperator::Exchange(Broadcast)),
                    Arc::new(right_exchange_input.clone()),
                )),
            ];
            Ok(join_s_expr.replace_children(new_join_children))
        }
    }

    fn merge_source_pattern() -> SExpr {
        // Input:
        //       Exchange(Merge)
        //          |
        //         Join
        //         /  \
        //        /    \
        //   Exchange   Exchange(Shuffle)
        //      |           |
        //      *           *
        // Output:
        //       Exchange
        //          |
        //         Join
        //         /  \
        //        /    \
        //       *     Exchange(Broadcast)
        //                |
        //                *
        SExpr::create_unary(
            Arc::new(
                PatternPlan {
                    plan_type: RelOp::Exchange,
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
                Arc::new(SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Exchange,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ))),
                )),
                Arc::new(SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Exchange,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ))),
                )),
            )),
        )
    }
}
