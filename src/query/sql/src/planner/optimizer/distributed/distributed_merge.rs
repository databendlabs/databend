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

use databend_common_exception::Result;

use crate::optimizer::SExpr;
use crate::plans::AddRowNumber;
use crate::plans::Exchange::Broadcast;
use crate::plans::Join;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOperator;
pub struct MergeSourceOptimizer {
    pub merge_source_pattern: SExpr,
}

impl MergeSourceOptimizer {
    pub fn create() -> Self {
        Self {
            merge_source_pattern: Self::merge_source_pattern(),
        }
    }

    // rewrite plan:
    // 1. if use right join, and the default
    // distributed right join will use shuffle hash join, but its
    // performance is very slow and poor. So we need to rewrite it.
    // In new distributed plan, target partitions will be shuffled
    // to query nodes, and source will be broadcasted to all nodes
    // and build hashtable. It means all nodes hold the same hashtable.
    // 2. if use left outer join, we will just use distributed optimizer's
    // result, so we won't arrive here, because the it doesn't take place
    // change join order.
    pub fn optimize(&self, s_expr: &SExpr) -> Result<SExpr> {
        let join_s_expr = s_expr.child(0)?;

        let left_exchange = join_s_expr.child(0)?;
        assert!(left_exchange.children.len() == 1);
        let left_exchange_input = left_exchange.child(0)?;

        let right_exchange = join_s_expr.child(1)?;
        assert!(right_exchange.children.len() == 1);
        let right_exchange_input = right_exchange.child(0)?;

        // source is build side
        let new_join_children = vec![
            Arc::new(left_exchange_input.clone()),
            Arc::new(SExpr::create_unary(
                Arc::new(RelOperator::Exchange(Broadcast)),
                Arc::new(SExpr::create_unary(
                    Arc::new(RelOperator::AddRowNumber(AddRowNumber)),
                    Arc::new(right_exchange_input.clone()),
                )),
            )),
        ];

        let mut join: Join = join_s_expr.plan().clone().try_into()?;
        join.need_hold_hash_table = true;
        let mut join_s_expr = join_s_expr.replace_plan(Arc::new(RelOperator::Join(join)));
        join_s_expr = join_s_expr.replace_children(new_join_children);
        Ok(s_expr.replace_children(vec![Arc::new(join_s_expr)]))
    }

    // for right outer join (source as build)
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
        // source is build we will get below:
        // Output:
        //       Exchange
        //          |
        //         Join
        //         /  \
        //        /    \
        // Exchange    Exchange(Broadcast)
        // (Random)           |
        //    |          AddRowNumber
        //    |               |
        //    *               *
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
