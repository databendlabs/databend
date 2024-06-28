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

use crate::optimizer::extract::Matcher;
use crate::optimizer::SExpr;
use crate::plans::AddRowNumber;
use crate::plans::Exchange::Broadcast;
use crate::plans::Join;
use crate::plans::RelOp;
use crate::plans::RelOperator;
pub struct MergeSourceOptimizer {
    pub merge_source_matcher: Matcher,
}

impl MergeSourceOptimizer {
    pub fn create() -> Self {
        Self {
            merge_source_matcher: Self::merge_source_matcher(),
        }
    }

    pub fn optimize(&self, s_expr: &SExpr, source_has_row_id: bool) -> Result<SExpr> {
        let left_exchange = s_expr.child(0)?;
        assert_eq!(left_exchange.children.len(), 1);
        let left_exchange_input = left_exchange.child(0)?;

        let right_exchange = s_expr.child(1)?;
        assert_eq!(right_exchange.children.len(), 1);
        let right_exchange_input = right_exchange.child(0)?;

        // source is build side
        let new_join_children = if source_has_row_id {
            vec![
                Arc::new(left_exchange_input.clone()),
                Arc::new(SExpr::create_unary(
                    Arc::new(RelOperator::Exchange(Broadcast)),
                    Arc::new(right_exchange_input.clone()),
                )),
            ]
        } else {
            vec![
                Arc::new(left_exchange_input.clone()),
                Arc::new(SExpr::create_unary(
                    Arc::new(RelOperator::Exchange(Broadcast)),
                    Arc::new(SExpr::create_unary(
                        Arc::new(RelOperator::AddRowNumber(AddRowNumber)),
                        Arc::new(right_exchange_input.clone()),
                    )),
                )),
            ]
        };

        let mut join: Join = s_expr.plan().clone().try_into()?;
        join.need_hold_hash_table = true;
        let mut join_s_expr = s_expr.replace_plan(Arc::new(RelOperator::Join(join)));
        join_s_expr = join_s_expr.replace_children(new_join_children);
        Ok(join_s_expr)
    }

    // for right outer join (source as build)
    fn merge_source_matcher() -> Matcher {
        // Input:
        //         Join
        //         /  \
        //        /    \
        //   Exchange   Exchange(Shuffle)
        //      |           |
        //      *           *
        // source is build we will get below:
        // Output:
        //         Join
        //         /  \
        //        /    \
        // Exchange    Exchange(Broadcast)
        // (Random)           |
        //    |          AddRowNumber
        //    |               |
        //    *               *
        Matcher::MatchOp {
            op_type: RelOp::Join,
            children: vec![
                Matcher::MatchOp {
                    op_type: RelOp::Exchange,
                    children: vec![Matcher::Leaf],
                },
                Matcher::MatchOp {
                    op_type: RelOp::Exchange,
                    children: vec![Matcher::Leaf],
                },
            ],
        }
    }
}
