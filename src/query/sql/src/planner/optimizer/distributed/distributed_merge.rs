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
use crate::plans::Exchange::Hash;
use crate::plans::Join;
use crate::plans::RelOp;
use crate::plans::RelOperator;
pub struct BroadcastToShuffleOptimizer {
    pub matcher: Matcher,
}

impl BroadcastToShuffleOptimizer {
    pub fn create() -> Self {
        Self {
            matcher: Self::matcher(),
        }
    }

    pub fn optimize(&self, s_expr: &SExpr) -> Result<SExpr> {
        let left_exchange_input = s_expr.child(0)?;

        let right_exchange = s_expr.child(1)?;
        assert_eq!(right_exchange.children.len(), 1);
        let right_exchange_input = right_exchange.child(0)?;

        let mut join: Join = s_expr.plan().clone().try_into()?;
        join.need_hold_hash_table = true;

        let (left_conditions, right_conditions): (Vec<_>, Vec<_>) = join
            .equi_conditions
            .iter()
            .map(|condition| (condition.left.clone(), condition.right.clone()))
            .unzip();

        let new_join_children = vec![
            Arc::new(SExpr::create_unary(
                Arc::new(RelOperator::Exchange(Hash(left_conditions))),
                Arc::new(left_exchange_input.clone()),
            )),
            Arc::new(SExpr::create_unary(
                Arc::new(RelOperator::Exchange(Hash(right_conditions))),
                Arc::new(right_exchange_input.clone()),
            )),
        ];
        let mut join_s_expr = s_expr.replace_plan(Arc::new(RelOperator::Join(join)));
        join_s_expr = join_s_expr.replace_children(new_join_children);
        Ok(join_s_expr)
    }

    fn matcher() -> Matcher {
        // Input:
        //         Join
        //         /  \
        //        /    \
        //          Exchange
        //        (Broadcast)
        //      |       |
        //      *       *
        // Output:
        //         Join
        //         /  \
        //        /    \
        // Exchange    Exchange
        // (Shuffle)   (Shuffle)
        //    |           |
        //    *           *
        Matcher::MatchOp {
            op_type: RelOp::Join,
            children: vec![Matcher::Leaf, Matcher::MatchOp {
                op_type: RelOp::Exchange,
                children: vec![Matcher::Leaf],
            }],
        }
    }
}
