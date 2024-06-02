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
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RelExpr;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOp;

/// Rule to apply commutativity of join operator.
/// Since we will always use the right child as build side, this
/// rule will help us measure which child is the better one.
pub struct RuleCommuteJoin {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RuleCommuteJoin {
    pub fn new() -> Self {
        Self {
            id: RuleID::CommuteJoin,

            // LogicalJoin
            // | \
            // *  *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Join,
                children: vec![Matcher::Leaf, Matcher::Leaf],
            }],
        }
    }
}

impl Rule for RuleCommuteJoin {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let mut join: Join = s_expr.plan().clone().try_into()?;

        if join.build_side_cache_info.is_some() {
            return Ok(());
        }

        let left_child = s_expr.child(0)?;
        let right_child = s_expr.child(1)?;
        let left_rel_expr = RelExpr::with_s_expr(left_child);
        let right_rel_expr = RelExpr::with_s_expr(right_child);
        let left_card = left_rel_expr.derive_cardinality()?.cardinality;
        let right_card = right_rel_expr.derive_cardinality()?.cardinality;

        let need_commute = if left_card < right_card {
            matches!(
                join.join_type,
                JoinType::Inner
                    | JoinType::Cross
                    | JoinType::Left
                    | JoinType::Right
                    | JoinType::LeftSingle
                    | JoinType::RightSingle
                    | JoinType::LeftSemi
                    | JoinType::RightSemi
                    | JoinType::LeftAnti
                    | JoinType::RightAnti
                    | JoinType::LeftMark
                    | JoinType::RightMark
            )
        } else if left_card == right_card {
            matches!(
                join.join_type,
                JoinType::Right | JoinType::RightSingle | JoinType::RightSemi | JoinType::RightAnti
            )
        } else {
            false
        };
        if need_commute {
            // Swap the join conditions side
            (join.left_conditions, join.right_conditions) =
                (join.right_conditions, join.left_conditions);
            join.join_type = join.join_type.opposite();
            let mut result = SExpr::create_binary(
                Arc::new(join.into()),
                Arc::new(right_child.clone()),
                Arc::new(left_child.clone()),
            );
            result.set_applied_rule(&self.id);
            state.add_result(result);
        }
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
