// Copyright 2022 Datafuse Labs.
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

use common_exception::Result;

use crate::sql::optimizer::rule::Rule;
use crate::sql::optimizer::rule::TransformState;
use crate::sql::optimizer::RuleID;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::JoinType;
use crate::sql::plans::LogicalInnerJoin;
use crate::sql::plans::PatternPlan;
use crate::sql::plans::RelOp;

/// Rule to apply commutivity of join operator.
/// Since we will always use the right child as build side, this
/// rule will help us measure which child is the better one.
///
/// TODO(leiysky): currently, we only support commutate for inner/cross join.
/// Other join types will be added as soon as we implement them in Processor.
pub struct RuleCommuteJoin {
    id: RuleID,
    pattern: SExpr,
}

impl RuleCommuteJoin {
    pub fn new() -> Self {
        Self {
            id: RuleID::CommuteJoin,

            // LogicalJoin
            // | \
            // *  *
            pattern: SExpr::create_binary(
                PatternPlan {
                    plan_type: RelOp::LogicalInnerJoin,
                }
                .into(),
                SExpr::create_pattern_leaf(),
                SExpr::create_pattern_leaf(),
            ),
        }
    }
}

impl Rule for RuleCommuteJoin {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformState) -> Result<()> {
        let mut join: LogicalInnerJoin = s_expr.plan().clone().try_into()?;
        let left_child = s_expr.child(0)?;
        let right_child = s_expr.child(1)?;

        match join.join_type {
            JoinType::Inner | JoinType::Cross => {
                // Swap the join conditions side
                (join.left_conditions, join.right_conditions) =
                    (join.right_conditions, join.left_conditions);
                let result =
                    SExpr::create_binary(join.into(), right_child.clone(), left_child.clone());
                state.add_result(result);
            }
            _ => {}
        }

        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
