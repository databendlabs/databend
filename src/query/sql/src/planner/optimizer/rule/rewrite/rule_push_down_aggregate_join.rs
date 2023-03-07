// Copyright 2023 Datafuse Labs.
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

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::Join;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOperator;

/// Input:     Aggregate(Final)
///                   |
///           Aggregate(Partial)
///                   |
///                  Join
///                 /    \
///                *      *
///
/// Output:    Aggregate(Final)
///                   |
///                  Join
///                 /    \
///                *      Aggregate(Partial)
///                        \
///                         *
pub struct RulePushDownAggregateJoin {
    id: RuleID,
    pattern: SExpr,
}

impl RulePushDownAggregateJoin {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownFilterAggregate,
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Aggregate,
                }
                .into(),
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::Aggregate,
                    }
                    .into(),
                    SExpr::create_binary(
                        PatternPlan {
                            plan_type: RelOp::Join,
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
                ),
            ),
        }
    }
}

impl Rule for RulePushDownAggregateJoin {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> common_exception::Result<()> {
        let agg_final: Aggregate = s_expr.plan().clone().try_into()?;
        let agg_partial_expr = s_expr.child(0)?;
        let agg_partial: Aggregate = agg_partial_expr.plan().clone().try_into()?;

        let join_expr = agg_partial_expr.child(0)?;
        let join: Join = join_expr.plan().clone().try_into()?;

        // TODO(dousir9) only consider one item in the group_items now.
        let mut can_push_to_left = true;
        let mut can_push_to_right = true;
        if agg_final.group_columns()?.len() != 1 {
            can_push_to_left = false;
            can_push_to_right = false;
        } else {
            if join.left_conditions.len() != 1
                || agg_final.group_items[0].scalar != join.left_conditions[0]
            {
                can_push_to_left = false;
            }
            if join.right_conditions.len() != 1
                || agg_final.group_items[0].scalar != join.right_conditions[0]
            {
                can_push_to_right = false;
            }
        }

        // TODO(dousir9) consider JoinType.
        if can_push_to_left {
            let mut result = s_expr.replace_children(vec![join_expr.replace_children(vec![
                SExpr::create_unary(
                    RelOperator::Aggregate(agg_partial),
                    join_expr.child(0)?.clone(),
                ),
                join_expr.child(1)?.clone(),
            ])]);
            result.set_applied_rule(&self.id);
            state.add_result(result);
        } else if can_push_to_right {
            let mut result = s_expr.replace_children(vec![join_expr.replace_children(vec![
                join_expr.child(0)?.clone(),
                SExpr::create_unary(
                    RelOperator::Aggregate(agg_partial),
                    join_expr.child(1)?.clone(),
                ),
            ])]);
            result.set_applied_rule(&self.id);
            state.add_result(result);
        }
        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
