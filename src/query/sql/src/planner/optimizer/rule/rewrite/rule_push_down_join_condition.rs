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

use crate::binder::JoinPredicate;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RelExpr;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Filter;
use crate::plans::LogicalInnerJoin;
use crate::plans::PatternPlan;
use crate::plans::RelOp;

pub struct RulePushDownJoinCondition {
    id: RuleID,
    pattern: SExpr,
}

impl RulePushDownJoinCondition {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownJoinCondition,
            //  InnerJoin
            //   | \
            //   *  *
            pattern: SExpr::create_binary(
                PatternPlan {
                    plan_type: RelOp::LogicalInnerJoin,
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
        }
    }
}

impl Rule for RulePushDownJoinCondition {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let mut join: LogicalInnerJoin = s_expr.plan().clone().try_into()?;
        if join.other_conditions.is_empty() {
            return Ok(());
        }

        let rel_expr = RelExpr::with_s_expr(s_expr);
        let left_prop = rel_expr.derive_relational_prop_child(0)?;
        let right_prop = rel_expr.derive_relational_prop_child(1)?;

        let mut left_push_down = vec![];
        let mut right_push_down = vec![];

        let mut need_push = false;

        for predicate in join.other_conditions.iter() {
            let pred = JoinPredicate::new(predicate, &left_prop, &right_prop);
            match pred {
                JoinPredicate::Left(_) => {
                    need_push = true;
                    left_push_down.push(predicate.clone());
                }
                JoinPredicate::Right(_) => {
                    need_push = true;
                    right_push_down.push(predicate.clone());
                }
                _ => unreachable!(),
            }
        }

        join.other_conditions.clear();

        if !need_push {
            return Ok(());
        }

        let mut left_child = s_expr.child(0)?.clone();
        let mut right_child = s_expr.child(1)?.clone();

        if !left_push_down.is_empty() {
            left_child = SExpr::create_unary(
                Filter {
                    predicates: left_push_down,
                    is_having: false,
                }
                .into(),
                left_child,
            );
        }

        if !right_push_down.is_empty() {
            right_child = SExpr::create_unary(
                Filter {
                    predicates: right_push_down,
                    is_having: false,
                }
                .into(),
                right_child,
            );
        }

        let result = SExpr::create_binary(join.into(), left_child, right_child);
        state.add_result(result);
        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
