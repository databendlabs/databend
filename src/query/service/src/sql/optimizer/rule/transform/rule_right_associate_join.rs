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

use super::util::get_join_predicates;
use crate::sql::binder::JoinPredicate;
use crate::sql::optimizer::rule::Rule;
use crate::sql::optimizer::rule::TransformState;
use crate::sql::optimizer::RelExpr;
use crate::sql::optimizer::RuleID;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::JoinType;
use crate::sql::plans::LogicalInnerJoin;
use crate::sql::plans::PatternPlan;
use crate::sql::plans::RelOp;

/// Rule to apply associativity of join.
/// The right associativity of join can be denoted as `A ⋈ (B ⋈ C) = (A ⋈ B) ⋈ C`.
///
/// If we have a join tree like:
///    join
///    /  \
///   t1  join
///       /  \
///      t2  t3
///
/// We can represent it as `t1 ⋈ (t2 ⋈ t3)`. With this rule, we can transform
/// it to `(t1 ⋈ t2) ⋈ t3`, which looks like:
///    join
///    /  \
///  join  t3
///  /  \
///  t1  t2
pub struct RuleRightAssociateJoin {
    id: RuleID,
    pattern: SExpr,
}

impl RuleRightAssociateJoin {
    pub fn new() -> Self {
        Self {
            id: RuleID::RightAssociateJoin,

            // LogicalJoin
            // | \
            // *  LogicalJoin
            //    | \
            //    *  *
            pattern: SExpr::create_binary(
                PatternPlan {
                    plan_type: RelOp::LogicalInnerJoin,
                }
                .into(),
                SExpr::create_pattern_leaf(),
                SExpr::create_binary(
                    PatternPlan {
                        plan_type: RelOp::LogicalInnerJoin,
                    }
                    .into(),
                    SExpr::create_pattern_leaf(),
                    SExpr::create_pattern_leaf(),
                ),
            ),
        }
    }
}

impl Rule for RuleRightAssociateJoin {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformState) -> Result<()> {
        // We denote the join tree with:
        //    join1
        //    /  \
        //   t1  join2
        //      /  \
        //     t2  t3
        //
        // After applying the transform, we will get:
        //    join3
        //    /  \
        //  join4 t3
        //  /  \
        // t1  t2
        let join1: LogicalInnerJoin = s_expr.plan.clone().try_into()?;
        let join2: LogicalInnerJoin = s_expr.child(1)?.plan.clone().try_into()?;
        let t1 = s_expr.child(0)?;
        let t2 = s_expr.child(1)?.child(0)?;
        let t3 = s_expr.child(1)?.child(1)?;

        // Ensure inner joins
        if join1.join_type != JoinType::Inner || join2.join_type != JoinType::Inner {
            return Ok(());
        }

        // Check if original sexpr contains cross join.
        // We will reject the results contain cross join if there is no cross join in original sexpr.
        let contains_cross_join =
            join1.join_type == JoinType::Cross || join2.join_type == JoinType::Cross;

        let predicates = vec![get_join_predicates(&join1), get_join_predicates(&join2)].concat();

        let mut join_3 = LogicalInnerJoin::default();
        let mut join_4 = LogicalInnerJoin::default();

        let t1_prop = RelExpr::with_s_expr(t1).derive_relational_prop()?;
        let t2_prop = RelExpr::with_s_expr(t2).derive_relational_prop()?;
        let t3_prop = RelExpr::with_s_expr(t3).derive_relational_prop()?;
        let join4_prop = RelExpr::with_s_expr(&SExpr::create_binary(
            join_4.clone().into(),
            t1.clone(),
            t2.clone(),
        ))
        .derive_relational_prop()?;

        let mut join_4_preds = vec![];

        // Resolve predicates for join3
        for predicate in predicates.iter() {
            let join_pred = JoinPredicate::new(predicate, &join4_prop, &t3_prop);
            match join_pred {
                JoinPredicate::Right(pred) => {
                    // TODO(leiysky): push down the predicate
                    join_3.other_conditions.push(pred.clone());
                }
                JoinPredicate::Left(pred) => {
                    join_4_preds.push(pred.clone());
                }
                JoinPredicate::Both { left, right } => {
                    join_3.left_conditions.push(left.clone());
                    join_3.right_conditions.push(right.clone());
                }
                JoinPredicate::Other(pred) => {
                    join_3.other_conditions.push(pred.clone());
                }
            }
        }

        if !join_3.left_conditions.is_empty() && !join_3.right_conditions.is_empty() {
            join_3.join_type = JoinType::Inner;
        }

        // Resolve predicates for join4
        for predicate in join_4_preds.iter() {
            let join_pred = JoinPredicate::new(predicate, &t1_prop, &t2_prop);
            match join_pred {
                JoinPredicate::Left(_) | JoinPredicate::Right(_) | JoinPredicate::Other(_) => {
                    // TODO(leiysky): push down the predicate
                    join_4.other_conditions.push(predicate.clone());
                }
                JoinPredicate::Both { left, right } => {
                    join_4.left_conditions.push(left.clone());
                    join_4.right_conditions.push(right.clone());
                }
            }
        }

        if !join_4.left_conditions.is_empty() && !join_4.right_conditions.is_empty() {
            join_4.join_type = JoinType::Inner;
        }

        // Reject inefficient cross join
        if !contains_cross_join
            && (join_3.join_type == JoinType::Cross || join_4.join_type == JoinType::Cross)
        {
            return Ok(());
        }

        let result = SExpr::create(
            join_3.into(),
            vec![
                SExpr::create_binary(join_4.into(), t1.clone(), t2.clone()),
                t3.clone(),
            ],
            s_expr.original_group,
            None,
        );

        state.add_result(result);

        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
