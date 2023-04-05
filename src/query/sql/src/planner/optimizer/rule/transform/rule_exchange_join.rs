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

use std::vec;

use common_exception::Result;

use super::util::get_join_predicates;
use crate::binder::JoinPredicate;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RelExpr;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::PatternPlan;
use crate::plans::RelOp;

/// Rule to apply exchange on a bushy join tree.
/// If we have a join tree like:
///    join
///    /  \
///  join  join
///  /  \  /  \
/// t1 t2 t3  t4
///
/// We can represent it as `(t1 ⋈ t2) ⋈ (t3 ⋈ t4)`.
/// With this rule, we can transform it to
/// `(t1 ⋈ t3) ⋈ (t2 ⋈ t4)`, which looks like:
///    join
///    /  \
///  join  join
///  /  \  /  \
/// t1 t3 t2  t4
pub struct RuleExchangeJoin {
    id: RuleID,
    patterns: Vec<SExpr>,
}

impl RuleExchangeJoin {
    pub fn new() -> Self {
        Self {
            id: RuleID::ExchangeJoin,

            // LogicalJoin
            // | \
            // |  LogicalJoin
            // |  | \
            // |  *  *
            // LogicalJoin
            // | \
            // *  *
            patterns: vec![SExpr::create_binary(
                PatternPlan {
                    plan_type: RelOp::Join,
                }
                .into(),
                SExpr::create_binary(
                    PatternPlan {
                        plan_type: RelOp::Join,
                    }
                    .into(),
                    SExpr::create_pattern_leaf(),
                    SExpr::create_pattern_leaf(),
                ),
                SExpr::create_binary(
                    PatternPlan {
                        plan_type: RelOp::Join,
                    }
                    .into(),
                    SExpr::create_pattern_leaf(),
                    SExpr::create_pattern_leaf(),
                ),
            )],
        }
    }
}

impl Rule for RuleExchangeJoin {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        // We denote the join tree with:
        //    join1
        //    /   \
        //  join2 join3
        //  /  \   /  \
        // t1  t2 t3  t4
        //
        // After applying the transform, we will get:
        //    join4
        //    /   \
        //  join5 join6
        //  /  \   /  \
        // t1  t3 t2  t4
        let join1: Join = s_expr.plan.clone().try_into()?;
        let join2: Join = s_expr.child(0)?.plan.clone().try_into()?;
        let join3: Join = s_expr.child(1)?.plan.clone().try_into()?;
        let t1 = s_expr.child(0)?.child(0)?;
        let t2 = s_expr.child(0)?.child(1)?;
        let t3 = s_expr.child(1)?.child(0)?;
        let t4 = s_expr.child(1)?.child(1)?;

        // Ensure inner joins or cross joins.
        if !matches!(join1.join_type, JoinType::Inner | JoinType::Cross)
            || !matches!(join2.join_type, JoinType::Inner | JoinType::Cross)
            || !matches!(join3.join_type, JoinType::Inner | JoinType::Cross)
        {
            return Ok(());
        }

        // Check if original sexpr contains cross join.
        // We will reject the results contain cross join if there is no cross join in original sexpr.
        let contains_cross_join = join1.join_type == JoinType::Cross
            || join2.join_type == JoinType::Cross
            || join3.join_type == JoinType::Cross;

        let predicates = vec![
            get_join_predicates(&join1)?,
            get_join_predicates(&join2)?,
            get_join_predicates(&join3)?,
        ]
        .concat();

        let mut join_4 = Join::default();
        let mut join_5 = Join::default();
        let mut join_6 = Join::default();

        let t1_prop = RelExpr::with_s_expr(t1).derive_relational_prop()?;
        let t2_prop = RelExpr::with_s_expr(t2).derive_relational_prop()?;
        let t3_prop = RelExpr::with_s_expr(t3).derive_relational_prop()?;
        let t4_prop = RelExpr::with_s_expr(t4).derive_relational_prop()?;
        let join5_prop = RelExpr::with_s_expr(&SExpr::create_binary(
            join_5.clone().into(),
            t1.clone(),
            t3.clone(),
        ))
        .derive_relational_prop()?;
        let join6_prop = RelExpr::with_s_expr(&SExpr::create_binary(
            join_6.clone().into(),
            t2.clone(),
            t4.clone(),
        ))
        .derive_relational_prop()?;

        let mut join_5_preds = vec![];
        let mut join_6_preds = vec![];

        // Resolve predicates for join3
        for predicate in predicates.iter() {
            let join_pred = JoinPredicate::new(predicate, &join5_prop, &join6_prop);
            match join_pred {
                JoinPredicate::Left(pred) => {
                    join_5_preds.push(pred.clone());
                }
                JoinPredicate::Right(pred) => {
                    join_6_preds.push(pred.clone());
                }
                JoinPredicate::Both { left, right, .. } => {
                    join_4.left_conditions.push(left.clone());
                    join_4.right_conditions.push(right.clone());
                }
                JoinPredicate::Other(pred) => {
                    join_4.non_equi_conditions.push(pred.clone());
                }
            }
        }

        if !join_4.left_conditions.is_empty() && !join_4.right_conditions.is_empty() {
            join_4.join_type = JoinType::Inner;
        }

        // Resolve predicates for join5
        for predicate in join_5_preds.iter() {
            let join_pred = JoinPredicate::new(predicate, &t1_prop, &t3_prop);
            match join_pred {
                JoinPredicate::Left(_) | JoinPredicate::Right(_) | JoinPredicate::Other(_) => {
                    // TODO(leiysky): push down the predicate
                    join_5.non_equi_conditions.push(predicate.clone());
                }
                JoinPredicate::Both { left, right, .. } => {
                    join_5.left_conditions.push(left.clone());
                    join_5.right_conditions.push(right.clone());
                }
            }
        }

        if !join_5.left_conditions.is_empty() && !join_5.right_conditions.is_empty() {
            join_5.join_type = JoinType::Inner;
        }

        // Resolve predicates for join6
        for predicate in join_6_preds.iter() {
            let join_pred = JoinPredicate::new(predicate, &t2_prop, &t4_prop);
            match join_pred {
                JoinPredicate::Left(_) | JoinPredicate::Right(_) | JoinPredicate::Other(_) => {
                    // TODO(leiysky): push down the predicate
                    join_6.non_equi_conditions.push(predicate.clone());
                }
                JoinPredicate::Both { left, right, .. } => {
                    join_6.left_conditions.push(left.clone());
                    join_6.right_conditions.push(right.clone());
                }
            }
        }

        if !join_6.left_conditions.is_empty() && !join_6.right_conditions.is_empty() {
            join_6.join_type = JoinType::Inner;
        }

        // Reject inefficient cross join
        if !contains_cross_join
            && (join_4.join_type == JoinType::Cross
                || join_5.join_type == JoinType::Cross
                || join_6.join_type == JoinType::Cross)
        {
            return Ok(());
        }

        let mut result = SExpr::create(
            join_4.into(),
            vec![
                SExpr::create_binary(join_5.into(), t1.clone(), t3.clone()),
                SExpr::create_binary(join_6.into(), t2.clone(), t4.clone()),
            ],
            None,
            None,
        );

        // Disable the following rules for join 4
        result.set_applied_rule(&RuleID::CommuteJoin);
        result.set_applied_rule(&RuleID::CommuteJoinBaseTable);
        result.set_applied_rule(&RuleID::LeftAssociateJoin);
        result.set_applied_rule(&RuleID::LeftExchangeJoin);
        result.set_applied_rule(&RuleID::RightAssociateJoin);
        result.set_applied_rule(&RuleID::RightExchangeJoin);
        result.set_applied_rule(&RuleID::ExchangeJoin);

        state.add_result(result);

        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }

    fn transformation(&self) -> bool {
        false
    }
}
