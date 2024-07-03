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

use std::ops::Deref;
use std::sync::Arc;
use std::vec;

use databend_common_exception::Result;

use super::util::get_join_predicates;
use crate::binder::JoinPredicate;
use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RelExpr;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Join;
use crate::plans::JoinEquiCondition;
use crate::plans::JoinType;
use crate::plans::RelOp;

/// Rule to apply swap on a left-deep join.
/// If we have a join tree like:
///    join
///    /  \
///  join  t3
///  /  \
///  t1  t2
///
/// We can represent it as `(t1 ⋈ t2) ⋈ t3`. With this rule, we can transform
/// it to `(t1 ⋈ t3) ⋈ t2`, which looks like:
///    join
///    /  \
///  join  t2
///  /  \
///  t1  t3
pub struct RuleLeftExchangeJoin {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RuleLeftExchangeJoin {
    pub fn new() -> Self {
        Self {
            id: RuleID::LeftExchangeJoin,

            // LogicalJoin
            // | \
            // |  *
            // LogicalJoin
            // | \
            // |  *
            // *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Join,
                children: vec![
                    Matcher::MatchOp {
                        op_type: RelOp::Join,
                        children: vec![Matcher::Leaf, Matcher::Leaf],
                    },
                    Matcher::Leaf,
                ],
            }],
        }
    }
}

impl Rule for RuleLeftExchangeJoin {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        // We denote the join tree with:
        //    join1
        //    /  \
        //  join2 t3
        //  /  \
        // t1  t2
        //
        // After applying the transform, we will get:
        //    join3
        //    /  \
        //  join4 t2
        //  /  \
        // t1  t3
        let join1: Join = s_expr.plan.deref().clone().try_into()?;
        let join2: Join = s_expr.child(0)?.plan.deref().clone().try_into()?;
        let t1 = s_expr.child(0)?.child(0)?;
        let t2 = s_expr.child(0)?.child(1)?;
        let t3 = s_expr.child(1)?;

        // Ensure inner joins or cross joins.
        if !matches!(join1.join_type, JoinType::Inner | JoinType::Cross)
            || !matches!(join2.join_type, JoinType::Inner | JoinType::Cross)
        {
            return Ok(());
        }

        // Check if original sexpr contains cross join.
        // We will reject the results contain cross join if there is no cross join in original sexpr.
        let contains_cross_join =
            join1.join_type == JoinType::Cross || join2.join_type == JoinType::Cross;

        let predicates = [get_join_predicates(&join1)?, get_join_predicates(&join2)?].concat();

        let mut join_3 = Join::default();
        let mut join_4 = Join::default();

        let t1_prop = RelExpr::with_s_expr(t1).derive_relational_prop()?;
        let t2_prop = RelExpr::with_s_expr(t2).derive_relational_prop()?;
        let t3_prop = RelExpr::with_s_expr(t3).derive_relational_prop()?;
        let join4_prop = RelExpr::with_s_expr(&SExpr::create_binary(
            Arc::new(join_4.clone().into()),
            Arc::new(t1.clone()),
            Arc::new(t3.clone()),
        ))
        .derive_relational_prop()?;

        let mut join_4_preds = vec![];

        // Resolve predicates for join3
        for predicate in predicates.iter() {
            let join_pred = JoinPredicate::new(predicate, &join4_prop, &t2_prop);
            match join_pred {
                JoinPredicate::ALL(pred) => {
                    join_4_preds.push(pred.clone());
                }
                JoinPredicate::Left(pred) => {
                    join_4_preds.push(pred.clone());
                }
                JoinPredicate::Right(pred) => {
                    // TODO(leiysky): push down the predicate
                    join_3.non_equi_conditions.push(pred.clone());
                }
                JoinPredicate::Both {
                    left,
                    right,
                    is_equal_op,
                } => {
                    if is_equal_op {
                        join_3.equi_conditions.push(JoinEquiCondition::new(
                            left.clone(),
                            right.clone(),
                            false,
                        ));
                    } else {
                        join_3.non_equi_conditions.push(predicate.clone());
                    }
                }
                JoinPredicate::Other(pred) => {
                    join_3.non_equi_conditions.push(pred.clone());
                }
            }
        }

        if !join_3.equi_conditions.is_empty() {
            join_3.join_type = JoinType::Inner;
        }

        // Resolve predicates for join4
        for predicate in join_4_preds.iter() {
            let join_pred = JoinPredicate::new(predicate, &t1_prop, &t3_prop);
            match join_pred {
                JoinPredicate::ALL(_)
                | JoinPredicate::Left(_)
                | JoinPredicate::Right(_)
                | JoinPredicate::Other(_) => {
                    // TODO(leiysky): push down the predicate
                    join_4.non_equi_conditions.push(predicate.clone());
                }
                JoinPredicate::Both {
                    left,
                    right,
                    is_equal_op,
                } => {
                    if is_equal_op {
                        join_4.equi_conditions.push(JoinEquiCondition::new(
                            left.clone(),
                            right.clone(),
                            false,
                        ));
                    } else {
                        join_4.non_equi_conditions.push(predicate.clone());
                    }
                }
            }
        }

        if !join_4.equi_conditions.is_empty() {
            join_4.join_type = JoinType::Inner;
        }

        // Reject inefficient cross join
        if !contains_cross_join
            && (join_3.join_type == JoinType::Cross || join_4.join_type == JoinType::Cross)
        {
            return Ok(());
        }

        let mut result = SExpr::create(
            Arc::new(join_3.into()),
            vec![
                Arc::new(SExpr::create_binary(
                    Arc::new(join_4.into()),
                    Arc::new(t1.clone()),
                    Arc::new(t3.clone()),
                )),
                Arc::new(t2.clone()),
            ],
            None,
            None,
            None,
        );

        // Disable the following rules for join 3
        result.set_applied_rule(&RuleID::CommuteJoinBaseTable);
        result.set_applied_rule(&RuleID::LeftExchangeJoin);

        state.add_result(result);

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }

    fn transformation(&self) -> bool {
        false
    }
}
