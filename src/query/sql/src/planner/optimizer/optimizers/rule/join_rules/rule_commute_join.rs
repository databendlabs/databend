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

use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOp;
use crate::plans::RelOperator;

fn contains_recursive_cte(expr: &SExpr) -> bool {
    matches!(expr.plan(), RelOperator::RecursiveCteScan(_))
        || expr.children().any(contains_recursive_cte)
}

fn join_build_cardinality(stat: &StatInfo) -> f64 {
    if stat.cardinality_is_severely_underestimated() && stat.max_cardinality.is_finite() {
        stat.max_cardinality.max(stat.cardinality)
    } else {
        // Unknown bounds still block automatic broadcast, but they should not
        // reorder unrelated joins. Preserve the existing expected-cardinality
        // ordering unless there is a finite, severe underestimate to correct.
        stat.cardinality
    }
}

fn should_commute(join_type: JoinType, left: &StatInfo, right: &StatInfo) -> bool {
    let left_build_cardinality = join_build_cardinality(left);
    let right_build_cardinality = join_build_cardinality(right);
    if left_build_cardinality < right_build_cardinality
        || (left_build_cardinality == right_build_cardinality
            && left.cardinality < right.cardinality)
    {
        return matches!(
            join_type,
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
        );
    }

    if left_build_cardinality != right_build_cardinality || left.cardinality != right.cardinality {
        return false;
    }

    matches!(
        join_type,
        JoinType::Right | JoinType::RightSingle | JoinType::RightSemi | JoinType::RightAnti
    )
}

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

        if join.join_type == JoinType::Cross
            && (contains_recursive_cte(left_child) || contains_recursive_cte(right_child))
        {
            return Ok(());
        }

        let left_rel_expr = RelExpr::with_s_expr(left_child);
        let right_rel_expr = RelExpr::with_s_expr(right_child);
        let left_stat = left_rel_expr.derive_cardinality()?;
        let right_stat = right_rel_expr.derive_cardinality()?;
        let need_commute = should_commute(join.join_type, &left_stat, &right_stat);
        if need_commute {
            // Swap the join conditions side
            for condition in join.equi_conditions.iter_mut() {
                (condition.left, condition.right) =
                    (condition.right.clone(), condition.left.clone());
            }
            join.join_type = join.join_type.opposite();
            join.single_to_inner = join.single_to_inner.map(|join_type| join_type.opposite());
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

impl Default for RuleCommuteJoin {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::ir::Statistics;

    fn estimated_stat(cardinality: f64) -> StatInfo {
        StatInfo {
            cardinality,
            max_cardinality: cardinality,
            statistics: Statistics::default(),
        }
    }

    #[test]
    fn test_commute_join_prefers_safer_build_cardinality() {
        let left = estimated_stat(1_000.0);
        let mut underestimated_right = estimated_stat(10.0);
        underestimated_right.max_cardinality = 200_000_000.0;

        assert!(should_commute(
            JoinType::Inner,
            &left,
            &underestimated_right
        ));
        assert!(!should_commute(
            JoinType::Inner,
            &underestimated_right,
            &left
        ));
    }

    #[test]
    fn test_commute_join_preserves_expected_order_without_severe_underestimate() {
        let mut selective_left = estimated_stat(1_000_000.0);
        selective_left.max_cardinality = 1_000_000_000.0;
        let right = estimated_stat(100_000_000.0);

        assert!(should_commute(JoinType::Inner, &selective_left, &right));
        assert!(!should_commute(JoinType::Inner, &right, &selective_left));
    }

    #[test]
    fn test_commute_join_preserves_expected_order_for_unknown_risk() {
        let known = estimated_stat(1_000.0);
        let unknown = StatInfo::default();

        assert!(!should_commute(JoinType::Inner, &known, &unknown));
        assert!(should_commute(JoinType::Inner, &unknown, &known));
    }
}
