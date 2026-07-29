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

fn should_commute(join_type: JoinType, left: &StatInfo, right: &StatInfo) -> bool {
    if left.cardinality < right.cardinality {
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

    if left.cardinality != right.cardinality {
        return false;
    }

    if left.cardinality == 0.0 && matches!(join_type, JoinType::Left | JoinType::Right) {
        let left_proven_empty = left.statistics.precise_cardinality == Some(0);
        let right_proven_empty = right.statistics.precise_cardinality == Some(0);
        if left_proven_empty != right_proven_empty {
            // The right child is the hash-build side. Prefer the input that is
            // known to be empty over one whose zero cardinality is only estimated.
            return left_proven_empty;
        }
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

    fn empty_stat(precise: bool) -> StatInfo {
        StatInfo {
            cardinality: 0.0,
            statistics: Statistics {
                precise_cardinality: precise.then_some(0),
                column_stats: Default::default(),
                top_n: Default::default(),
                count_min_sketch: Default::default(),
            },
        }
    }

    #[test]
    fn test_outer_join_zero_tie_prefers_proven_empty_build_side() {
        let proven_empty = empty_stat(true);
        let estimated_empty = empty_stat(false);

        assert!(should_commute(
            JoinType::Left,
            &proven_empty,
            &estimated_empty
        ));
        assert!(!should_commute(
            JoinType::Left,
            &estimated_empty,
            &proven_empty
        ));
        assert!(should_commute(
            JoinType::Right,
            &proven_empty,
            &estimated_empty
        ));
        assert!(!should_commute(
            JoinType::Right,
            &estimated_empty,
            &proven_empty
        ));
    }

    #[test]
    fn test_outer_join_zero_tie_preserves_existing_canonicalization() {
        let left = empty_stat(false);
        let right = empty_stat(false);

        assert!(!should_commute(JoinType::Left, &left, &right));
        assert!(should_commute(JoinType::Right, &left, &right));
    }
}
