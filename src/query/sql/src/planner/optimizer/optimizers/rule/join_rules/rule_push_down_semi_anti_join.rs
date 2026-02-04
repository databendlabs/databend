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

use databend_common_exception::Result;

use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOp;

/// Push down left semi/anti join below a left child inner/cross/semi/anti join
/// when the semi/anti join predicates do not reference one side of the
/// inner/cross/semi/anti join. It can be pushed down to either the left or right
/// side depending on which side is unused.
pub struct RulePushDownSemiAntiJoin {
    id: RuleID,
    matchers: Vec<Matcher>,
}

const COST_EPSILON: f64 = 1e-6;

impl RulePushDownSemiAntiJoin {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownSemiAntiJoin,
            // Join
            // | \
            // Join  *
            // | \
            // *  *
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

    fn estimate_join_cost(join_expr: &SExpr) -> Result<Option<f64>> {
        let left_card = RelExpr::with_s_expr(join_expr.child(0)?)
            .derive_cardinality()?
            .cardinality;
        let right_card = RelExpr::with_s_expr(join_expr.child(1)?)
            .derive_cardinality()?
            .cardinality;
        if !left_card.is_finite() || !right_card.is_finite() {
            return Ok(None);
        }
        if left_card == 0.0 || right_card == 0.0 {
            return Ok(None);
        }
        Ok(Some(left_card + right_card))
    }

    fn estimate_total_cost(top_join: &SExpr, lower_join: &SExpr) -> Result<Option<f64>> {
        let top_cost = Self::estimate_join_cost(top_join)?;
        let lower_cost = Self::estimate_join_cost(lower_join)?;
        Ok(top_cost.and_then(|top_cost| lower_cost.map(|lower_cost| top_cost + lower_cost)))
    }

    fn should_push_down(before_cost: f64, after_cost: Option<f64>) -> bool {
        matches!(after_cost, Some(after_cost) if after_cost + COST_EPSILON < before_cost)
    }
}

impl Rule for RulePushDownSemiAntiJoin {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let join1: Join = s_expr.plan().clone().try_into()?;
        if !matches!(join1.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
            return Ok(());
        }
        if join1.is_lateral || join1.build_side_cache_info.is_some() {
            return Ok(());
        }

        let left_child = s_expr.child(0)?;
        let right_child = s_expr.child(1)?;
        let join2: Join = left_child.plan().clone().try_into()?;
        if !matches!(
            join2.join_type,
            JoinType::Inner | JoinType::Cross | JoinType::LeftSemi | JoinType::LeftAnti
        ) {
            return Ok(());
        }
        if join2.is_lateral || join2.build_side_cache_info.is_some() {
            return Ok(());
        }

        let left_left_child = left_child.child(0)?;
        let left_right_child = left_child.child(1)?;
        let left_child_prop = RelExpr::with_s_expr(left_left_child).derive_relational_prop()?;
        let right_child_prop = RelExpr::with_s_expr(left_right_child).derive_relational_prop()?;
        let join1_used_columns = join1.used_columns()?;

        let can_push_left = join1_used_columns.is_disjoint(&right_child_prop.output_columns);
        let can_push_right = join1_used_columns.is_disjoint(&left_child_prop.output_columns)
            && join2.join_type != JoinType::LeftAnti;

        if !can_push_left && !can_push_right {
            return Ok(());
        }

        let Some(before_cost) = Self::estimate_total_cost(s_expr, left_child)? else {
            return Ok(());
        };

        if can_push_left {
            let new_left =
                SExpr::create_binary(join1.clone(), left_left_child.clone(), right_child.clone());
            let mut result =
                SExpr::create_binary(join2.clone(), new_left, left_right_child.clone());
            let after_cost = Self::estimate_total_cost(&result, result.child(0)?)?;
            if Self::should_push_down(before_cost, after_cost) {
                result.set_applied_rule(&self.id);
                state.add_result(result);
            }
        }

        if can_push_right {
            let new_right =
                SExpr::create_binary(join1.clone(), left_right_child.clone(), right_child.clone());
            let mut result = SExpr::create_binary(join2, left_left_child.clone(), new_right);
            let after_cost = Self::estimate_total_cost(&result, result.child(1)?)?;
            if Self::should_push_down(before_cost, after_cost) {
                result.set_applied_rule(&self.id);
                state.add_result(result);
            }
        }

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RulePushDownSemiAntiJoin {
    fn default() -> Self {
        Self::new()
    }
}
