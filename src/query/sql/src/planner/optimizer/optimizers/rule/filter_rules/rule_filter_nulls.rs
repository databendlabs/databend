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
use crate::optimizer::ir::Statistics;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::ScalarExpr;

const NULL_THRESHOLD_RATIO: f64 = 0.2;

/// A rule that filters out NULL values from join keys when they exceed a certain threshold.
/// This optimization can improve join performance by reducing the amount of data processed.
pub struct RuleFilterNulls {
    id: RuleID,
    matchers: Vec<Matcher>,
    is_distributed: bool,
}

impl RuleFilterNulls {
    pub fn new(is_distributed: bool) -> Self {
        Self {
            id: RuleID::FilterNulls,
            // Join
            // /  \
            //... ...
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Join,
                children: vec![Matcher::Leaf, Matcher::Leaf],
            }],
            is_distributed,
        }
    }

    /// Checks if null filtering should be applied to a join key based on statistics.
    ///
    /// # Arguments
    /// * `key_expr` - The join key expression to check
    /// * `stats` - Statistics for the relation containing the key
    /// * `cardinality` - Total cardinality of the relation
    ///
    /// # Returns
    /// Some(filter) if null filtering should be applied, None otherwise
    fn should_filter_nulls(
        key_expr: &ScalarExpr,
        stats: &Statistics,
        cardinality: f64,
    ) -> Option<ScalarExpr> {
        if !key_expr.has_one_column_ref() {
            return None;
        }

        let column_id = *key_expr.used_columns().iter().next()?;
        let column_stats = stats.column_stats.get(&column_id)?;

        if cardinality == 0.0 {
            return None;
        }

        if (column_stats.null_count as f64 / cardinality) >= NULL_THRESHOLD_RATIO {
            Some(join_key_null_filter(key_expr))
        } else {
            None
        }
    }
}

impl Rule for RuleFilterNulls {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        if !self.is_distributed {
            state.add_result(s_expr.clone());
            return Ok(());
        }
        let join: Join = s_expr.plan().clone().try_into()?;
        if !matches!(
            join.join_type,
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
        ) || join.is_lateral
        {
            state.add_result(s_expr.clone());
            return Ok(());
        }
        let mut left_child = s_expr.child(0)?.clone();
        let mut right_child = s_expr.child(1)?.clone();

        let left_stat = RelExpr::with_s_expr(&left_child).derive_cardinality()?;
        let right_stat = RelExpr::with_s_expr(&right_child).derive_cardinality()?;
        let mut left_null_predicates = vec![];
        let mut right_null_predicates = vec![];
        for join_key in join.equi_conditions.iter() {
            let left_key = &join_key.left;
            let right_key = &join_key.right;
            if single_plan(&left_child) {
                if let Some(filter) = Self::should_filter_nulls(
                    left_key,
                    &left_stat.statistics,
                    left_stat.cardinality,
                ) {
                    left_null_predicates.push(filter);
                }
            }
            if single_plan(&right_child) {
                if let Some(filter) = Self::should_filter_nulls(
                    right_key,
                    &right_stat.statistics,
                    right_stat.cardinality,
                ) {
                    right_null_predicates.push(filter);
                }
            }
        }
        if !left_null_predicates.is_empty() {
            let left_null_filter = Filter {
                predicates: left_null_predicates,
            };
            left_child = SExpr::create_unary(
                Arc::new(RelOperator::Filter(left_null_filter)),
                Arc::new(left_child.clone()),
            );
        }

        if !right_null_predicates.is_empty() {
            let right_null_filter = Filter {
                predicates: right_null_predicates,
            };
            right_child = SExpr::create_unary(
                Arc::new(RelOperator::Filter(right_null_filter)),
                Arc::new(right_child.clone()),
            );
        }

        let mut res = s_expr.replace_children(vec![Arc::new(left_child), Arc::new(right_child)]);
        res.set_applied_rule(&self.id());
        state.add_result(res);

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

/// Creates a NOT NULL filter predicate for a given join key.
fn join_key_null_filter(key: &ScalarExpr) -> ScalarExpr {
    ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "is_not_null".to_string(),
        params: vec![],
        arguments: vec![key.clone()],
    })
}

/// Checks if an expression represents a single-path plan.
///
/// A single-path plan is one that follows a linear chain of operations,
/// meaning each node has at most one child. Examples include:
/// - Leaf nodes (table scans)
/// - Linear chains of unary operators (such as: scan -> filter -> project -> limit)
fn single_plan(expr: &SExpr) -> bool {
    let children = &expr.children;
    if children.len() > 1 {
        return false;
    }
    if children.is_empty() {
        return true;
    }
    single_plan(&children[0])
}
