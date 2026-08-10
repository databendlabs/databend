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
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;

/// Null addition rule: for a join with null-rejecting equi conditions
/// (i.e. `=`, not null-safe `IS NOT DISTINCT FROM`), rows with NULL join keys
/// can never match, so we may derive `is_not_null(join_key)` filters below the
/// join on the branches whose NULL-key rows contribute nothing to the result:
///
/// - Inner join: both branches (a NULL key on either side never matches,
///   and unmatched rows are not preserved).
/// - Left outer join: only the right (null-supplying) branch. NULL-key rows
///   on the preserved left branch are still emitted with NULL extension, so
///   filtering them would change results.
/// - Right outer join: symmetric to left.
/// - LeftSemi/RightSemi: both branches (semi joins never preserve unmatched
///   probe rows, and NULL keys never match).
/// - Full outer join: neither branch is safe.
///
/// The derived predicates carry `derived_from = Some("null_addition")`: they
/// are redundant for semantics (the join would drop those rows anyway) and
/// therefore must never introduce new evaluation errors. Their purpose is to
/// seed downstream rules with null-rejection information, e.g. once pushed
/// into a subquery branch, `outer_join_to_inner_join` can eliminate an outer
/// join whose null-supplying side is constrained by the derived `is_not_null`:
///
/// ```text
///   Join(a.id = v.id)                          Join(a.id = v.id)
///   ├── a                          ──►         ├── Filter(derived: is_not_null(a.id))
///   └── v = (b LEFT JOIN c ...)                │   └── a
///                                              └── Filter(derived: is_not_null(v.id))
///                                                  └── v = (b LEFT JOIN c ...)
///                                                  ... then pushdown turns the outer
///                                                  join inside v into an inner join
/// ```
pub struct RuleNullAddition {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RuleNullAddition {
    pub fn new() -> Self {
        Self {
            id: RuleID::NullAddition,
            // Join
            // |  \
            // *   *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Join,
                children: vec![Matcher::Leaf, Matcher::Leaf],
            }],
        }
    }
}

impl Rule for RuleNullAddition {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let join: Join = s_expr.plan().clone().try_into()?;
        // Which branches may drop NULL-key rows? See the type-level comment.
        let (filter_left, filter_right) = match join.join_type {
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi => (true, true),
            JoinType::Left => (false, true),
            JoinType::Right => (true, false),
            _ => (false, false),
        };
        if join.is_lateral || join.equi_conditions.is_empty() || (!filter_left && !filter_right) {
            return Ok(());
        }

        let mut left_null_predicates = vec![];
        let mut right_null_predicates = vec![];
        for condition in join.equi_conditions.iter() {
            // Null-safe equality (IS NOT DISTINCT FROM) does not reject NULLs.
            if condition.is_null_equal {
                continue;
            }
            if filter_left && let Some(predicate) = nullable_key_filter(&condition.left) {
                left_null_predicates.push(predicate);
            }
            if filter_right && let Some(predicate) = nullable_key_filter(&condition.right) {
                right_null_predicates.push(predicate);
            }
        }

        let mut changed = false;
        let mut left_child = s_expr.child(0)?.clone();
        if let Some(filter_expr) = add_derived_null_filter(&left_child, left_null_predicates)? {
            left_child = filter_expr;
            changed = true;
        }
        let mut right_child = s_expr.child(1)?.clone();
        if let Some(filter_expr) = add_derived_null_filter(&right_child, right_null_predicates)? {
            right_child = filter_expr;
            changed = true;
        }

        if changed {
            let mut res =
                s_expr.replace_children(vec![Arc::new(left_child), Arc::new(right_child)]);
            res.set_applied_rule(&self.id());
            state.add_result(res);
        }
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

/// Build a derived `is_not_null(key)` for a nullable join key column
/// reference. Complex key expressions are evaluated by an `EvalScalar` below
/// the join, so equi keys are plain column references at this point.
fn nullable_key_filter(key: &ScalarExpr) -> Option<ScalarExpr> {
    match key {
        ScalarExpr::BoundColumnRef(column_ref)
            if column_ref.column.data_type.is_nullable_or_null() =>
        {
            Some(ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "is_not_null".to_string(),
                params: vec![],
                arguments: vec![key.clone()],
                derived_from: Some("null_addition"),
            }))
        }
        _ => None,
    }
}

/// Wrap `child` with a filter holding the derived `is_not_null` predicates.
/// Predicates that are already present directly above the child are skipped,
/// so re-application (e.g. after the child is rebuilt by other rules) does
/// not stack duplicates.
fn add_derived_null_filter(
    child: &SExpr,
    mut predicates: Vec<ScalarExpr>,
) -> Result<Option<SExpr>> {
    if predicates.is_empty() {
        return Ok(None);
    }
    if let RelOperator::Filter(filter) = child.plan() {
        predicates.retain(|predicate| !filter.predicates.contains(predicate));
    }
    if predicates.is_empty() {
        return Ok(None);
    }
    Ok(Some(SExpr::create_unary(
        Arc::new(RelOperator::Filter(Filter { predicates })),
        Arc::new(child.clone()),
    )))
}

impl Default for RuleNullAddition {
    fn default() -> Self {
        Self::new()
    }
}
