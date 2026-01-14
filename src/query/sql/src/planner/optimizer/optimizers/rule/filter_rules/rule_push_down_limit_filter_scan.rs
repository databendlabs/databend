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
use crate::plans::IndexPredicateChecker;
use crate::plans::Limit;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::Scan;
use crate::plans::Visitor;

/// Matches: Limit -> [EvalScalar ->] Filter -> [SecureFilter ->] Scan
///
/// Push down limit to Scan when filter predicates use inverted_index or vector_index.
/// SecureFilter (from row access policy) is preserved in the plan tree if present.
pub struct RulePushDownLimitFilterScan {
    id: RuleID,
    matchers: Vec<Matcher>,
}

macro_rules! match_op {
    ($op:ident) => {
        Matcher::MatchOp {
            op_type: RelOp::$op,
            children: vec![],
        }
    };
    ($op:ident, $($child:expr),+) => {
        Matcher::MatchOp {
            op_type: RelOp::$op,
            children: vec![$($child),+],
        }
    };
}

impl RulePushDownLimitFilterScan {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownLimitFilterScan,
            matchers: vec![
                // Limit -> Filter -> Scan
                match_op!(Limit, match_op!(Filter, match_op!(Scan))),
                // Limit -> Filter -> SecureFilter -> Scan
                match_op!(
                    Limit,
                    match_op!(Filter, match_op!(SecureFilter, match_op!(Scan)))
                ),
                // Limit -> EvalScalar -> Filter -> Scan
                match_op!(
                    Limit,
                    match_op!(EvalScalar, match_op!(Filter, match_op!(Scan)))
                ),
                // Limit -> EvalScalar -> Filter -> SecureFilter -> Scan
                match_op!(
                    Limit,
                    match_op!(
                        EvalScalar,
                        match_op!(Filter, match_op!(SecureFilter, match_op!(Scan)))
                    )
                ),
            ],
        }
    }
}

impl Rule for RulePushDownLimitFilterScan {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let limit: Limit = s_expr.plan().clone().try_into()?;
        let limit_child = s_expr.child(0)?;

        let (eval_scalar_expr, filter_expr) = match limit_child.plan() {
            RelOperator::Filter(_) => (None, limit_child.clone()),
            RelOperator::EvalScalar(_) => {
                (Some(limit_child.clone()), limit_child.child(0)?.clone())
            }
            _ => unreachable!(),
        };

        let filter: Filter = filter_expr.plan().clone().try_into()?;
        let scan_parent_expr = filter_expr.child(0)?;
        let scan_expr = match scan_parent_expr.plan() {
            RelOperator::Scan(_) => scan_parent_expr.clone(),
            RelOperator::SecureFilter(_) => scan_parent_expr.child(0)?.clone(),
            _ => unreachable!(),
        };
        let mut scan: Scan = scan_expr.plan().clone().try_into()?;

        // The following conditions must be met to push down filter and limit for index:
        // 1. Scan must contain vector_index or inverted_index, because we can use the index
        //    to determine which rows in the block match, and the topn pruner can use this information
        //    to retain only the matched blocks.
        // 2. The number of `push_down_predicates` in Scan must be the same as the number of `predicates`
        //    in Filter to ensure that all filter conditions are pushed down.
        //    (Filter `predicates` has been pushed down in `RulePushDownFilterScan` rule.)
        // 3. Limit must have limit in order to prune unused blocks.
        let push_down_predicates = scan.push_down_predicates.clone().unwrap_or_default();
        let has_inverted_index = scan.inverted_index.is_some();
        let has_vector_index = scan.vector_index.is_some();
        if (!has_inverted_index && !has_vector_index)
            || push_down_predicates.len() != filter.predicates.len()
            || limit.limit.is_none()
            || !filter_contains_only_index_predicates(&filter, has_inverted_index, has_vector_index)
        {
            return Ok(());
        }

        scan.limit = Some(limit.limit.unwrap() + limit.offset);
        let new_scan_expr = SExpr::create_leaf(Arc::new(RelOperator::Scan(scan)));
        let new_scan_parent_expr =
            if matches!(scan_parent_expr.plan(), RelOperator::SecureFilter(_)) {
                SExpr::create_unary(
                    Arc::new(scan_parent_expr.plan().clone()),
                    Arc::new(new_scan_expr),
                )
            } else {
                new_scan_expr
            };

        let new_filter_expr = filter_expr.replace_children(vec![Arc::new(new_scan_parent_expr)]);
        let mut result = if let Some(eval_scalar_expr) = eval_scalar_expr {
            let new_eval_scalar_expr =
                eval_scalar_expr.replace_children(vec![Arc::new(new_filter_expr)]);
            s_expr.replace_children(vec![Arc::new(new_eval_scalar_expr)])
        } else {
            s_expr.replace_children(vec![Arc::new(new_filter_expr)])
        };

        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RulePushDownLimitFilterScan {
    fn default() -> Self {
        Self::new()
    }
}

fn filter_contains_only_index_predicates(
    filter: &Filter,
    has_inverted_index: bool,
    has_vector_index: bool,
) -> bool {
    if !has_inverted_index && !has_vector_index {
        return false;
    }

    filter
        .predicates
        .iter()
        .all(|predicate| is_index_predicate(predicate, has_inverted_index, has_vector_index))
}

fn is_index_predicate(predicate: &ScalarExpr, allow_inverted: bool, allow_vector: bool) -> bool {
    let mut checker = IndexPredicateChecker::new(allow_inverted, allow_vector);
    let _ = checker.visit(predicate);
    checker.valid && checker.has_index_column
}
