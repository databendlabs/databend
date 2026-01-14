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
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::Scan;
use crate::plans::SecureFilter;
use crate::plans::Sort;
use crate::plans::Visitor;

/// Matches: Sort -> [EvalScalar ->] Filter -> [SecureFilter ->] Scan
///
/// Push down order_by and limit from Sort to Scan when filter predicates
/// use inverted_index or vector_index. SecureFilter (from row access policy)
/// is preserved in the plan tree if present.
pub struct RulePushDownSortFilterScan {
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

impl RulePushDownSortFilterScan {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownSortFilterScan,
            matchers: vec![
                // Sort -> Filter -> Scan
                match_op!(Sort, match_op!(Filter, match_op!(Scan))),
                // Sort -> Filter -> SecureFilter -> Scan
                match_op!(
                    Sort,
                    match_op!(Filter, match_op!(SecureFilter, match_op!(Scan)))
                ),
                // Sort -> EvalScalar -> Filter -> Scan
                match_op!(
                    Sort,
                    match_op!(EvalScalar, match_op!(Filter, match_op!(Scan)))
                ),
                // Sort -> EvalScalar -> Filter -> SecureFilter -> Scan
                match_op!(
                    Sort,
                    match_op!(
                        EvalScalar,
                        match_op!(Filter, match_op!(SecureFilter, match_op!(Scan)))
                    )
                ),
            ],
        }
    }
}

impl Rule for RulePushDownSortFilterScan {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let sort: Sort = s_expr.plan().clone().try_into()?;

        let child = s_expr.child(0)?;
        let (eval_scalar, filter, mut scan, secure_filter) = match child.plan() {
            RelOperator::Filter(filter) => {
                let scan_parent_expr = child.child(0)?;
                match scan_parent_expr.plan() {
                    RelOperator::Scan(_) => {
                        let scan: Scan = scan_parent_expr.plan().clone().try_into()?;
                        (None, filter.clone(), scan, None)
                    }
                    RelOperator::SecureFilter(_) => {
                        let secure_filter: SecureFilter =
                            scan_parent_expr.plan().clone().try_into()?;
                        let scan: Scan = scan_parent_expr.child(0)?.plan().clone().try_into()?;
                        (None, filter.clone(), scan, Some(secure_filter))
                    }
                    _ => unreachable!(),
                }
            }
            RelOperator::EvalScalar(eval_scalar) => {
                let filter_expr = child.child(0)?;
                let filter: Filter = filter_expr.plan().clone().try_into()?;
                let scan_parent_expr = filter_expr.child(0)?;
                match scan_parent_expr.plan() {
                    RelOperator::Scan(_) => {
                        let scan: Scan = scan_parent_expr.plan().clone().try_into()?;
                        (Some(eval_scalar.clone()), filter, scan, None)
                    }
                    RelOperator::SecureFilter(_) => {
                        let secure_filter: SecureFilter =
                            scan_parent_expr.plan().clone().try_into()?;
                        let scan: Scan = scan_parent_expr.child(0)?.plan().clone().try_into()?;
                        (Some(eval_scalar.clone()), filter, scan, Some(secure_filter))
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        };

        // The following conditions must be met to push down filter and sort for index:
        // 1. Scan must contain vector_index or inverted_index, because we can use the index
        //    to determine which rows in the block match, and the topn pruner can use this information
        //    to retain only the matched blocks.
        // 2. The number of `push_down_predicates` in Scan must be the same as the number of `predicates`
        //    in Filter to ensure that all filter conditions are pushed down.
        //    (Filter `predicates` has been pushed down in `RulePushDownFilterScan` rule.)
        // 3. Sort must have limit in order to prune unused blocks.
        let push_down_predicates = scan.push_down_predicates.clone().unwrap_or_default();
        let has_inverted_index = scan.inverted_index.is_some();
        let has_vector_index = scan.vector_index.is_some();
        if (!has_inverted_index && !has_vector_index)
            || push_down_predicates.len() != filter.predicates.len()
            || sort.limit.is_none()
            || !filter_contains_only_index_predicates(&filter, has_inverted_index, has_vector_index)
        {
            return Ok(());
        }

        scan.order_by = Some(sort.items);
        // Note: Unlike other rules, we don't need SecureFilter safety check here.
        // The condition above (push_down_predicates.len() == filter.predicates.len())
        // guarantees that predicates are non-empty when we reach here.
        // With non-empty filters, storage layer won't trigger early termination,
        // so pushing down limit is always safe in this rule.
        scan.limit = sort.limit;

        let new_scan = SExpr::create_leaf(Arc::new(RelOperator::Scan(scan)));
        let new_scan_parent = if let Some(secure_filter) = secure_filter {
            SExpr::create_unary(
                Arc::new(RelOperator::SecureFilter(secure_filter)),
                Arc::new(new_scan),
            )
        } else {
            new_scan
        };

        let mut result = if eval_scalar.is_some() {
            let filter_expr = child.child(0)?;
            let new_filter = filter_expr.replace_children(vec![Arc::new(new_scan_parent)]);
            let new_eval_scalar = child.replace_children(vec![Arc::new(new_filter)]);
            s_expr.replace_children(vec![Arc::new(new_eval_scalar)])
        } else {
            let new_filter = child.replace_children(vec![Arc::new(new_scan_parent)]);
            s_expr.replace_children(vec![Arc::new(new_filter)])
        };

        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RulePushDownSortFilterScan {
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
