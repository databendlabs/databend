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

use std::cmp;
use std::sync::Arc;

use databend_common_exception::Result;

use crate::match_op;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::RelOperator;
use crate::plans::Scan;
use crate::plans::SecureFilter;
use crate::plans::Sort;

/// Matches: Sort -> [EvalScalar ->] [SecureFilter ->] Scan
///
/// Push down order_by and limit from Sort to Scan. SecureFilter (from row access policy)
/// is preserved in the plan tree if present.
pub struct RulePushDownSortScan {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RulePushDownSortScan {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownSortScan,
            matchers: vec![
                match_op!(Sort -> Scan),
                match_op!(Sort -> SecureFilter -> Scan),
                match_op!(Sort -> EvalScalar -> Scan),
                match_op!(Sort -> EvalScalar -> SecureFilter -> Scan),
            ],
        }
    }
}

impl Rule for RulePushDownSortScan {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let sort: Sort = s_expr.plan().clone().try_into()?;
        let child = s_expr.child(0)?;

        let (eval_scalar, secure_filter, mut scan) = match child.plan() {
            RelOperator::Scan(scan) => (None, None, scan.clone()),
            RelOperator::SecureFilter(_) => {
                let secure_filter: SecureFilter = child.plan().clone().try_into()?;
                let scan: Scan = child.child(0)?.plan().clone().try_into()?;
                (None, Some(secure_filter), scan)
            }
            RelOperator::EvalScalar(eval_scalar) => {
                let grand_child = child.child(0)?;
                match grand_child.plan() {
                    RelOperator::Scan(scan) => (Some(eval_scalar.clone()), None, scan.clone()),
                    RelOperator::SecureFilter(_) => {
                        let secure_filter: SecureFilter = grand_child.plan().clone().try_into()?;
                        let scan: Scan = grand_child.child(0)?.plan().clone().try_into()?;
                        (Some(eval_scalar.clone()), Some(secure_filter), scan)
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        };

        if scan.order_by.is_none() {
            scan.order_by = Some(sort.items);
        }

        // When SecureFilter exists, pushing limit is unsafe if push_down_predicates is empty.
        // Storage triggers TopK pruning with (order_by + limit + no predicates), returning N rows
        // before SecureFilter can filter them, causing fewer results than expected.
        let can_push_limit = secure_filter.is_none()
            || scan
                .push_down_predicates
                .as_ref()
                .is_some_and(|preds| !preds.is_empty());

        if can_push_limit {
            if let Some(limit) = sort.limit {
                scan.limit = Some(scan.limit.map_or(limit, |c| cmp::max(c, limit)));
            }
        }

        let scan_expr = SExpr::create_leaf(Arc::new(RelOperator::Scan(scan)));
        let scan_parent_expr = if let Some(secure_filter) = secure_filter {
            SExpr::create_unary(
                Arc::new(RelOperator::SecureFilter(secure_filter)),
                Arc::new(scan_expr),
            )
        } else {
            scan_expr
        };
        let new_child_expr = if let Some(eval_scalar) = eval_scalar {
            SExpr::create_unary(
                Arc::new(RelOperator::EvalScalar(eval_scalar)),
                Arc::new(scan_parent_expr),
            )
        } else {
            scan_parent_expr
        };

        let mut result = s_expr.replace_children(vec![Arc::new(new_child_expr)]);

        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RulePushDownSortScan {
    fn default() -> Self {
        Self::new()
    }
}
