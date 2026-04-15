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
use crate::plans::Sort;

/// Matches: Sort -> [EvalScalar ->] Scan
///
/// Push down order_by and limit from Sort to Scan.
/// When `secure_predicates` is present, limit is not pushed down because
/// storage TopK pruning would return N rows based only on user predicates,
/// then secure predicates filter them further, yielding fewer rows than requested.
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
                match_op!(Sort -> EvalScalar -> Scan),
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

        let (eval_scalar, mut scan) = match child.plan() {
            RelOperator::Scan(scan) => (None, scan.clone()),
            RelOperator::EvalScalar(eval_scalar) => {
                let grand_child = child.child(0)?;
                let scan: Scan = grand_child.plan().clone().try_into()?;
                (Some(eval_scalar.clone()), scan)
            }
            _ => unreachable!(),
        };

        if scan.order_by.is_none() {
            scan.order_by = Some(sort.items);
        }

        // When row access policy is active, pushing limit is unsafe because
        // secure predicates are applied after storage-level TopK pruning.
        let can_push_limit = scan.secure_predicates.is_none();

        if can_push_limit {
            if let Some(limit) = sort.limit {
                scan.limit = Some(scan.limit.map_or(limit, |c| cmp::max(c, limit)));
            }
        }

        let scan_expr = SExpr::create_leaf(Arc::new(RelOperator::Scan(scan)));
        let new_child_expr = if let Some(eval_scalar) = eval_scalar {
            SExpr::create_unary(
                Arc::new(RelOperator::EvalScalar(eval_scalar)),
                Arc::new(scan_expr),
            )
        } else {
            scan_expr
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
