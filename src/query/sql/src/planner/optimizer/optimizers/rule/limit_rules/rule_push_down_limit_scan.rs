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

use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Limit;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::Scan;
use crate::plans::SecureFilter;

/// Input:
/// (1)    Limit
///          \
///          Scan
/// (2)    Limit
///          \
///          SecureFilter
///            \
///            Scan
///
/// Output:
/// (1)    Limit
///          \
///          Scan(padding limit)
/// (2)    Limit
///          \
///          SecureFilter
///            \
///            Scan(padding limit)
pub struct RulePushDownLimitScan {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RulePushDownLimitScan {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownLimitScan,
            matchers: vec![
                Matcher::MatchOp {
                    op_type: RelOp::Limit,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Scan,
                        children: vec![],
                    }],
                },
                Matcher::MatchOp {
                    op_type: RelOp::Limit,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::SecureFilter,
                        children: vec![Matcher::MatchOp {
                            op_type: RelOp::Scan,
                            children: vec![],
                        }],
                    }],
                },
            ],
        }
    }
}

impl Rule for RulePushDownLimitScan {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let limit: Limit = s_expr.plan().clone().try_into()?;
        let Some(mut count) = limit.limit else {
            return Ok(());
        };
        count += limit.offset;

        let child = s_expr.child(0)?;
        let (mut scan, secure_filter) = match child.plan() {
            RelOperator::Scan(_) => (child.plan().clone().try_into()?, None),
            RelOperator::SecureFilter(_) => {
                let secure_filter: SecureFilter = child.plan().clone().try_into()?;
                let scan: Scan = child.child(0)?.plan().clone().try_into()?;
                (scan, Some(secure_filter))
            }
            _ => unreachable!(),
        };

        // When SecureFilter exists, pushing limit is unsafe if push_down_predicates is empty.
        // Storage triggers early termination with (limit + no predicates), returning N rows
        // before SecureFilter can filter them, causing fewer results than expected.
        let has_predicates = scan
            .push_down_predicates
            .as_ref()
            .map_or(false, |preds| !preds.is_empty());

        if secure_filter.is_some() && !has_predicates {
            return Ok(());
        }

        scan.limit = Some(scan.limit.map_or(count, |c| cmp::max(c, count)));
        let scan_expr = SExpr::create_leaf(Arc::new(RelOperator::Scan(scan)));
        let child_expr = if let Some(secure_filter) = secure_filter {
            SExpr::create_unary(
                Arc::new(RelOperator::SecureFilter(secure_filter)),
                Arc::new(scan_expr),
            )
        } else {
            scan_expr
        };

        let mut result = s_expr.replace_children(vec![Arc::new(child_expr)]);
        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RulePushDownLimitScan {
    fn default() -> Self {
        Self::new()
    }
}
