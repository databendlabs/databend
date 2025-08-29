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
use crate::plans::Limit;
use crate::plans::RelOp;

/// Rule to merge consecutive Limit operators.
///
/// This rule matches the pattern Limit(Limit(*)) and merges them into a single Limit
/// by taking the more restrictive limit (minimum of the two).
///
/// Example:
/// Limit(5)
///   Limit(10)
///     TableScan
///
/// Becomes:
/// Limit(5)
///   TableScan
///
/// Another example:
/// Limit(15)
///   Limit(10)
///     TableScan
///
/// Becomes:
/// Limit(10)
///   TableScan
pub struct RuleMergeLimit {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RuleMergeLimit {
    pub fn new() -> Self {
        Self {
            id: RuleID::MergeLimit,
            // Limit
            //  \
            //   Limit
            //    \
            //     *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Limit,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Limit,
                    children: vec![Matcher::Leaf],
                }],
            }],
        }
    }
}

impl Rule for RuleMergeLimit {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let outer_limit: Limit = s_expr.plan().clone().try_into()?;
        let inner_limit_expr = s_expr.child(0)?;
        let inner_limit: Limit = inner_limit_expr.plan().clone().try_into()?;
        let input = inner_limit_expr.child(0)?;

        // Merge offset: the total offset is the sum of inner and outer offsets.
        let merged_offset = inner_limit.offset + outer_limit.offset;

        // Merge limit:
        // - If both inner and outer have limits, apply inner limit first, then outer offset, then outer limit.
        // - If only inner has limit, apply outer offset.
        // - If only outer has limit, use it.
        // - If neither has limit, result is unlimited.
        let merged_limit = match (inner_limit.limit, outer_limit.limit) {
            (Some(inner), Some(outer)) => {
                let after_offset = inner.saturating_sub(outer_limit.offset);
                Some(after_offset.min(outer))
            }
            (Some(inner), None) => Some(inner.saturating_sub(outer_limit.offset)),
            (None, Some(outer)) => Some(outer),
            (None, None) => None,
        };

        // Create the merged limit operator
        let merged_limit_op = Limit {
            before_exchange: outer_limit.before_exchange || inner_limit.before_exchange,
            limit: merged_limit,
            offset: merged_offset,
            lazy_columns: outer_limit
                .lazy_columns
                .union(&inner_limit.lazy_columns)
                .cloned()
                .collect(),
        };

        let merged_expr =
            SExpr::create_unary(Arc::new(merged_limit_op.into()), Arc::new(input.clone()));

        state.add_result(merged_expr);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RuleMergeLimit {
    fn default() -> Self {
        Self::new()
    }
}
