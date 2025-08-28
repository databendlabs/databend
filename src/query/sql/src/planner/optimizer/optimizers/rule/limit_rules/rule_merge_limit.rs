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

        // Calculate the effective limit by taking the minimum
        // When we have Limit(A, Limit(B, input)), the result should be Limit(min(A, B), input)
        let effective_limit = if let (Some(outer_count), Some(inner_count)) =
            (outer_limit.limit, inner_limit.limit) {
            outer_count.min(inner_count)
        } else {
            // If either limit is None, use the other one
            outer_limit.limit.or(inner_limit.limit).unwrap_or(0)
        };

        // Calculate the effective offset
        // When merging Limit(limit=A, offset=X, Limit(limit=B, offset=Y, input)):
        // - The inner offset Y is applied first
        // - Then the outer offset X is applied
        // - The total offset is X + Y
        let effective_offset = outer_limit.offset + inner_limit.offset;

        // Create the merged limit operator
        let merged_limit = Limit {
            before_exchange: outer_limit.before_exchange || inner_limit.before_exchange,
            limit: Some(effective_limit),
            offset: effective_offset,
            lazy_columns: outer_limit.lazy_columns.union(&inner_limit.lazy_columns).cloned().collect(),
        };

        let merged_expr = SExpr::create_unary(
            Arc::new(merged_limit.into()),
            Arc::new(input.clone()),
        );

        state.add_result(merged_expr);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
