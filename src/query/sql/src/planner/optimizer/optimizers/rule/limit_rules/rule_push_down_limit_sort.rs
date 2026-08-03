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

use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Limit;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::Sort;
use crate::plans::TopN;

/// Input:  Limit
///           \
///          Sort
///             \
///              *
///
/// Output: TopN (fused Limit + Sort, when `enable_top_n` is set,
///         `limit + offset <= max_limit`, and the sort is a plain query sort)
///           \
///            *
///
/// When fusion is disabled or the sort carries window, projection, or exchange
/// state, fall back to padding `Sort.limit` while keeping the `Limit`:
///
///         Limit
///           \
///          Sort(padding limit)
///             \
///               *
pub struct RulePushDownLimitSort {
    id: RuleID,
    matchers: Vec<Matcher>,
    max_limit: usize,
    enable_top_n: bool,
}

impl RulePushDownLimitSort {
    pub fn new(max_limit: usize, enable_top_n: bool) -> Self {
        Self {
            id: RuleID::PushDownLimitSort,
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Limit,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Sort,
                    children: vec![Matcher::Leaf],
                }],
            }],
            max_limit,
            enable_top_n,
        }
    }
}

impl Rule for RulePushDownLimitSort {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut TransformResult,
    ) -> databend_common_exception::Result<()> {
        let limit: Limit = s_expr.plan().clone().try_into()?;
        let Some(limit_rows) = limit.limit else {
            return Ok(());
        };
        let count = limit_rows.saturating_add(limit.offset);
        if count > self.max_limit {
            return Ok(());
        }

        let sort = s_expr.child(0)?;
        let mut sort_limit: Sort = sort.plan().clone().try_into()?;

        // Fuse `Limit + Sort` into a `TopN` operator. The partial stage keeps
        // `limit + offset` candidates and the final stage applies the offset.
        if self.enable_top_n
            && sort_limit.window_partition.is_none()
            && sort_limit.pre_projection.is_none()
            && sort_limit.after_exchange.is_none()
            && limit_rows > 0
            && !limit.before_exchange
        {
            let top_n = TopN {
                items: sort_limit.items.clone(),
                limit: limit_rows,
                offset: limit.offset,
                lazy_columns: limit.lazy_columns.clone(),
                after_exchange: None,
            };

            let mut result = SExpr::create_unary(
                Arc::new(RelOperator::TopN(top_n)),
                Arc::new(sort.child(0)?.clone()),
            );
            result.set_applied_rule(&self.id);
            state.add_result(result);
            return Ok(());
        }

        // Fallback: pad `Sort.limit` as a candidate capacity marker.
        let limit = sort_limit.limit.map_or(count, |c| cmp::max(c, count));
        sort_limit.limit = Some(limit);
        let sort = SExpr::create_unary(
            Arc::new(RelOperator::Sort(sort_limit)),
            Arc::new(sort.child(0)?.clone()),
        );

        let mut result = s_expr.replace_children(vec![Arc::new(sort)]);
        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
