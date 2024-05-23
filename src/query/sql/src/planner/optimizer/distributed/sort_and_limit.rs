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

use crate::optimizer::extract::Matcher;
use crate::optimizer::SExpr;
use crate::plans::Exchange;
use crate::plans::Limit;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::Sort;

pub struct SortAndLimitPushDownOptimizer {
    sort_matcher: Matcher,
    limit_matcher: Matcher,
}

impl SortAndLimitPushDownOptimizer {
    pub fn create() -> Self {
        Self {
            sort_matcher: Self::sort_matcher(),
            limit_matcher: Self::limit_matcher(),
        }
    }

    /// `limit` is already pushed down to `Sort`,
    /// so the TopN scenario is already contained in this pattern.
    fn sort_matcher() -> Matcher {
        // Input:
        //   Sort
        //    \
        //     Exchange
        //      \
        //       *
        // Output:
        //   Sort (after_exchange = true)
        //    \
        //     Exchange
        //      \
        //       Sort (after_exchange = false)
        //        \
        //         *
        Matcher::MatchOp {
            op_type: RelOp::Sort,
            children: vec![Matcher::MatchOp {
                op_type: RelOp::Exchange,
                children: vec![Matcher::Leaf],
            }],
        }
    }

    fn limit_matcher() -> Matcher {
        // Input:
        // Limit
        //  \
        //   Exchange
        //    \
        //     *
        // Output:
        // Limit
        //  \
        //   Exchange
        //    \
        //     Limit
        //      \
        //       *
        Matcher::MatchOp {
            op_type: RelOp::Limit,
            children: vec![Matcher::MatchOp {
                op_type: RelOp::Exchange,
                children: vec![Matcher::Leaf],
            }],
        }
    }

    pub fn optimize(&self, s_expr: &SExpr) -> Result<SExpr> {
        let mut replaced_children = Vec::with_capacity(s_expr.arity());
        for child in s_expr.children.iter() {
            let new_child = self.optimize(child)?;
            replaced_children.push(Arc::new(new_child));
        }
        let new_sexpr = s_expr.replace_children(replaced_children);
        let apply_topn_res = self.apply_sort(&new_sexpr)?;
        self.apply_limit(&apply_topn_res)
    }

    fn apply_sort(&self, s_expr: &SExpr) -> Result<SExpr> {
        if !self.sort_matcher.matches(s_expr) {
            return Ok(s_expr.clone());
        }

        let mut sort: Sort = s_expr.plan().clone().try_into()?;
        sort.after_exchange = Some(false);
        let exchange_sexpr = s_expr.child(0)?;
        debug_assert!(matches!(
            exchange_sexpr.plan.as_ref(),
            RelOperator::Exchange(Exchange::Merge) | RelOperator::Exchange(Exchange::MergeSort)
        ));

        debug_assert!(exchange_sexpr.children.len() == 1);
        let exchange_sexpr = exchange_sexpr.replace_plan(Arc::new(Exchange::MergeSort.into()));

        let child = exchange_sexpr.child(0)?.clone();
        let before_exchange_sort =
            SExpr::create_unary(Arc::new(sort.clone().into()), Arc::new(child));
        let new_exchange = exchange_sexpr.replace_children(vec![Arc::new(before_exchange_sort)]);
        sort.after_exchange = Some(true);
        let new_plan = SExpr::create_unary(Arc::new(sort.into()), Arc::new(new_exchange));
        Ok(new_plan)
    }

    fn apply_limit(&self, s_expr: &SExpr) -> Result<SExpr> {
        if !self.limit_matcher.matches(s_expr) {
            return Ok(s_expr.clone());
        }

        let exchange_sexpr = s_expr.child(0)?;
        let mut limit: Limit = s_expr.plan().clone().try_into()?;

        if limit.limit.is_none() {
            if limit.offset != 0 {
                // Only offset: SELECT number from numbers(1000) offset 100;
                return Ok(s_expr.clone());
            }

            // Dummy limit: remove limit.
            return Ok(s_expr.child(0)?.clone());
        }

        limit.limit = limit.limit.map(|v| v + limit.offset);
        limit.offset = 0;
        limit.before_exchange = true;

        debug_assert!(exchange_sexpr.children.len() == 1);
        let child = exchange_sexpr.child(0)?.clone();
        let new_child = SExpr::create_unary(Arc::new(limit.into()), Arc::new(child));
        let new_exchange = exchange_sexpr.replace_children(vec![Arc::new(new_child)]);
        Ok(s_expr.replace_children(vec![Arc::new(new_exchange)]))
    }
}
