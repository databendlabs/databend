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

use common_exception::Result;

use crate::optimizer::SExpr;
use crate::plans::Limit;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::Sort;

pub(super) struct TopNPushDownOptimizer {
    topn_pattern: SExpr,
    limit_pattern: SExpr,
}

impl TopNPushDownOptimizer {
    pub fn create() -> Self {
        Self {
            topn_pattern: Self::topn_pattern(),
            limit_pattern: Self::limit_pattern(),
        }
    }

    fn topn_pattern() -> SExpr {
        // Input:
        // Limit
        //  \
        //   Sort
        //    \
        //     Exchange
        //      \
        //       *
        // Output:
        // Limit
        //  \
        //   Sort (after_exchange = true)
        //    \
        //     Exchange
        //      \
        //       Sort (after_exchange = false)
        //        \
        //         *
        SExpr::create_unary(
            Arc::new(
                PatternPlan {
                    plan_type: RelOp::Limit,
                }
                .into(),
            ),
            Arc::new(SExpr::create_unary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Sort,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Exchange,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ))),
                )),
            )),
        )
    }

    fn limit_pattern() -> SExpr {
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
        SExpr::create_unary(
            Arc::new(
                PatternPlan {
                    plan_type: RelOp::Limit,
                }
                .into(),
            ),
            Arc::new(SExpr::create_unary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Exchange,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_leaf(Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Pattern,
                    }
                    .into(),
                ))),
            )),
        )
    }

    pub fn optimize(&self, s_expr: &SExpr) -> Result<SExpr> {
        let mut replaced_children = Vec::with_capacity(s_expr.arity());
        for child in s_expr.children.iter() {
            let new_child = self.optimize(child)?;
            replaced_children.push(Arc::new(new_child));
        }
        let new_sexpr = s_expr.replace_children(replaced_children);
        let apply_topn_res = self.apply_topn(&new_sexpr)?;
        self.apply_limit(&apply_topn_res)
    }

    fn apply_topn(&self, s_expr: &SExpr) -> Result<SExpr> {
        if !s_expr.match_pattern(&self.topn_pattern) {
            return Ok(s_expr.clone());
        }

        let sort_sexpr = s_expr.child(0)?;
        let exchange_sexpr = sort_sexpr.child(0)?;

        let mut sort: Sort = sort_sexpr.plan().clone().try_into()?;

        if sort.limit.is_none() {
            // It could be a ORDER BY ... OFFSET ... clause. (No LIMIT)
            return Ok(s_expr.clone());
        }

        debug_assert!(exchange_sexpr.children.len() == 1);

        let child = exchange_sexpr.child(0)?.clone();
        let before_exchange_sort =
            SExpr::create_unary(Arc::new(sort.clone().into()), Arc::new(child));
        let new_exchange = exchange_sexpr.replace_children(vec![Arc::new(before_exchange_sort)]);
        sort.after_exchange = true;
        let new_sort = SExpr::create_unary(Arc::new(sort.into()), Arc::new(new_exchange));
        let new_plan = s_expr.replace_children(vec![Arc::new(new_sort)]);
        Ok(new_plan)
    }

    fn apply_limit(&self, s_expr: &SExpr) -> Result<SExpr> {
        if !s_expr.match_pattern(&self.limit_pattern) {
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
