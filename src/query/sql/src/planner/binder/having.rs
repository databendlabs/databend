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

use common_ast::ast::Expr;
use common_exception::Result;
use common_exception::Span;

use crate::binder::aggregate::AggregateRewriter;
use crate::binder::split_conjunctions;
use crate::binder::ExprContext;
use crate::binder::ScalarBinder;
use crate::optimizer::SExpr;
use crate::planner::semantic::GroupingChecker;
use crate::plans::Filter;
use crate::plans::ScalarExpr;
use crate::BindContext;
use crate::Binder;

impl Binder {
    /// Analyze aggregates in having clause, this will rewrite aggregate functions.
    /// See `AggregateRewriter` for more details.
    #[async_backtrace::framed]
    pub async fn analyze_aggregate_having<'a>(
        &mut self,
        bind_context: &mut BindContext,
        aliases: &[(String, ScalarExpr)],
        having: &Expr,
    ) -> Result<(ScalarExpr, Span)> {
        bind_context.set_expr_context(ExprContext::HavingClause);
        let mut scalar_binder = ScalarBinder::new(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            aliases,
            self.m_cte_bound_ctx.clone(),
        );
        let (scalar, _) = scalar_binder.bind(having).await?;
        let mut rewriter = AggregateRewriter::new(bind_context, self.metadata.clone());
        Ok((rewriter.visit(&scalar)?, having.span()))
    }

    #[async_backtrace::framed]
    pub async fn bind_having(
        &mut self,
        bind_context: &mut BindContext,
        having: ScalarExpr,
        span: Span,
        child: SExpr,
    ) -> Result<SExpr> {
        bind_context.set_expr_context(ExprContext::HavingClause);

        let scalar = if bind_context.in_grouping {
            // If we are in grouping context, we will perform the grouping check
            let grouping_checker = GroupingChecker::new(bind_context);
            grouping_checker.resolve(&having, span)?
        } else {
            // Otherwise we just fallback to a normal selection as `WHERE` clause.
            // This follows behavior of MySQL and Snowflake.
            having
        };

        let predicates = split_conjunctions(&scalar);

        let filter = Filter {
            predicates,
            is_having: true,
        };

        Ok(SExpr::create_unary(
            Arc::new(filter.into()),
            Arc::new(child),
        ))
    }
}
