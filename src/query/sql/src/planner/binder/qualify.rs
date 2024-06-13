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

use databend_common_ast::ast::Expr;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::Finder;
use crate::binder::split_conjunctions;
use crate::binder::window::WindowRewriter;
use crate::binder::ColumnBindingBuilder;
use crate::binder::ExprContext;
use crate::binder::ScalarBinder;
use crate::binder::Visibility;
use crate::optimizer::SExpr;
use crate::planner::semantic::GroupingChecker;
use crate::plans::walk_expr_mut;
use crate::plans::BoundColumnRef;
use crate::plans::Filter;
use crate::plans::ScalarExpr;
use crate::plans::SubqueryExpr;
use crate::plans::Visitor;
use crate::plans::VisitorMut;
use crate::BindContext;
use crate::Binder;

impl Binder {
    /// Analyze window in qualify clause, this will rewrite window functions.
    /// See `WindowRewriter` for more details.
    #[async_backtrace::framed]
    pub async fn analyze_window_qualify<'a>(
        &mut self,
        bind_context: &mut BindContext,
        aliases: &[(String, ScalarExpr)],
        qualify: &Expr,
    ) -> Result<ScalarExpr> {
        bind_context.set_expr_context(ExprContext::QualifyClause);
        let mut scalar_binder = ScalarBinder::new(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            aliases,
            self.m_cte_bound_ctx.clone(),
            self.ctes_map.clone(),
        );
        let (mut scalar, _) = scalar_binder.bind(qualify)?;
        let mut rewriter = WindowRewriter::new(bind_context, self.metadata.clone());
        rewriter.visit(&mut scalar)?;
        Ok(scalar)
    }

    #[async_backtrace::framed]
    pub async fn bind_qualify(
        &mut self,
        bind_context: &mut BindContext,
        qualify: ScalarExpr,
        child: SExpr,
    ) -> Result<SExpr> {
        bind_context.set_expr_context(ExprContext::QualifyClause);

        let f = |scalar: &ScalarExpr| matches!(scalar, ScalarExpr::AggregateFunction(_));
        let mut finder = Finder::new(&f);
        finder.visit(&qualify)?;
        if !finder.scalars().is_empty() {
            return Err(ErrorCode::SemanticError(
                "Qualify clause must not contain aggregate functions".to_string(),
            )
            .set_span(qualify.span()));
        }

        let scalar = {
            let mut qualify = qualify;
            if bind_context.in_grouping {
                // If we are in grouping context, we will perform the grouping check
                let mut grouping_checker = GroupingChecker::new(bind_context);
                grouping_checker.visit(&mut qualify)?;
            } else {
                let mut qualify_checker = QualifyChecker::new(bind_context);
                qualify_checker.visit(&mut qualify)?;
            }
            qualify
        };

        let predicates = split_conjunctions(&scalar);

        let filter = Filter { predicates };

        Ok(SExpr::create_unary(
            Arc::new(filter.into()),
            Arc::new(child),
        ))
    }
}

pub struct QualifyChecker<'a> {
    bind_context: &'a BindContext,
}

impl<'a> QualifyChecker<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        Self { bind_context }
    }
}

impl<'a> VisitorMut<'_> for QualifyChecker<'a> {
    fn visit(&mut self, expr: &mut ScalarExpr) -> Result<()> {
        if let ScalarExpr::WindowFunction(window) = expr {
            if let Some(column) = self
                .bind_context
                .windows
                .window_functions_map
                .get(&window.display_name)
            {
                // The exprs in `win` has already been rewrittern to `BoundColumnRef` in `WindowRewriter`.
                // So we need to check the exprs in `bind_context.windows`
                let window_info = &self.bind_context.windows.window_functions[*column];

                let column_binding = ColumnBindingBuilder::new(
                    window.display_name.clone(),
                    window_info.index,
                    Box::new(window_info.func.return_type()),
                    Visibility::Visible,
                )
                .build();
                *expr = BoundColumnRef {
                    span: None,
                    column: column_binding,
                }
                .into();
                return Ok(());
            }
            return Err(ErrorCode::Internal(
                "Qualify check: Invalid window function",
            ));
        }

        if let ScalarExpr::AggregateFunction(agg) = expr {
            if let Some(column) = self
                .bind_context
                .aggregate_info
                .aggregate_functions_map
                .get(&agg.display_name)
            {
                let agg_func = &self.bind_context.aggregate_info.aggregate_functions[*column];
                let column_binding = ColumnBindingBuilder::new(
                    agg.display_name.clone(),
                    agg_func.index,
                    Box::new(agg_func.scalar.data_type()?),
                    Visibility::Visible,
                )
                .build();
                *expr = BoundColumnRef {
                    span: None,
                    column: column_binding,
                }
                .into();
                return Ok(());
            }

            return Err(ErrorCode::Internal("Invalid aggregate function"));
        }

        walk_expr_mut(self, expr)
    }

    fn visit_subquery_expr(&mut self, _: &mut SubqueryExpr) -> Result<()> {
        // TODO(leiysky): check subquery in the future
        Ok(())
    }
}
