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

use crate::BindContext;
use crate::Binder;
use crate::binder::ExprContext;
use crate::binder::ScalarBinder;
use crate::binder::split_conjunctions;
use crate::binder::window::WindowRewriter;
use crate::binder::window::find_replaced_window_function;
use crate::optimizer::ir::SExpr;
use crate::planner::semantic::GroupingChecker;
use crate::plans::BoundColumnRef;
use crate::plans::Filter;
use crate::plans::ScalarExpr;
use crate::plans::SubqueryExpr;
use crate::plans::VisitorMut;
use crate::plans::walk_expr_mut;

impl Binder {
    /// Analyze window in qualify clause, this will rewrite window functions.
    /// See `WindowRewriter` for more details.
    pub fn analyze_window_qualify(
        &mut self,
        bind_context: &mut BindContext,
        aliases: &[(String, ScalarExpr)],
        qualify: &Expr,
        needs_window_rewrite: bool,
    ) -> Result<ScalarExpr> {
        bind_context.expr_context = ExprContext::QualifyClause;
        let mut scalar_binder = ScalarBinder::new(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            aliases,
        );
        let (mut scalar, _) = scalar_binder.bind(qualify)?;
        if needs_window_rewrite {
            let mut rewriter = WindowRewriter::new(bind_context, self.metadata.clone());
            rewriter.visit(&mut scalar)?;
        }
        Ok(scalar)
    }

    pub fn bind_qualify(
        &mut self,
        bind_context: &mut BindContext,
        qualify: ScalarExpr,
        child: SExpr,
    ) -> Result<SExpr> {
        bind_context.expr_context = ExprContext::QualifyClause;

        let scalar = {
            let mut qualify = qualify;
            if bind_context.in_grouping {
                // If we are in grouping context, we will perform the grouping check
                let mut grouping_checker = GroupingChecker::new(
                    bind_context,
                    Some("Qualify clause must not contain aggregate functions"),
                );
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

impl VisitorMut<'_> for QualifyChecker<'_> {
    fn visit(&mut self, expr: &mut ScalarExpr) -> Result<()> {
        if let ScalarExpr::WindowFunction(window) = expr {
            if let Some(column_binding) = find_replaced_window_function(
                &self.bind_context.windows,
                window,
                &window.display_name,
            ) {
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
            return Err(ErrorCode::SemanticError(
                "Qualify clause must not contain aggregate functions".to_string(),
            )
            .set_span(agg.span));
        }

        if let ScalarExpr::UDAFCall(udaf) = expr {
            return Err(ErrorCode::SemanticError(
                "Qualify clause must not contain aggregate functions".to_string(),
            )
            .set_span(udaf.span));
        }

        walk_expr_mut(self, expr)
    }

    fn visit_bound_column_ref(&mut self, column: &mut BoundColumnRef) -> Result<()> {
        if self
            .bind_context
            .aggregate_info
            .has_aggregate_call_index(column.column.index)
        {
            return Err(ErrorCode::SemanticError(
                "Qualify clause must not contain aggregate functions".to_string(),
            )
            .set_span(column.span));
        }

        Ok(())
    }

    fn visit_subquery_expr(&mut self, _: &mut SubqueryExpr) -> Result<()> {
        // TODO(leiysky): check subquery in the future
        Ok(())
    }
}
