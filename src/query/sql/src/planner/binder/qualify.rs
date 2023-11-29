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
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::Span;

use super::Finder;
use crate::binder::split_conjunctions;
use crate::binder::window::WindowRewriter;
use crate::binder::ColumnBindingBuilder;
use crate::binder::ExprContext;
use crate::binder::ScalarBinder;
use crate::binder::Visibility;
use crate::optimizer::SExpr;
use crate::planner::semantic::GroupingChecker;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::LambdaFunc;
use crate::plans::ScalarExpr;
use crate::plans::UDFLambdaCall;
use crate::plans::UDFServerCall;
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
        dbg!("analyze window qualify", qualify);
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
        let (mut scalar, _) = scalar_binder.bind(qualify).await?;
        let mut rewriter = WindowRewriter::new(bind_context, self.metadata.clone());
        rewriter.visit(&mut scalar)?;
        Ok(scalar)
    }

    #[async_backtrace::framed]
    pub async fn bind_qualify(
        &mut self,
        bind_context: &mut BindContext,
        qualify: ScalarExpr,
        span: Span,
        child: SExpr,
    ) -> Result<SExpr> {
        dbg!("bind qualify", &qualify);
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

        let scalar = if bind_context.in_grouping {
            // If we are in grouping context, we will perform the grouping check
            let grouping_checker = GroupingChecker::new(bind_context);
            grouping_checker.resolve(&qualify, span)?
        } else {
            let qualify_checker = QualifyChecker::new(bind_context);
            qualify_checker.resolve(&qualify)?
        };

        dbg!(&scalar);

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

    pub fn resolve(&self, scalar: &ScalarExpr) -> Result<ScalarExpr> {
        match scalar {
            ScalarExpr::BoundColumnRef(_)
            | ScalarExpr::ConstantExpr(_)
            | ScalarExpr::SubqueryExpr(_) => Ok(scalar.clone()),
            ScalarExpr::FunctionCall(func) => {
                let args = func
                    .arguments
                    .iter()
                    .map(|arg| self.resolve(arg))
                    .collect::<Result<Vec<ScalarExpr>>>()?;
                Ok(FunctionCall {
                    span: func.span,
                    params: func.params.clone(),
                    arguments: args,
                    func_name: func.func_name.clone(),
                }
                .into())
            }

            ScalarExpr::LambdaFunction(lambda_func) => {
                let args = lambda_func
                    .args
                    .iter()
                    .map(|arg| self.resolve(arg))
                    .collect::<Result<Vec<ScalarExpr>>>()?;
                Ok(LambdaFunc {
                    span: lambda_func.span,
                    func_name: lambda_func.func_name.clone(),
                    args,
                    lambda_expr: lambda_func.lambda_expr.clone(),
                    lambda_display: lambda_func.lambda_display.clone(),
                    return_type: lambda_func.return_type.clone(),
                }
                .into())
            }

            ScalarExpr::CastExpr(cast) => Ok(CastExpr {
                span: cast.span,
                is_try: cast.is_try,
                argument: Box::new(self.resolve(&cast.argument)?),
                target_type: cast.target_type.clone(),
            }
            .into()),

            ScalarExpr::WindowFunction(win) => {
                if let Some(column) = self
                    .bind_context
                    .windows
                    .window_functions_map
                    .get(&win.display_name)
                {
                    // The exprs in `win` has already been rewrittern to `BoundColumnRef` in `WindowRewriter`.
                    // So we need to check the exprs in `bind_context.windows`
                    let window_info = &self.bind_context.windows.window_functions[*column];

                    let column_binding = ColumnBindingBuilder::new(
                        win.display_name.clone(),
                        window_info.index,
                        Box::new(window_info.func.return_type()),
                        Visibility::Visible,
                    )
                    .build();
                    return Ok(BoundColumnRef {
                        span: None,
                        column: column_binding,
                    }
                    .into());
                }
                Err(ErrorCode::Internal(
                    "Qualify check: Invalid window function",
                ))
            }

            ScalarExpr::AggregateFunction(agg) => {
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
                    return Ok(BoundColumnRef {
                        span: None,
                        column: column_binding,
                    }
                    .into());
                }
                Err(ErrorCode::Internal("Invalid aggregate function"))
            }
            ScalarExpr::UDFServerCall(udf) => {
                let args = udf
                    .arguments
                    .iter()
                    .map(|arg| self.resolve(arg))
                    .collect::<Result<Vec<ScalarExpr>>>()?;
                Ok(UDFServerCall {
                    span: udf.span,
                    func_name: udf.func_name.clone(),
                    display_name: udf.display_name.clone(),
                    server_addr: udf.server_addr.clone(),
                    arg_types: udf.arg_types.clone(),
                    return_type: udf.return_type.clone(),
                    arguments: args,
                }
                .into())
            }
            ScalarExpr::UDFLambdaCall(udf) => {
                let scalar = &udf.scalar;
                let new_scalar = self.resolve(scalar)?;
                Ok(UDFLambdaCall {
                    span: udf.span,
                    func_name: udf.func_name.clone(),
                    scalar: Box::new(new_scalar),
                }
                .into())
            }
        }
    }
}
