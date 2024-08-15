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
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::Window;
use databend_common_exception::Result;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::FunctionKind;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use derive_visitor::Drive;
use derive_visitor::Visitor;

use crate::binder::ColumnBindingBuilder;
use crate::binder::ExprContext;
use crate::optimizer::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::FunctionCall as ScalarExprFunctionCall;
use crate::plans::ProjectSet;
use crate::plans::SrfItem;
use crate::BindContext;
use crate::Binder;
use crate::ScalarBinder;
use crate::ScalarExpr;
use crate::Visibility;

#[derive(Visitor)]
#[visitor(FunctionCall(enter), Window)]
pub struct SrfCollector {
    srfs: Vec<Expr>,
    in_window: bool,
}

impl SrfCollector {
    fn enter_function_call(&mut self, func: &FunctionCall) {
        // TODO(andylokandy/leisky): SRF in window function is not supported yet.
        // This is a workaround to skip SRF in window function.
        if self.in_window {
            return;
        }

        let FunctionCall {
            distinct,
            name,
            args,
            params,
            window,
            lambda,
        } = func;

        if BUILTIN_FUNCTIONS
            .get_property(&name.name)
            .map(|property| property.kind == FunctionKind::SRF)
            .unwrap_or(false)
        {
            // Collect the srf
            self.srfs.push(Expr::FunctionCall {
                span: name.span,
                func: FunctionCall {
                    distinct: *distinct,
                    name: name.clone(),
                    args: args.to_vec(),
                    params: params.to_vec(),
                    window: window.clone(),
                    lambda: lambda.clone(),
                },
            });
        }
    }

    fn enter_window(&mut self, _window: &Window) {
        self.in_window = true;
    }

    fn exit_window(&mut self, _window: &Window) {
        self.in_window = false;
    }
}

impl SrfCollector {
    pub fn new() -> Self {
        SrfCollector {
            srfs: vec![],
            in_window: false,
        }
    }

    pub fn visit(&mut self, expr: &Expr) {
        expr.drive(self);
    }

    pub fn into_srfs(self) -> Vec<Expr> {
        self.srfs
    }
}

impl Binder {
    pub fn bind_project_set(
        &mut self,
        bind_context: &mut BindContext,
        srfs: &[Expr],
        s_expr: SExpr,
    ) -> Result<SExpr> {
        if srfs.is_empty() {
            return Ok(s_expr);
        }

        let mut items = Vec::with_capacity(srfs.len());
        for srf in srfs {
            let (name, srf_scalar) = match srf {
                Expr::FunctionCall {
                    func: FunctionCall { name, args, .. },
                    ..
                } => {
                    let name = name.normalized_name();

                    let original_context = bind_context.expr_context.clone();
                    bind_context.set_expr_context(ExprContext::InSetReturningFunction);

                    let mut arguments = Vec::with_capacity(args.len());
                    for arg in args.iter() {
                        let mut scalar_binder = ScalarBinder::new(
                            bind_context,
                            self.ctx.clone(),
                            self.metadata.clone(),
                            &[],
                            self.m_cte_bound_ctx.clone(),
                            self.ctes_map.clone(),
                        );
                        let (scalar, _) = scalar_binder.bind(arg)?;
                        arguments.push(scalar);
                    }

                    // Restore the original context
                    bind_context.set_expr_context(original_context);

                    let scalar = ScalarExpr::FunctionCall(ScalarExprFunctionCall {
                        span: srf.span(),
                        func_name: name.clone(),
                        params: vec![],
                        arguments,
                    });

                    (name, scalar)
                }

                // Should have been checked by SrfCollector
                _ => unreachable!(),
            };

            let srf_expr = srf_scalar.as_expr()?;
            let return_types = srf_expr.data_type().as_tuple().unwrap();

            // Add result column to metadata
            let column_index = self.metadata.write().add_derived_column(
                name.clone(),
                srf_expr.data_type().clone(),
                Some(srf_scalar.clone()),
            );
            let column = ColumnBindingBuilder::new(
                name.clone(),
                column_index,
                Box::new(srf_expr.data_type().clone()),
                Visibility::InVisible,
            )
            .build();

            let item = SrfItem {
                scalar: srf_scalar,
                index: column_index,
            };
            items.push(item);

            // If tuple has more than one field, return the tuple column,
            // otherwise, extract the tuple field to top level column.
            let result_column = if return_types.len() > 1 {
                ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: srf.span(),
                    column,
                })
            } else {
                ScalarExpr::FunctionCall(ScalarExprFunctionCall {
                    span: srf.span(),
                    func_name: "get".to_string(),
                    params: vec![Scalar::Number(NumberScalar::Int64(1))],
                    arguments: vec![ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: srf.span(),
                        column,
                    })],
                })
            };

            // Add the srf to bind context, so we can replace the srfs later.
            bind_context.srfs.insert(srf.to_string(), result_column);
        }

        let project_set = ProjectSet { srfs: items };

        Ok(SExpr::create_unary(
            Arc::new(project_set.into()),
            Arc::new(s_expr),
        ))
    }
}
