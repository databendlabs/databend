// Copyright 2022 Datafuse Labs.
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

use common_ast::ast::Expr;
use common_ast::ast::Identifier;
use common_ast::ast::Literal;
use common_ast::ast::WindowSpec;
use common_ast::Visitor;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::Span;
use common_functions::srfs::check_srf;
use common_functions::srfs::BUILTIN_SET_RETURNING_FUNCTIONS;

use crate::binder::ExprContext;
use crate::normalize_identifier;
use crate::optimizer::SExpr;
use crate::plans::ProjectSet;
use crate::plans::SrfItem;
use crate::BindContext;
use crate::Binder;
use crate::ColumnBinding;
use crate::ScalarBinder;
use crate::Visibility;

pub struct SrfCollector {
    srfs: Vec<Expr>,
}

impl<'a> Visitor<'a> for SrfCollector {
    fn visit_function_call(
        &mut self,
        span: Span,
        distinct: bool,
        name: &'a Identifier,
        args: &'a [Expr],
        params: &'a [Literal],
        over: &'a Option<WindowSpec>,
    ) {
        if BUILTIN_SET_RETURNING_FUNCTIONS.contains(&name.to_string().to_lowercase()) {
            // Collect the srf
            self.srfs.push(Expr::FunctionCall {
                span,
                distinct,
                name: name.clone(),
                args: args.to_vec(),
                params: params.to_vec(),
                window: over.clone(),
            });
        }
    }
}

impl SrfCollector {
    pub fn new() -> Self {
        SrfCollector { srfs: vec![] }
    }

    pub fn visit(&mut self, expr: &Expr) {
        self.visit_expr(expr);
    }

    pub fn into_srfs(self) -> Vec<Expr> {
        self.srfs
    }
}

impl Binder {
    pub async fn bind_project_set(
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
            let (name, args) = match srf {
                Expr::FunctionCall { name, args, .. } => {
                    let name = normalize_identifier(name, &self.name_resolution_ctx).to_string();

                    let original_context = bind_context.expr_context.clone();
                    bind_context.set_expr_context(ExprContext::InSetReturningFunction);

                    let mut arguments = Vec::with_capacity(args.len());
                    for arg in args.iter() {
                        let mut scalar_binder = ScalarBinder::new(
                            bind_context,
                            self.ctx.clone(),
                            &self.name_resolution_ctx,
                            self.metadata.clone(),
                            &[],
                        );
                        let (scalar, _) = scalar_binder.bind(arg).await?;
                        arguments.push(scalar);
                    }

                    // Restore the original context
                    bind_context.set_expr_context(original_context);

                    (name, arguments)
                }

                // Should have been checked by SrfCollector
                _ => unreachable!(),
            };

            // We checked it here only for the return type of srf
            let checked_args = args
                .iter()
                .map(|arg| arg.as_expr_with_col_index())
                .collect::<Result<Vec<_>>>()?;
            let srf_instance = check_srf(&name, &checked_args, &BUILTIN_SET_RETURNING_FUNCTIONS)?;
            let return_types = srf_instance.return_types;

            if return_types.len() > 1 {
                return Err(ErrorCode::Unimplemented(
                    "set-returning functions with more than one return type are not supported yet",
                ));
            }

            // Add result columns to metadata
            let columns = return_types
                .iter()
                .map(|return_type| {
                    let column = self
                        .metadata
                        .write()
                        .add_derived_column(name.clone(), return_type.clone());
                    ColumnBinding {
                        database_name: None,
                        table_name: None,
                        column_name: name.clone(),
                        index: column,
                        data_type: Box::new(return_type.clone()),
                        visibility: Visibility::Visible,
                    }
                })
                .collect::<Vec<_>>();

            let item = SrfItem {
                srf_name: name,
                args,
                columns: columns.iter().map(|column| column.index).collect(),
            };
            items.push(item);

            // Add the srf to bind context, so we can replace the srfs later.
            columns.into_iter().for_each(|column| {
                bind_context.srfs.insert(srf.to_string(), column);
            });
        }

        let project_set = ProjectSet { srfs: items };

        Ok(SExpr::create_unary(project_set.into(), s_expr))
    }
}
