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

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::Result;

use super::select::SelectList;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::SExpr;
use crate::plans::AggregateFunction;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::EvalScalar;
use crate::plans::FunctionCall;
use crate::plans::Lambda;
use crate::plans::LambdaFunc;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::WindowFunc;
use crate::plans::WindowOrderBy;
use crate::BindContext;
use crate::Binder;
use crate::MetadataRef;
use crate::Visibility;

#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct LambdaInfo {
    /// Arguments of lambda functions
    pub lambda_arguments: Vec<ScalarItem>,
    /// Lambda functions
    pub lambda_functions: Vec<ScalarItem>,
    /// Mapping: (lambda function display name) -> (derived column ref)
    /// This is used to generate column in projection.
    pub lambda_functions_map: HashMap<String, BoundColumnRef>,
}

pub(super) struct LambdaRewriter<'a> {
    pub bind_context: &'a mut BindContext,
    pub metadata: MetadataRef,
}

impl<'a> LambdaRewriter<'a> {
    pub fn new(bind_context: &'a mut BindContext, metadata: MetadataRef) -> Self {
        Self {
            bind_context,
            metadata,
        }
    }

    pub fn visit(&mut self, scalar: &ScalarExpr) -> Result<ScalarExpr> {
        match scalar {
            ScalarExpr::BoundColumnRef(_) => Ok(scalar.clone()),
            ScalarExpr::ConstantExpr(_) => Ok(scalar.clone()),
            ScalarExpr::FunctionCall(func) => {
                let new_args = func
                    .arguments
                    .iter()
                    .map(|arg| self.visit(arg))
                    .collect::<Result<Vec<_>>>()?;
                Ok(FunctionCall {
                    span: func.span,
                    func_name: func.func_name.clone(),
                    params: func.params.clone(),
                    arguments: new_args,
                }
                .into())
            }
            ScalarExpr::CastExpr(cast) => Ok(CastExpr {
                span: cast.span,
                is_try: cast.is_try,
                argument: Box::new(self.visit(&cast.argument)?),
                target_type: cast.target_type.clone(),
            }
            .into()),

            // TODO(leiysky): should we recursively process subquery here?
            ScalarExpr::SubqueryExpr(_) => Ok(scalar.clone()),

            ScalarExpr::AggregateFunction(agg_func) => {
                let new_args = agg_func
                    .args
                    .iter()
                    .map(|arg| self.visit(arg))
                    .collect::<Result<Vec<_>>>()?;
                Ok(AggregateFunction {
                    func_name: agg_func.func_name.clone(),
                    distinct: agg_func.distinct,
                    params: agg_func.params.clone(),
                    args: new_args,
                    return_type: agg_func.return_type.clone(),
                    display_name: agg_func.display_name.clone(),
                }
                .into())
            }

            ScalarExpr::WindowFunction(window) => {
                let new_partition_by = window
                    .partition_by
                    .iter()
                    .map(|partition_by| self.visit(partition_by))
                    .collect::<Result<Vec<_>>>()?;

                let mut new_order_by = Vec::with_capacity(window.order_by.len());
                for order_by in window.order_by.iter() {
                    new_order_by.push(WindowOrderBy {
                        expr: self.visit(&order_by.expr)?,
                        asc: order_by.asc,
                        nulls_first: order_by.nulls_first,
                    });
                }

                Ok(WindowFunc {
                    span: window.span,
                    display_name: window.display_name.clone(),
                    partition_by: new_partition_by,
                    func: window.func.clone(),
                    order_by: new_order_by,
                    frame: window.frame.clone(),
                }
                .into())
            }

            ScalarExpr::LambdaFunction(lambda_func) => {
                let mut replaced_args = Vec::with_capacity(lambda_func.args.len());
                for (i, arg) in lambda_func.args.iter().enumerate() {
                    let new_arg = self.visit(arg)?;
                    if let ScalarExpr::LambdaFunction(_) = new_arg {
                        replaced_args.push(new_arg);
                        continue;
                    }

                    let replaced_arg = if let ScalarExpr::BoundColumnRef(ref column_ref) = new_arg {
                        column_ref.clone()
                    } else {
                        let name = format!("{}_arg_{}", &lambda_func.display_name, i);
                        let index = self
                            .metadata
                            .write()
                            .add_derived_column(name.clone(), new_arg.data_type()?);

                        // Generate a ColumnBinding for each argument of lambda function
                        let column = ColumnBindingBuilder::new(
                            name,
                            index,
                            Box::new(new_arg.data_type()?),
                            Visibility::Visible,
                        )
                        .build();

                        BoundColumnRef {
                            span: new_arg.span(),
                            column,
                        }
                    };

                    self.bind_context
                        .lambda_info
                        .lambda_arguments
                        .push(ScalarItem {
                            index: replaced_arg.column.index,
                            scalar: new_arg,
                        });
                    replaced_args.push(replaced_arg.into());
                }

                let index = self
                    .metadata
                    .write()
                    .add_derived_column(lambda_func.display_name.clone(), scalar.data_type()?);

                let column = ColumnBindingBuilder::new(
                    lambda_func.display_name.clone(),
                    index,
                    Box::new(scalar.data_type()?),
                    Visibility::Visible,
                )
                .build();

                let replaced_column = BoundColumnRef {
                    span: scalar.span(),
                    column,
                };

                let replaced_lambda = LambdaFunc {
                    span: lambda_func.span,
                    func_name: lambda_func.func_name.clone(),
                    display_name: lambda_func.display_name.clone(),
                    args: replaced_args,
                    params: lambda_func.params.clone(),
                    lambda_expr: lambda_func.lambda_expr.clone(),
                    return_type: lambda_func.return_type.clone(),
                };

                self.bind_context
                    .lambda_info
                    .lambda_functions_map
                    .insert(lambda_func.display_name.clone(), replaced_column);
                self.bind_context
                    .lambda_info
                    .lambda_functions
                    .push(ScalarItem {
                        index,
                        scalar: replaced_lambda.clone().into(),
                    });

                Ok(replaced_lambda.into())
            }
        }
    }
}

impl Binder {
    /// Analyze lambda functions in select clause, this will rewrite lambda functions.
    /// See [`LambdaRewriter`] for more details.
    pub(crate) fn analyze_lambda(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
    ) -> Result<()> {
        for item in select_list.items.iter_mut() {
            let mut rewriter = LambdaRewriter::new(bind_context, self.metadata.clone());
            let new_scalar = rewriter.visit(&item.scalar)?;
            item.scalar = new_scalar;
        }

        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn bind_lambda(
        &mut self,
        bind_context: &mut BindContext,
        child: SExpr,
    ) -> Result<SExpr> {
        let lambda_info = &bind_context.lambda_info;
        if lambda_info.lambda_functions.is_empty() {
            return Ok(child);
        }

        let mut new_expr = child;
        if !lambda_info.lambda_arguments.is_empty() {
            let mut scalar_items = lambda_info.lambda_arguments.clone();
            scalar_items.sort_by_key(|item| item.index);
            let eval_scalar = EvalScalar {
                items: scalar_items,
            };
            new_expr = SExpr::create_unary(Arc::new(eval_scalar.into()), Arc::new(new_expr));
        }

        let lambda_plan = Lambda {
            items: lambda_info.lambda_functions.clone(),
        };
        new_expr = SExpr::create_unary(Arc::new(lambda_plan.into()), Arc::new(new_expr));

        Ok(new_expr)
    }
}
