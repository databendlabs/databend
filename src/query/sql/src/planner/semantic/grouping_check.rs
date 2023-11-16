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

use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::Span;

use crate::binder::ColumnBindingBuilder;
use crate::binder::Visibility;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::FunctionCall;
use crate::plans::LambdaFunc;
use crate::plans::ScalarExpr;
use crate::plans::UDFServerCall;
use crate::BindContext;

/// Check validity of scalar expression in a grouping context.
/// The matched grouping item will be replaced with a BoundColumnRef
/// to corresponding grouping item column.
///
/// Also replaced the matched window function with a BoundColumnRef.
pub struct GroupingChecker<'a> {
    bind_context: &'a BindContext,
}

impl<'a> GroupingChecker<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        Self { bind_context }
    }

    pub fn resolve(&self, scalar: &ScalarExpr, span: Span) -> Result<ScalarExpr> {
        if let Some(index) = self.bind_context.aggregate_info.group_items_map.get(scalar) {
            let column = &self.bind_context.aggregate_info.group_items[*index];
            let mut column_binding = if let ScalarExpr::BoundColumnRef(column_ref) = &column.scalar
            {
                column_ref.column.clone()
            } else {
                ColumnBindingBuilder::new(
                    "group_item".to_string(),
                    column.index,
                    Box::new(column.scalar.data_type()?),
                    Visibility::Visible,
                )
                .build()
            };

            if let Some(grouping_sets) = &self.bind_context.aggregate_info.grouping_sets {
                if grouping_sets.grouping_id_column.index != column_binding.index {
                    column_binding.data_type = Box::new(column_binding.data_type.wrap_nullable());
                }
            }

            return Ok(BoundColumnRef {
                span: scalar.span(),
                column: column_binding,
            }
            .into());
        }

        match scalar {
            ScalarExpr::BoundColumnRef(column) => {
                if self
                    .bind_context
                    .aggregate_info
                    .aggregate_functions_map
                    .contains_key(&column.column.column_name)
                {
                    // Be replaced by `WindowRewriter`.
                    return Ok(scalar.clone());
                }
                // If this is a group item, then it should have been replaced with `group_items_map`
                Err(ErrorCode::SemanticError(format!(
                    "column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function",
                    &column.column.column_name
                )).set_span(span))
            }
            ScalarExpr::ConstantExpr(_) => Ok(scalar.clone()),
            ScalarExpr::FunctionCall(func) => {
                let args = func
                    .arguments
                    .iter()
                    .map(|arg| self.resolve(arg, span))
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
                    .map(|arg| self.resolve(arg, span))
                    .collect::<Result<Vec<ScalarExpr>>>()?;
                Ok(LambdaFunc {
                    span: lambda_func.span,
                    func_name: lambda_func.func_name.clone(),
                    display_name: lambda_func.display_name.clone(),
                    args,
                    params: lambda_func.params.clone(),
                    lambda_expr: lambda_func.lambda_expr.clone(),
                    return_type: lambda_func.return_type.clone(),
                }
                .into())
            }
            ScalarExpr::CastExpr(cast) => Ok(CastExpr {
                span: cast.span,
                is_try: cast.is_try,
                argument: Box::new(self.resolve(&cast.argument, span)?),
                target_type: cast.target_type.clone(),
            }
            .into()),
            ScalarExpr::SubqueryExpr(_) => {
                // TODO(leiysky): check subquery in the future
                Ok(scalar.clone())
            }

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
                    // Just check if the exprs are in grouping items.
                    for part in window_info.partition_by_items.iter() {
                        self.resolve(&part.scalar, span)?;
                    }
                    // Just check if the exprs are in grouping items.
                    for order in window_info.order_by_items.iter() {
                        self.resolve(&order.order_by_item.scalar, span)?;
                    }
                    // Just check if the exprs are in grouping items.
                    for arg in window_info.arguments.iter() {
                        self.resolve(&arg.scalar, span)?;
                    }

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
                Err(ErrorCode::Internal("Group Check: Invalid window function"))
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
                    .map(|arg| self.resolve(arg, span))
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
        }
    }
}
