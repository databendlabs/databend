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

use crate::binder::ColumnBindingBuilder;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::FunctionCall;
use crate::plans::UDFServerCall;
use crate::BindContext;
use crate::ScalarExpr;
use crate::Visibility;

pub struct WindowChecker<'a> {
    bind_context: &'a BindContext,
}

impl<'a> WindowChecker<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        Self { bind_context }
    }

    pub fn resolve(&self, scalar: &ScalarExpr) -> Result<ScalarExpr> {
        match scalar {
            ScalarExpr::BoundColumnRef(_) | ScalarExpr::ConstantExpr(_) => Ok(scalar.clone()),
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
            ScalarExpr::LambdaFunction(lambda) => {
                if let Some(column_ref) = self
                    .bind_context
                    .lambda_info
                    .lambda_functions_map
                    .get(&lambda.display_name)
                {
                    return Ok(column_ref.clone().into());
                }
                Err(ErrorCode::Internal("Window Check: Invalid lambda function"))
            }
            ScalarExpr::CastExpr(cast) => Ok(CastExpr {
                span: cast.span,
                is_try: cast.is_try,
                argument: Box::new(self.resolve(&cast.argument)?),
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
                Err(ErrorCode::Internal("Window Check: Invalid window function"))
            }

            ScalarExpr::AggregateFunction(_) => unreachable!(),
            ScalarExpr::UDFServerCall(udf) => {
                let new_args = udf
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
                    arguments: new_args,
                }
                .into())
            }
        }
    }
}
