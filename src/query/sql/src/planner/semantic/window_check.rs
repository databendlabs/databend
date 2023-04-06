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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::plans::AndExpr;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ComparisonExpr;
use crate::plans::FunctionCall;
use crate::plans::NotExpr;
use crate::plans::OrExpr;
use crate::BindContext;
use crate::ColumnBinding;
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
            ScalarExpr::BoundColumnRef(_)
            | ScalarExpr::BoundInternalColumnRef(_)
            | ScalarExpr::ConstantExpr(_) => Ok(scalar.clone()),
            ScalarExpr::AndExpr(scalar) => Ok(AndExpr {
                left: Box::new(self.resolve(&scalar.left)?),
                right: Box::new(self.resolve(&scalar.right)?),
            }
            .into()),
            ScalarExpr::OrExpr(scalar) => Ok(OrExpr {
                left: Box::new(self.resolve(&scalar.left)?),
                right: Box::new(self.resolve(&scalar.right)?),
            }
            .into()),
            ScalarExpr::NotExpr(scalar) => Ok(NotExpr {
                argument: Box::new(self.resolve(&scalar.argument)?),
            }
            .into()),
            ScalarExpr::ComparisonExpr(scalar) => Ok(ComparisonExpr {
                op: scalar.op.clone(),
                left: Box::new(self.resolve(&scalar.left)?),
                right: Box::new(self.resolve(&scalar.right)?),
            }
            .into()),
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
                    let column_binding = ColumnBinding {
                        database_name: None,
                        table_name: None,
                        table_index: None,
                        column_name: win.display_name.clone(),
                        index: window_info.index,
                        data_type: Box::new(window_info.func.return_type()),
                        visibility: Visibility::Visible,
                    };
                    return Ok(BoundColumnRef {
                        span: None,
                        column: column_binding,
                    }
                    .into());
                }
                Err(ErrorCode::Internal("Invalid window function"))
            }

            ScalarExpr::AggregateFunction(_) => unreachable!(),
        }
    }
}
