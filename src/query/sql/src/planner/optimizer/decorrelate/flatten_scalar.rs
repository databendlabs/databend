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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::binder::ColumnBindingBuilder;
use crate::optimizer::ColumnSet;
use crate::optimizer::SubqueryRewriter;
use crate::plans::AggregateFunction;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;
use crate::plans::UDFServerCall;

impl SubqueryRewriter {
    pub(crate) fn flatten_scalar(
        &mut self,
        scalar: &ScalarExpr,
        correlated_columns: &ColumnSet,
    ) -> Result<ScalarExpr> {
        match scalar {
            ScalarExpr::BoundColumnRef(bound_column) => {
                let column_binding = bound_column.column.clone();
                if correlated_columns.contains(&column_binding.index) {
                    let index = self.derived_columns.get(&column_binding.index).unwrap();
                    let metadata = self.metadata.read();
                    let column_entry = metadata.column(*index);
                    return Ok(ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: scalar.span(),
                        column: ColumnBindingBuilder::new(
                            column_entry.name(),
                            *index,
                            Box::new(column_entry.data_type()),
                            column_binding.visibility,
                        )
                        .build(),
                    }));
                }
                Ok(scalar.clone())
            }
            ScalarExpr::ConstantExpr(_) => Ok(scalar.clone()),
            ScalarExpr::AggregateFunction(agg) => {
                let mut args = Vec::with_capacity(agg.args.len());
                for arg in &agg.args {
                    args.push(self.flatten_scalar(arg, correlated_columns)?);
                }
                Ok(ScalarExpr::AggregateFunction(AggregateFunction {
                    display_name: agg.display_name.clone(),
                    func_name: agg.func_name.clone(),
                    distinct: agg.distinct,
                    params: agg.params.clone(),
                    args,
                    return_type: agg.return_type.clone(),
                }))
            }
            ScalarExpr::FunctionCall(func) => {
                let arguments = func
                    .arguments
                    .iter()
                    .map(|arg| self.flatten_scalar(arg, correlated_columns))
                    .collect::<Result<Vec<_>>>()?;
                Ok(ScalarExpr::FunctionCall(FunctionCall {
                    span: func.span,
                    func_name: func.func_name.clone(),
                    params: func.params.clone(),
                    arguments,
                }))
            }
            ScalarExpr::CastExpr(cast_expr) => {
                let scalar = self.flatten_scalar(&cast_expr.argument, correlated_columns)?;
                Ok(ScalarExpr::CastExpr(CastExpr {
                    span: cast_expr.span,
                    is_try: cast_expr.is_try,
                    argument: Box::new(scalar),
                    target_type: cast_expr.target_type.clone(),
                }))
            }
            ScalarExpr::UDFServerCall(udf) => {
                let arguments = udf
                    .arguments
                    .iter()
                    .map(|arg| self.flatten_scalar(arg, correlated_columns))
                    .collect::<Result<Vec<_>>>()?;
                Ok(ScalarExpr::UDFServerCall(UDFServerCall {
                    span: udf.span,
                    func_name: udf.func_name.clone(),
                    display_name: udf.display_name.clone(),
                    server_addr: udf.server_addr.clone(),
                    arg_types: udf.arg_types.clone(),
                    return_type: udf.return_type.clone(),
                    arguments,
                }))
            }
            _ => Err(ErrorCode::Internal(
                "Invalid scalar for flattening subquery",
            )),
        }
    }
}
