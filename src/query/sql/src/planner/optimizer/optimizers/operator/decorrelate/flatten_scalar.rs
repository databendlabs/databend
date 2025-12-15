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

use crate::ColumnSet;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::optimizers::operator::SubqueryDecorrelatorOptimizer;
use crate::plans::AggregateFunction;
use crate::plans::AggregateFunctionScalarSortDesc;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;
use crate::plans::UDFCall;

impl SubqueryDecorrelatorOptimizer {
    #[recursive::recursive]
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
                let mut sort_descs = Vec::with_capacity(agg.sort_descs.len());
                for desc in &agg.sort_descs {
                    sort_descs.push(AggregateFunctionScalarSortDesc {
                        expr: self.flatten_scalar(&desc.expr, correlated_columns)?,
                        is_reuse_index: desc.is_reuse_index,
                        nulls_first: desc.nulls_first,
                        asc: desc.asc,
                    });
                }
                Ok(ScalarExpr::AggregateFunction(AggregateFunction {
                    span: agg.span,
                    display_name: agg.display_name.clone(),
                    func_name: agg.func_name.clone(),
                    distinct: agg.distinct,
                    params: agg.params.clone(),
                    args,
                    return_type: agg.return_type.clone(),
                    sort_descs,
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
            ScalarExpr::UDFCall(udf) => {
                let arguments = udf
                    .arguments
                    .iter()
                    .map(|arg| self.flatten_scalar(arg, correlated_columns))
                    .collect::<Result<Vec<_>>>()?;
                Ok(ScalarExpr::UDFCall(UDFCall {
                    span: udf.span,
                    name: udf.name.clone(),
                    handler: udf.handler.clone(),
                    headers: udf.headers.clone(),
                    display_name: udf.display_name.clone(),
                    udf_type: udf.udf_type.clone(),
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
