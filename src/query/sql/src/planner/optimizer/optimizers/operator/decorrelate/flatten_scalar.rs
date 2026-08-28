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

use super::DerivedColumnScope;
use crate::ColumnSet;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::optimizers::operator::SubqueryDecorrelatorOptimizer;
use crate::plans::AggregateFunction;
use crate::plans::AggregateFunctionScalarSortDesc;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::FunctionCall;
use crate::plans::LambdaFunc;
use crate::plans::ScalarExpr;
use crate::plans::UDAFCall;
use crate::plans::UDFCall;
use crate::plans::UDFLambdaCall;

impl SubqueryDecorrelatorOptimizer {
    #[recursive::recursive]
    pub(crate) fn flatten_scalar(
        &self,
        scalar: &ScalarExpr,
        correlated_columns: &ColumnSet,
        derived_columns: &DerivedColumnScope,
    ) -> Result<ScalarExpr> {
        match scalar {
            ScalarExpr::BoundColumnRef(bound_column) => {
                let column_binding = bound_column.column.clone();
                if correlated_columns.contains(&column_binding.index) {
                    let index = derived_columns.must_resolve(column_binding.index)?;
                    let metadata = self.metadata.read();
                    let column_entry = metadata.column(index);
                    return Ok(ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: scalar.span(),
                        column: ColumnBindingBuilder::new(
                            column_entry.name(),
                            index,
                            Box::new(column_entry.data_type()),
                            column_binding.visibility,
                        )
                        .build(),
                    }));
                }
                Ok(scalar.clone())
            }
            ScalarExpr::ConstantExpr(_) | ScalarExpr::TypedConstantExpr(_, _) => Ok(scalar.clone()),
            ScalarExpr::AggregateFunction(agg) => {
                let mut args = Vec::with_capacity(agg.args.len());
                for arg in &agg.args {
                    args.push(self.flatten_scalar(arg, correlated_columns, derived_columns)?);
                }
                let mut sort_descs = Vec::with_capacity(agg.sort_descs.len());
                for desc in &agg.sort_descs {
                    sort_descs.push(AggregateFunctionScalarSortDesc {
                        expr: self.flatten_scalar(
                            &desc.expr,
                            correlated_columns,
                            derived_columns,
                        )?,
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
                    .map(|arg| self.flatten_scalar(arg, correlated_columns, derived_columns))
                    .collect::<Result<Vec<_>>>()?;
                Ok(ScalarExpr::FunctionCall(FunctionCall {
                    span: func.span,
                    func_name: func.func_name.clone(),
                    params: func.params.clone(),
                    arguments,
                    return_type: func.return_type.clone(),
                }))
            }
            ScalarExpr::LambdaFunction(lambda) => {
                let args = lambda
                    .args
                    .iter()
                    .map(|arg| self.flatten_scalar(arg, correlated_columns, derived_columns))
                    .collect::<Result<Vec<_>>>()?;
                let mut lambda = LambdaFunc {
                    span: lambda.span,
                    func_name: lambda.func_name.clone(),
                    args,
                    lambda_expr: lambda.lambda_expr.clone(),
                    lambda_display: lambda.lambda_display.clone(),
                    return_type: lambda.return_type.clone(),
                };
                lambda.refresh_return_type()?;
                Ok(ScalarExpr::LambdaFunction(lambda))
            }
            ScalarExpr::CastExpr(cast_expr) => {
                let scalar =
                    self.flatten_scalar(&cast_expr.argument, correlated_columns, derived_columns)?;
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
                    .map(|arg| self.flatten_scalar(arg, correlated_columns, derived_columns))
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
            ScalarExpr::UDAFCall(udaf) => {
                let arguments = udaf
                    .arguments
                    .iter()
                    .map(|arg| self.flatten_scalar(arg, correlated_columns, derived_columns))
                    .collect::<Result<Vec<_>>>()?;
                Ok(ScalarExpr::UDAFCall(UDAFCall {
                    span: udaf.span,
                    name: udaf.name.clone(),
                    display_name: udaf.display_name.clone(),
                    arg_types: udaf.arg_types.clone(),
                    state_fields: udaf.state_fields.clone(),
                    return_type: udaf.return_type.clone(),
                    arguments,
                    udf_type: udaf.udf_type.clone(),
                }))
            }
            ScalarExpr::UDFLambdaCall(udf) => {
                let scalar =
                    self.flatten_scalar(&udf.scalar, correlated_columns, derived_columns)?;
                Ok(ScalarExpr::UDFLambdaCall(UDFLambdaCall {
                    span: udf.span,
                    func_name: udf.func_name.clone(),
                    scalar: Box::new(scalar),
                }))
            }
            ScalarExpr::WindowFunction(_) => Err(ErrorCode::SemanticError(
                "Window functions are not supported while flattening correlated subqueries",
            )),
            ScalarExpr::SubqueryExpr(_) => Err(ErrorCode::Internal(
                "Nested subqueries must be decorrelated before flattening correlated subqueries",
            )),
            ScalarExpr::AsyncFunctionCall(_) => Err(ErrorCode::SemanticError(
                "Async functions are not supported while flattening correlated subqueries",
            )),
        }
    }
}
