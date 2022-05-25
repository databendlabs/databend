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

use common_datablocks::DataBlock;
use common_datavalues::ColumnRef;
use common_datavalues::ColumnWithField;
use common_datavalues::DataField;
use common_datavalues::DataType;
use common_datavalues::DataTypeImpl;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::CastFunction;
use common_functions::scalars::FunctionContext;
use common_functions::scalars::FunctionFactory;
use common_planners::Expression;

pub struct ExpressionEvaluator;

impl ExpressionEvaluator {
    pub fn eval(
        func_ctx: &FunctionContext,
        expression: &Expression,
        block: &DataBlock,
    ) -> Result<ColumnRef> {
        match expression {
            Expression::Column(name) => block.try_column_by_name(name.as_str()).cloned(),

            Expression::Alias(_, _) => Err(ErrorCode::UnImplement(
                "Unsupported Alias scalar expression",
            )),

            Expression::QualifiedColumn(_) => Err(ErrorCode::LogicalError(
                "Unsupported QualifiedColumn scalar expression",
            )),

            Expression::Literal {
                value, data_type, ..
            } => data_type.create_constant_column(value, block.num_rows()),

            Expression::UnaryExpression { op, expr } => {
                let result = Self::eval(func_ctx, expr, block)?;
                let arg_types: Vec<DataTypeImpl> = vec![result.data_type()];
                let arg_types2: Vec<&DataTypeImpl> = arg_types.iter().collect();
                let func = FunctionFactory::instance().get(op, &arg_types2)?;

                let columns = [ColumnWithField::new(
                    result.clone(),
                    DataField::new("", result.data_type()),
                )];
                func.eval(func_ctx.clone(), &columns, block.num_rows())
            }

            Expression::BinaryExpression { left, op, right } => {
                let left_result = Self::eval(func_ctx, left.as_ref(), block)?;
                let right_result = Self::eval(func_ctx, right.as_ref(), block)?;
                let arg_types: Vec<DataTypeImpl> =
                    vec![left_result.data_type(), right_result.data_type()];
                let arg_types2: Vec<&DataTypeImpl> = arg_types.iter().collect();
                let func = FunctionFactory::instance().get(op, &arg_types2)?;

                let columns = [
                    ColumnWithField::new(
                        left_result.clone(),
                        DataField::new("", left_result.data_type()),
                    ),
                    ColumnWithField::new(
                        right_result.clone(),
                        DataField::new("", right_result.data_type()),
                    ),
                ];
                func.eval(func_ctx.clone(), &columns, block.num_rows())
            }

            Expression::ScalarFunction { op, args } => {
                let results = args
                    .iter()
                    .map(|expr| Self::eval(func_ctx, expr, block))
                    .collect::<Result<Vec<ColumnRef>>>()?;
                let arg_types: Vec<DataTypeImpl> =
                    results.iter().map(|col| col.data_type()).collect();
                let arg_types2: Vec<&DataTypeImpl> = arg_types.iter().collect();

                let func = FunctionFactory::instance().get(op, &arg_types2)?;
                let columns: Vec<ColumnWithField> = results
                    .into_iter()
                    .map(|col| {
                        ColumnWithField::new(col.clone(), DataField::new("", col.data_type()))
                    })
                    .collect();
                func.eval(func_ctx.clone(), &columns, block.num_rows())
            }

            Expression::AggregateFunction { .. } => Err(ErrorCode::LogicalError(
                "Unsupported AggregateFunction scalar expression",
            )),

            Expression::Sort { .. } => Err(ErrorCode::LogicalError(
                "Unsupported Sort scalar expression",
            )),

            Expression::Cast {
                expr, data_type, ..
            } => {
                let result = Self::eval(func_ctx, expr, block)?;
                let func_name = "cast".to_string();
                let from_type = result.data_type();
                let type_name = data_type.name();

                let func = if data_type.is_nullable() {
                    CastFunction::create_try(&func_name, &type_name, from_type)
                } else {
                    CastFunction::create(&func_name, &type_name, from_type)
                }?;

                let columns = [ColumnWithField::new(
                    result.clone(),
                    DataField::new("", result.data_type()),
                )];

                func.eval(func_ctx.clone(), &columns, block.num_rows())
            }

            Expression::ScalarSubquery { name, .. } => {
                // Scalar subquery results are ready in the expression input
                let expr = Expression::Column(name.clone());
                Self::eval(func_ctx, &expr, block)
            }

            Expression::Subquery { name, .. } => {
                // Subquery results are ready in the expression input
                let expr = Expression::Column(name.clone());
                Self::eval(func_ctx, &expr, block)
            }

            Expression::MapAccess { args, .. } => {
                let results = args
                    .iter()
                    .map(|expr| Self::eval(func_ctx, expr, block))
                    .collect::<Result<Vec<ColumnRef>>>()?;
                let arg_types: Vec<DataTypeImpl> =
                    results.iter().map(|col| col.data_type()).collect();
                let arg_types2: Vec<&DataTypeImpl> = arg_types.iter().collect();

                let func_name = "get_path";
                let func = FunctionFactory::instance().get(func_name, &arg_types2)?;
                let columns: Vec<ColumnWithField> = results
                    .into_iter()
                    .map(|col| {
                        ColumnWithField::new(col.clone(), DataField::new("", col.data_type()))
                    })
                    .collect();
                func.eval(func_ctx.clone(), &columns, block.num_rows())
            }

            Expression::Wildcard => Err(ErrorCode::LogicalError("Unsupported wildcard expression")),
        }
    }
}
