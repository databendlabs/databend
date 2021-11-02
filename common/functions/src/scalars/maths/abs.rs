// Copyright 2020 Datafuse Labs.
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

use std::fmt;

use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::compute::arity::unary;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function::Function;
use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;

#[derive(Clone)]
pub struct AbsFunction {
    _display_name: String,
}

impl AbsFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(AbsFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }
}

macro_rules! impl_abs_function {
    ($column:expr, $data_type:expr, $type:ident) => {{
        let array = $column.column().to_minimal_array()?.get_array_ref();
        let primitive_array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<$type>>()
            .ok_or_else(|| {
                ErrorCode::UnexpectedError(format!(
                    "Downcast failed to type PrimitiveArray<{}>, value: {:?}",
                    std::any::type_name::<$type>(),
                    array
                ))
            })?;
        let result = unary(primitive_array, |v| v.abs(), $data_type.to_arrow());
        Ok(DFPrimitiveArray::new(result).into())
    }};
}

impl Function for AbsFunction {
    fn name(&self) -> &str {
        "abs"
    }

    fn num_arguments(&self) -> usize {
        1
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(args[0].clone())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        match columns[0].data_type() {
            DataType::Int8 => impl_abs_function!(columns[0], DataType::Int8, i8),
            DataType::Int16 => impl_abs_function!(columns[0], DataType::Int16, i16),
            DataType::Int32 => impl_abs_function!(columns[0], DataType::Int32, i32),
            DataType::Int64 => impl_abs_function!(columns[0], DataType::Int64, i64),
            DataType::Float32 => impl_abs_function!(columns[0], DataType::Float32, f32),
            DataType::Float64 => impl_abs_function!(columns[0], DataType::Float64, f64),
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                Ok(columns[0].column().clone())
            }
            _ => Err(ErrorCode::IllegalDataType(format!(
                "Expected numeric types, but got {}",
                columns[0].data_type()
            ))),
        }
    }
}

impl fmt::Display for AbsFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ABS")
    }
}
