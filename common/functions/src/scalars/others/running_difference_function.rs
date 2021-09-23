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

use common_arrow::arrow::array::Array;
use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::DataColumnsWithField;
use common_datavalues::prelude::*;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone)]
pub struct RunningDifferenceFunction {
    display_name: String,
}

impl RunningDifferenceFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(RunningDifferenceFunction {
            display_name: display_name.to_string(),
        }))
    }
}

impl Function for RunningDifferenceFunction {
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        match args[0] {
            DataType::Int8 | DataType::UInt8 => Ok(DataType::Int16),
            DataType::Int16 | DataType::UInt16 | DataType::Date16 => Ok(DataType::Int32),
            DataType::Int32
            | DataType::UInt32
            | DataType::Int64
            | DataType::UInt64
            | DataType::Date32
            | DataType::DateTime32(_) => Ok(DataType::Int64),
            _ => Result::Err(ErrorCode::IllegalDataType(
                "Argument for function runningDifference must have numeric type",
            )),
        }
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(true)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        match columns[0].data_type() {
            DataType::Int8 => compute_i8(columns[0].column(), input_rows),
            DataType::UInt8 => compute_u8(columns[0].column(), input_rows),
            DataType::Int16 => compute_i16(columns[0].column(), input_rows),
            DataType::UInt16 | DataType::Date16 => compute_u16(columns[0].column(), input_rows),
            DataType::Int32 => compute_i32(columns[0].column(), input_rows),
            DataType::UInt32 | DataType::Date32 | DataType::DateTime32(_) => {
                compute_u32(columns[0].column(), input_rows)
            }
            DataType::Int64 => compute_i64(columns[0].column(), input_rows),
            DataType::UInt64 => compute_u64(columns[0].column(), input_rows),
            _ => Result::Err(ErrorCode::IllegalDataType(
                format!(
                    "Argument for function runningDifference must have numeric type.: While processing runningDifference({})",
                    columns[0].field().name(),
                ))),
        }
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn num_arguments(&self) -> usize {
        1
    }
}

macro_rules! run_difference_compute {
    ($method:ident, $result_type:ident, $target_type:ty, $func: ident) => {
        fn $func(column: &DataColumn, input_rows: usize) -> Result<DataColumn> {
            if let DataColumn::Constant(_, _) = column {
                Ok(DataColumn::Constant(
                    DataValue::$result_type(Some(0i8 as $target_type)),
                    input_rows,
                ))
            } else {
                let series = column.to_array()?;
                let array = series.$method()?.inner();

                let mut result_vec = Vec::with_capacity(array.len());
                for index in 0..array.len() {
                    match array.is_null(index) {
                        true => result_vec.push(None),
                        false => {
                            if index == 0 {
                                result_vec.push(Some(0i8 as $target_type))
                            } else if array.is_null(index - 1) {
                                result_vec.push(None)
                            } else {
                                let diff = array.value(index) as $target_type
                                    - array.value(index - 1) as $target_type;
                                result_vec.push(Some(diff))
                            }
                        }
                    }
                }

                Ok(Series::new(result_vec).into())
            }
        }
    };
}

run_difference_compute!(i8, Int16, i16, compute_i8);
run_difference_compute!(u8, Int16, i16, compute_u8);
run_difference_compute!(i16, Int32, i32, compute_i16);
run_difference_compute!(u16, Int32, i32, compute_u16);
run_difference_compute!(i32, Int64, i64, compute_i32);
run_difference_compute!(u32, Int64, i64, compute_u32);
run_difference_compute!(i64, Int64, i64, compute_i64);
run_difference_compute!(u64, Int64, i64, compute_u64);

impl fmt::Display for RunningDifferenceFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}
