// Copyright 2021 Datafuse Labs.
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

use common_arrow::arrow::buffer::MutableBuffer;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct CharFunction {
    _display_name: String,
}

impl CharFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(CharFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(1, 1024),
        )
    }
}

impl Function for CharFunction {
    fn name(&self) -> &str {
        "char"
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        for arg in args {
            if !arg.is_numeric() {
                return Err(ErrorCode::IllegalDataType(format!(
                    "Expected numeric type, but got {}",
                    arg
                )));
            }
        }
        Ok(DataType::String)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        let row_count = columns[0].column().len();
        let column_count = columns.len();
        let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(row_count * column_count);
        let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(row_count + 1);
        offsets.push(0);

        for (i, column) in columns.iter().enumerate() {
            let column = column.column();
            if column.data_type().is_null() {
                continue;
            }
            let column = column.cast_with_type(&DataType::UInt8)?;
            match column {
                DataColumn::Array(uint8_arr) => {
                    let uint8_arr = uint8_arr.u8()?;
                    for (j, ch) in uint8_arr.into_iter().enumerate() {
                        match ch {
                            Some(&ch) => unsafe {
                                *values.as_mut_ptr().add(column_count * j + i) = ch;
                            },
                            None => {
                                return Err(ErrorCode::IllegalDataType(
                                    "Expected args a const, an expression, an expression, a const and a const"
                                ));
                            }
                        }
                    }
                }
                DataColumn::Constant(uint8_arr, _) => match uint8_arr {
                    DataValue::UInt8(Some(ch)) => unsafe {
                        for j in 0..row_count {
                            *values.as_mut_ptr().add(column_count * j + i) = ch;
                        }
                    },
                    _ => {
                        return Err(ErrorCode::IllegalDataType(
                            "Expected args a const, an expression, an expression, a const and a const"
                        ));
                    }
                },
            }
        }
        for i in 1..row_count + 1 {
            offsets.push(i as i64 * column_count as i64);
        }
        unsafe {
            offsets.set_len(row_count + 1);
            values.set_len(row_count * column_count);
            Ok(DataColumn::from(DFStringArray::from_data_unchecked(
                offsets.into(),
                values.into(),
                None,
            )))
        }
    }
}

impl fmt::Display for CharFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CHAR")
    }
}
