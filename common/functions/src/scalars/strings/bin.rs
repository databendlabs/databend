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

use common_datavalues::prelude::*;
use common_datavalues::DataTypeAndNullable;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct BinFunction {
    _display_name: String,
}

impl BinFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(BinFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

impl Function for BinFunction {
    fn name(&self) -> &str {
        "bin"
    }

    fn return_type(&self, args: &[DataTypeAndNullable]) -> Result<DataTypeAndNullable> {
        if !args[0].is_numeric() && !args[0].is_null() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected number or null, but got {}",
                args[0]
            )));
        }

        let nullable = args.iter().any(|arg| arg.is_nullable());
        let dt = DataType::String;
        Ok(DataTypeAndNullable::create(&dt, nullable))
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let mut string_array = StringArrayBuilder::with_capacity(input_rows);
        match columns[0].data_type() {
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                for value in columns[0]
                    .column()
                    .cast_with_type(&DataType::UInt64)?
                    .to_minimal_array()?
                    .u64()?
                {
                    string_array.append_option(value.map(|n| format!("{:b}", n)));
                }
            }
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                for value in columns[0]
                    .column()
                    .cast_with_type(&DataType::Int64)?
                    .to_minimal_array()?
                    .i64()?
                {
                    string_array.append_option(value.map(|n| format!("{:b}", n)));
                }
            }
            DataType::Float32 | DataType::Float64 => {
                for value in columns[0]
                    .column()
                    .cast_with_type(&DataType::Float64)?
                    .to_minimal_array()?
                    .f64()?
                {
                    string_array.append_option(value.map(|n| {
                        if n.ge(&0f64) {
                            format!(
                                "{:b}",
                                n.max(i64::MIN as f64).min(i64::MAX as f64).round() as i64
                            )
                        } else {
                            format!(
                                "{:b}",
                                n.max(u64::MIN as f64).min(u64::MAX as f64).round() as u64
                            )
                        }
                    }));
                }
            }
            _ => {
                string_array.append_null();
            }
        }

        let column: DataColumn = string_array.finish().into();
        Ok(column.resize_constant(columns[0].column().len()))
    }
}

impl fmt::Display for BinFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BIN")
    }
}
