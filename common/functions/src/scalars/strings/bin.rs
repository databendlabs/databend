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

use std::fmt;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::cast_column_field;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

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

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        if !args[0].data_type_id().is_numeric() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer but got {}",
                args[0].data_type_id()
            )));
        }

        Ok(StringType::arc())
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let mut builder: ColumnBuilder<Vu8> = ColumnBuilder::with_capacity(input_rows);

        match columns[0].data_type().data_type_id() {
            TypeID::UInt8 | TypeID::UInt16 | TypeID::UInt32 | TypeID::UInt64 => {
                let col = cast_column_field(&columns[0], &UInt64Type::arc())?;
                let col = col.as_any().downcast_ref::<UInt64Column>().unwrap();
                for val in col.iter() {
                    builder.append(format!("{:b}", val).as_bytes());
                }
            }
            TypeID::Int8 | TypeID::Int16 | TypeID::Int32 | TypeID::Int64 => {
                let col = cast_column_field(&columns[0], &Int64Type::arc())?;
                let col = col.as_any().downcast_ref::<Int64Column>().unwrap();
                for val in col.iter() {
                    builder.append(format!("{:b}", val).as_bytes());
                }
            }
            TypeID::Float32 | TypeID::Float64 => {
                let col = cast_column_field(&columns[0], &Float64Type::arc())?;
                let col = col.as_any().downcast_ref::<Float64Column>().unwrap();
                for val in col.iter() {
                    let val = if val.ge(&0f64) {
                        format!(
                            "{:b}",
                            val.max(i64::MIN as f64).min(i64::MAX as f64).round() as i64
                        )
                    } else {
                        format!(
                            "{:b}",
                            val.max(u64::MIN as f64).min(u64::MAX as f64).round() as u64
                        )
                    };
                    builder.append(val.as_bytes());
                }
            }
            _ => {
                return Err(ErrorCode::IllegalDataType(format!(
                    "Expected integer but got {}",
                    columns[0].data_type().data_type_id()
                )));
            }
        }
        Ok(builder.build(input_rows))
    }
}

impl fmt::Display for BinFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BIN")
    }
}
