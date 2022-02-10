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

use std::cmp::Ordering;
use std::fmt;

use common_datavalues2::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::cast_column_field;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function2;
use crate::scalars::Function2Description;

#[derive(Clone)]
pub struct HexFunction {
    _display_name: String,
}

impl HexFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function2>> {
        Ok(Box::new(HexFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> Function2Description {
        Function2Description::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

impl Function2 for HexFunction {
    fn name(&self) -> &str {
        "hex"
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        if !args[0].data_type_id().is_numeric() && !args[0].data_type_id().is_string() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string but got {}",
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
                    builder.append(format!("{:x}", val).as_bytes());
                }
            }
            TypeID::Int8 | TypeID::Int16 | TypeID::Int32 | TypeID::Int64 => {
                let col = cast_column_field(&columns[0], &Int64Type::arc())?;
                let col = col.as_any().downcast_ref::<Int64Column>().unwrap();
                for val in col.iter() {
                    let val = match val.cmp(&0) {
                        Ordering::Less => {
                            format!("-{:x}", val.unsigned_abs())
                        }
                        _ => {
                            format!("{:x}", val)
                        }
                    };
                    builder.append(val.as_bytes());
                }
            }
            TypeID::String => {
                let col = cast_column_field(&columns[0], &StringType::arc())?;
                let col = col.as_any().downcast_ref::<StringColumn>().unwrap();
                for val in col.iter() {
                    let mut buffer = vec![0u8; val.len() * 2];
                    let buffer = &mut buffer[0..val.len() * 2];
                    let _ = hex::encode_to_slice(val, buffer);
                    builder.append(buffer)
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

impl fmt::Display for HexFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "HEX")
    }
}
