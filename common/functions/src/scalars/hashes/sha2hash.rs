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

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::*;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;
use sha2::Digest;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct Sha2HashFunction {
    display_name: String,
}

impl Sha2HashFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(Sha2HashFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}

impl Function for Sha2HashFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args[0] == DataType::String
            && matches!(
                args[1],
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32
            )
        {
            Ok(DataType::String)
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "Expected string and numeric type, but got {}",
                args[0]
            )))
        }
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let i_series = columns[0]
            .column()
            .to_minimal_array()?
            .cast_with_type(&DataType::String)?;
        let i_array = i_series.string()?;
        let l_column: DataColumn = columns[1].column().cast_with_type(&DataType::UInt16)?;

        let result = match l_column {
            DataColumn::Constant(v, _) => {
                let l: Result<u16> = v.as_u64().map(|v| v as u16);
                match l {
                    Err(_) => DFStringArray::full_null(input_rows),
                    Ok(v) => {
                        let validity = i_array.inner().validity().cloned();
                        match v {
                            224 => {
                                let iter = i_array.into_no_null_iter().map(|i| {
                                    let mut h = sha2::Sha224::new();
                                    h.update(i);
                                    format!("{:x}", h.finalize())
                                });

                                DFStringArray::new_from_iter_validity(iter, validity)
                            }
                            256 | 0 => {
                                let iter = i_array.into_no_null_iter().map(|i| {
                                    let mut h = sha2::Sha256::new();
                                    h.update(i);
                                    format!("{:x}", h.finalize())
                                });
                                DFStringArray::new_from_iter_validity(iter, validity)
                            }
                            384 => {
                                let iter = i_array.into_no_null_iter().map(|i| {
                                    let mut h = sha2::Sha384::new();
                                    h.update(i);
                                    format!("{:x}", h.finalize())
                                });
                                DFStringArray::new_from_iter_validity(iter, validity)
                            }
                            512 => {
                                let iter = i_array.into_no_null_iter().map(|i| {
                                    let mut h = sha2::Sha512::new();
                                    h.update(i);
                                    format!("{:x}", h.finalize())
                                });
                                DFStringArray::new_from_iter_validity(iter, validity)
                            }
                            _ => DFStringArray::full_null(input_rows),
                        }
                    }
                }
            }
            DataColumn::Array(l_series) => {
                let hash = |i: &[u8], l: &u16| match l {
                    224 => {
                        let mut h = sha2::Sha224::new();
                        h.update(i);
                        Some(format!("{:x}", h.finalize()))
                    }
                    256 | 0 => {
                        let mut h = sha2::Sha256::new();
                        h.update(i);
                        Some(format!("{:x}", h.finalize()))
                    }
                    384 => {
                        let mut h = sha2::Sha384::new();
                        h.update(i);
                        Some(format!("{:x}", h.finalize()))
                    }
                    512 => {
                        let mut h = sha2::Sha512::new();
                        h.update(i);
                        Some(format!("{:x}", h.finalize()))
                    }
                    _ => None,
                };

                let opt_iter =
                    i_array
                        .into_iter()
                        .zip(l_series.u16()?.into_iter())
                        .map(|(i, l)| match (i, l) {
                            (Some(i), Some(l)) => hash(i, l),
                            _ => None,
                        });
                DFStringArray::new_from_opt_iter(opt_iter)
            }
        };
        let column: DataColumn = result.into();
        Ok(column.resize_constant(columns[0].column().len()))
    }
}

impl fmt::Display for Sha2HashFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
