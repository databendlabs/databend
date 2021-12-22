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
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct ConcatWsFunction {
    _display_name: String,
}

impl ConcatWsFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(ConcatWsFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }

    fn concat_column_with_seperator(
        seperator: &DataColumn,
        acc: DataColumn,
        columns: &DataColumnsWithField,
    ) -> Result<DataColumn> {
        if columns.is_empty() {
            return Ok(acc);
        }
        let current = columns[0].column();
        // if this column is DataType::Null, just skip it
        if current.data_type().is_null() {
            return Self::concat_column_with_seperator(seperator, acc, &columns[1..]);
        }
        // whether is the last column to decide decide add separator or not
        let is_last = columns.len() == 1;
        // add this column value with acc
        let current = current.cast_with_type(&DataType::String)?;
        let acc = acc.cast_with_type(&DataType::String)?;
        let sep = seperator.cast_with_type(&DataType::String)?;
        let column = match sep {
            DataColumn::Constant(DataValue::String(Some(sep)), len) => match (acc, current) {
                (DataColumn::Array(lhs), DataColumn::Array(rhs)) => {
                    let l_array = lhs.string()?;
                    let r_array = rhs.string()?;
                    let mut builder = StringArrayBuilder::with_capacity(l_array.len());
                    let iter = l_array.into_iter().zip(r_array.into_iter());
                    for (l, r) in iter {
                        match (l, r) {
                            (Some(l), Some(r)) => {
                                if is_last {
                                    builder.append_value([l, r].concat());
                                } else {
                                    builder.append_value([l, r, sep.as_slice()].concat());
                                }
                            }
                            (Some(l), None) => {
                                // current row is NULL, do not add separator
                                builder.append_value(l);
                            }
                            // acc column could never be null
                            (None, _) => {
                                return Err(ErrorCode::UnexpectedError("CONCAT_WS internal error"));
                            }
                        }
                    }
                    Ok(DataColumn::Array(builder.finish().into_series()))
                }
                (DataColumn::Array(lhs), DataColumn::Constant(rhs, _)) => match rhs {
                    DataValue::String(Some(r_value)) => {
                        let l_array = lhs.string()?;
                        let mut builder = StringArrayBuilder::with_capacity(len);
                        let iter = l_array.into_iter();
                        for val in iter {
                            if let Some(val) = val {
                                if is_last {
                                    builder.append_value([val, r_value.as_slice()].concat());
                                } else {
                                    builder.append_value(
                                        [val, r_value.as_slice(), sep.as_slice()].concat(),
                                    );
                                }
                            } else {
                                // this never happens, acc column could never be null
                                return Err(ErrorCode::UnexpectedError("CONCAT_WS internal error"));
                            }
                        }
                        let array = builder.finish();
                        Ok(DataColumn::Array(array.into_series()))
                    }
                    _ => Ok(DataColumn::Array(lhs)),
                },
                (DataColumn::Constant(lhs, _), DataColumn::Array(rhs)) => match lhs {
                    DataValue::String(Some(l_value)) => {
                        let r_array = rhs.string()?;
                        let mut builder = StringArrayBuilder::with_capacity(len);
                        let iter = r_array.into_iter();
                        for val in iter {
                            if let Some(val) = val {
                                if is_last {
                                    builder.append_value([l_value.as_slice(), val].concat());
                                } else {
                                    builder.append_value(
                                        [l_value.as_slice(), val, sep.as_slice()].concat(),
                                    );
                                }
                            } else {
                                // r_value is NULL, do not add separator
                                builder.append_value(l_value.as_slice());
                            }
                        }
                        let array = builder.finish();
                        Ok(DataColumn::Array(array.into_series()))
                    }
                    // acc column could never be null
                    _ => Err(ErrorCode::UnexpectedError("CONCAT_WS internal error")),
                },
                (DataColumn::Constant(lhs, _), DataColumn::Constant(rhs, _)) => match &lhs {
                    DataValue::String(Some(l_val)) => match rhs {
                        DataValue::String(Some(r_val)) => {
                            let value = if is_last {
                                DataValue::String(Some(
                                    [l_val.as_slice(), r_val.as_slice()].concat(),
                                ))
                            } else {
                                DataValue::String(Some(
                                    [l_val.as_slice(), r_val.as_slice(), sep.as_slice()].concat(),
                                ))
                            };
                            let column = DataColumn::Constant(value, len);
                            Ok(column)
                        }
                        // current column is NULL, do not add separator
                        _ => Ok(DataColumn::Constant(lhs.clone(), len)),
                    },
                    // acc column could never be NULL
                    _ => Err(ErrorCode::UnexpectedError("CONCAT_WS internal error")),
                },
            },
            // seprator must be DataColumn::Constant(DataValue::String(Some(x)))
            _ => Err(ErrorCode::UnexpectedError(
                "CONCAT_WS separator must be constant",
            )),
        }?;
        let result = Self::concat_column_with_seperator(seperator, column, &columns[1..])?;
        Ok(result)
    }
}

impl Function for ConcatWsFunction {
    fn name(&self) -> &str {
        "concat_ws"
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args[0].is_null() {
            return Ok(DataType::Null);
        }
        Ok(DataType::String)
    }

    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        Some((2, 1024))
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let seperator = &columns[0];
        if seperator.column().data_type().is_null() {
            return Ok(DataColumn::Constant(DataValue::Null, input_rows));
        }
        // simplify seperator with only one column
        let acc = DataColumn::Constant(DataValue::String(Some(Vec::new())), input_rows);
        let result = Self::concat_column_with_seperator(seperator.column(), acc, &columns[1..])?;
        Ok(result)
    }

    fn passthrough_null(&self) -> bool {
        false
    }
}

impl fmt::Display for ConcatWsFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CONCAT_WS")
    }
}
