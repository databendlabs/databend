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
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct ConcatFunction {
    _display_name: String,
}

impl ConcatFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(ConcatFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }

    fn concat_column(lhs: DataColumn, columns: &DataColumnsWithField) -> Result<DataColumn> {
        if lhs.data_type().is_null() {
            return Ok(lhs);
        }
        let lhs = lhs.cast_with_type(&DataType::String)?;
        if columns.is_empty() {
            return Ok(lhs);
        }
        let rhs = columns[0].column();
        if rhs.data_type().is_null() {
            return Ok(rhs.clone());
        }
        let rhs = rhs.cast_with_type(&DataType::String)?;
        let column = match (lhs, rhs) {
            (DataColumn::Array(lhs), DataColumn::Array(rhs)) => {
                let l_array = lhs.string()?;
                let r_array = rhs.string()?;
                let array = DFStringArray::new_from_iter_validity(
                    l_array
                        .into_no_null_iter()
                        .zip(r_array.into_no_null_iter())
                        .map(|(l, r)| [l, r].concat()),
                    combine_validities(l_array.inner().validity(), r_array.inner().validity()),
                );
                DataColumn::Array(array.into_series())
            }
            (DataColumn::Array(lhs), DataColumn::Constant(rhs, len)) => match rhs {
                DataValue::String(Some(r_value)) => {
                    let l_array = lhs.string()?;
                    let mut builder = StringArrayBuilder::with_capacity(len);
                    let iter = l_array.into_iter();
                    for val in iter {
                        if let Some(val) = val {
                            builder.append_value([val, r_value.as_slice()].concat());
                        } else {
                            builder.append_null();
                        }
                    }
                    let array = builder.finish();
                    DataColumn::Array(array.into_series())
                }
                _ => DataColumn::Array(DFStringArray::full_null(len).into_series()),
            },
            (DataColumn::Constant(lhs, len), DataColumn::Array(rhs)) => match lhs {
                DataValue::String(Some(l_value)) => {
                    let r_array = rhs.string()?;
                    let mut builder = StringArrayBuilder::with_capacity(len);
                    let iter = r_array.into_iter();
                    for val in iter {
                        if let Some(val) = val {
                            builder.append_value([val, l_value.as_slice()].concat());
                        } else {
                            builder.append_null();
                        }
                    }
                    let array = builder.finish();
                    DataColumn::Array(array.into_series())
                }
                _ => DataColumn::Array(DFStringArray::full_null(len).into_series()),
            },
            (DataColumn::Constant(lhs, len), DataColumn::Constant(rhs, _)) => match lhs {
                DataValue::String(Some(l_val)) => match rhs {
                    DataValue::String(Some(r_val)) => DataColumn::Constant(
                        DataValue::String(Some([l_val.as_slice(), r_val.as_slice()].concat())),
                        len,
                    ),
                    _ => DataColumn::Constant(DataValue::Null, len),
                },
                _ => DataColumn::Constant(DataValue::Null, len),
            },
        };
        let result = Self::concat_column(column, &columns[1..])?;
        Ok(result)
    }
}

impl Function for ConcatFunction {
    fn name(&self) -> &str {
        "concat"
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        for arg in args {
            if arg.is_null() {
                return Ok(DataType::Null);
            }
        }
        Ok(DataType::String)
    }

    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        Some((1, 1024))
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        let result = Self::concat_column(columns[0].column().clone(), &columns[1..])?;
        Ok(result)
    }

    fn passthrough_null(&self) -> bool {
        false
    }
}

impl fmt::Display for ConcatFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CONCAT")
    }
}
