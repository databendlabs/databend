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
use itertools::izip;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct ReplaceFunction {
    display_name: String,
}

impl ReplaceFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(ReplaceFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }
}

impl Function for ReplaceFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn num_arguments(&self) -> usize {
        3
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(true)
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if !args[0].is_integer() && args[0] != DataType::String && args[0] != DataType::Null {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string or null, but got {}",
                args[0]
            )));
        }
        if !args[1].is_integer() && args[1] != DataType::String && args[1] != DataType::Null {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string or null, but got {}",
                args[1]
            )));
        }
        if !args[2].is_integer() && args[2] != DataType::String && args[2] != DataType::Null {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string or null, but got {}",
                args[2]
            )));
        }
        Ok(DataType::String)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let s_column = columns[0].column().cast_with_type(&DataType::String)?;
        let f_column = columns[1].column().cast_with_type(&DataType::String)?;
        let t_column = columns[2].column().cast_with_type(&DataType::String)?;

        let r_column: DataColumn = match (s_column, f_column, t_column) {
            // #000
            (
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::String(f), _),
                DataColumn::Constant(DataValue::String(t), _),
            ) => {
                if let (Some(s), Some(f), Some(t)) = (s, f, t) {
                    DataColumn::Constant(DataValue::String(Some(replace(&s, &f, &t))), input_rows)
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #100
            (
                DataColumn::Array(s_series),
                DataColumn::Constant(DataValue::String(f), _),
                DataColumn::Constant(DataValue::String(t), _),
            ) => {
                if let (Some(f), Some(t)) = (f, t) {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for os in s_series.string()? {
                        r_array.append_option(os.map(|s| replace(s, &f, &t)));
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #010
            (
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Array(f_series),
                DataColumn::Constant(DataValue::String(t), _),
            ) => {
                if let (Some(s), Some(t)) = (s, t) {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for of in f_series.string()? {
                        r_array.append_option(of.map(|f| replace(&s, f, &t)));
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #110
            (
                DataColumn::Array(s_series),
                DataColumn::Array(f_series),
                DataColumn::Constant(DataValue::String(t), _),
            ) => {
                if let Some(t) = t {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for s_f in izip!(s_series.string()?, f_series.string()?) {
                        r_array.append_option(match s_f {
                            (Some(s), Some(f)) => Some(replace(s, f, &t)),
                            _ => None,
                        });
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #001
            (
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::String(f), _),
                DataColumn::Array(t_series),
            ) => {
                if let (Some(s), Some(f)) = (s, f) {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for ot in t_series.string()? {
                        r_array.append_option(ot.map(|t| replace(&s, &f, t)));
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #101
            (
                DataColumn::Array(s_series),
                DataColumn::Constant(DataValue::String(f), _),
                DataColumn::Array(t_series),
            ) => {
                if let Some(f) = f {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for s_t in izip!(s_series.string()?, t_series.string()?) {
                        r_array.append_option(match s_t {
                            (Some(s), Some(t)) => Some(replace(s, &f, t)),
                            _ => None,
                        });
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #011
            (
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Array(f_series),
                DataColumn::Array(t_series),
            ) => {
                if let Some(s) = s {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for f_t in izip!(f_series.string()?, t_series.string()?) {
                        r_array.append_option(match f_t {
                            (Some(f), Some(t)) => Some(replace(&s, f, t)),
                            _ => None,
                        });
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #111
            (
                DataColumn::Array(s_series),
                DataColumn::Array(f_series),
                DataColumn::Array(t_series),
            ) => {
                let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                for s_f_t in izip!(s_series.string()?, f_series.string()?, t_series.string()?) {
                    r_array.append_option(match s_f_t {
                        (Some(s), Some(f), Some(t)) => Some(replace(s, f, t)),
                        _ => None,
                    });
                }
                r_array.finish().into()
            }
            _ => DataColumn::Constant(DataValue::Null, input_rows),
        };
        Ok(r_column)
    }
}

impl fmt::Display for ReplaceFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

#[inline]
fn replace<'a>(str: &'a [u8], from: &'a [u8], to: &'a [u8]) -> Vec<u8> {
    let mut result = str.to_vec();
    let from_len = from.len();
    if from_len > 0 {
        let to_len = to.len();
        let mut i = 0;
        while i + from_len <= result.len() {
            if result[i..].starts_with(from) {
                result.splice(i..i + from_len, to.iter().cloned());
                i += to_len;
            } else {
                i += 1;
            }
        }
    }
    result
}
