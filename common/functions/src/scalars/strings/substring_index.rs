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
pub struct SubstringIndexFunction {
    display_name: String,
}

impl SubstringIndexFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(SubstringIndexFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(3))
    }
}

impl Function for SubstringIndexFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(true)
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if !args[0].is_numeric() && args[0] != DataType::String && args[0] != DataType::Null {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected string or null, but got {}",
                args[0]
            )));
        }
        if !args[1].is_numeric() && args[1] != DataType::String && args[1] != DataType::Null {
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
        let d_column = columns[1].column().cast_with_type(&DataType::String)?;
        let c_column = columns[2].column().cast_with_type(&DataType::Int64)?;

        let r_column: DataColumn = match (s_column, d_column, c_column) {
            //000
            (
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::String(d), _),
                DataColumn::Constant(DataValue::Int64(c), _),
            ) => {
                if let (Some(s), Some(d), Some(c)) = (s, d, c) {
                    DataColumn::Constant(
                        DataValue::String(Some(substring_index(&s, &d, &c).to_owned())),
                        input_rows,
                    )
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            //100
            (
                DataColumn::Array(s_series),
                DataColumn::Constant(DataValue::String(d), _),
                DataColumn::Constant(DataValue::Int64(c), _),
            ) => {
                if let (Some(d), Some(c)) = (d, c) {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for os in s_series.string()? {
                        r_array.append_option(os.map(|s| substring_index(s, &d, &c)));
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            //010
            (
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Array(d_series),
                DataColumn::Constant(DataValue::Int64(c), _),
            ) => {
                if let (Some(s), Some(c)) = (s, c) {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for od in d_series.string()? {
                        r_array.append_option(od.map(|d| substring_index(&s, d, &c)));
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            //110
            (
                DataColumn::Array(s_series),
                DataColumn::Array(d_series),
                DataColumn::Constant(DataValue::Int64(c), _),
            ) => {
                if let Some(c) = c {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for s_d in izip!(s_series.string()?, d_series.string()?) {
                        r_array.append_option(match s_d {
                            (Some(s), Some(d)) => Some(substring_index(s, d, &c)),
                            _ => None,
                        });
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            //001
            (
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::String(d), _),
                DataColumn::Array(c_series),
            ) => {
                if let (Some(s), Some(d)) = (s, d) {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for oc in c_series.i64()? {
                        r_array.append_option(oc.map(|c| substring_index(&s, &d, c)));
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            //101
            (
                DataColumn::Array(s_series),
                DataColumn::Constant(DataValue::String(d), _),
                DataColumn::Array(c_series),
            ) => {
                if let Some(d) = d {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for s_c in izip!(s_series.string()?, c_series.i64()?) {
                        r_array.append_option(match s_c {
                            (Some(s), Some(c)) => Some(substring_index(s, &d, c)),
                            _ => None,
                        });
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            //011
            (
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Array(d_series),
                DataColumn::Array(c_series),
            ) => {
                if let Some(s) = s {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for d_c in izip!(d_series.string()?, c_series.i64()?) {
                        r_array.append_option(match d_c {
                            (Some(d), Some(c)) => Some(substring_index(&s, d, c)),
                            _ => None,
                        });
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            //111
            (
                DataColumn::Array(s_series),
                DataColumn::Array(d_series),
                DataColumn::Array(c_series),
            ) => {
                let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                for s_d_c in izip!(s_series.string()?, d_series.string()?, c_series.i64()?) {
                    r_array.append_option(match s_d_c {
                        (Some(s), Some(d), Some(c)) => Some(substring_index(s, d, c)),
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

impl fmt::Display for SubstringIndexFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

#[inline]
fn substring_index<'a>(str: &'a [u8], delim: &'a [u8], count: &i64) -> &'a [u8] {
    if *count == 0 {
        return &str[0..0];
    }
    if *count > 0 {
        let count = (*count) as usize;
        let mut c = 0;
        for (p, w) in str.windows(delim.len()).enumerate() {
            if w == delim {
                c += 1;
                if c == count {
                    return &str[0..p];
                }
            }
        }
    } else {
        let count = (*count).abs() as usize;
        let mut c = 0;
        for (p, w) in str.windows(delim.len()).rev().enumerate() {
            if w == delim {
                c += 1;
                if c == count {
                    return &str[str.len() - p..];
                }
            }
        }
    }
    str
}
