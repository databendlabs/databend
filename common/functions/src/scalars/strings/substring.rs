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
use itertools::izip;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct SubstringFunction {
    display_name: String,
}

impl SubstringFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(SubstringFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(2, 3),
        )
    }
}

impl Function for SubstringFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[DataTypeAndNullable]) -> Result<DataType> {
        if !args[0].is_numeric() && !args[0].is_string() && !args[0].is_null() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected string or null, but got {}",
                args[0]
            )));
        }
        if !args[1].is_integer() && !args[1].is_string() && !args[1].is_null() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string or null, but got {}",
                args[1]
            )));
        }
        if args.len() > 2 && !args[2].is_integer() && !args[2].is_string() && !args[2].is_null() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string or null, but got {}",
                args[2]
            )));
        }
        Ok(DataType::String)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let s_column = columns[0].column().cast_with_type(&DataType::String)?;
        let p_column = columns[1].column().cast_with_type(&DataType::Int64)?;

        let r_column: DataColumn = match (s_column, p_column) {
            // #00
            (
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::Int64(p), _),
            ) => {
                if let (Some(s), Some(p)) = (s, p) {
                    if columns.len() > 2 {
                        match columns[2].column().cast_with_type(&DataType::UInt64)? {
                            DataColumn::Constant(DataValue::UInt64(l), _) => {
                                if let Some(l) = l {
                                    DataColumn::Constant(
                                        DataValue::String(Some(
                                            substr_from_for(&s, &p, &l).to_owned(),
                                        )),
                                        input_rows,
                                    )
                                } else {
                                    DataColumn::Constant(DataValue::Null, input_rows)
                                }
                            }
                            DataColumn::Array(l_series) => {
                                let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                                for ol in l_series.u64()? {
                                    r_array.append_option(ol.map(|l| substr_from_for(&s, &p, l)));
                                }
                                r_array.finish().into()
                            }
                            _ => DataColumn::Constant(DataValue::Null, input_rows),
                        }
                    } else {
                        DataColumn::Constant(
                            DataValue::String(Some(substr_from(&s, &p).to_owned())),
                            input_rows,
                        )
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #10
            (DataColumn::Array(s_series), DataColumn::Constant(DataValue::Int64(p), _)) => {
                if let Some(p) = p {
                    if columns.len() > 2 {
                        match columns[2].column().cast_with_type(&DataType::UInt64)? {
                            DataColumn::Constant(DataValue::UInt64(l), _) => {
                                if let Some(l) = l {
                                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                                    for os in s_series.string()? {
                                        r_array
                                            .append_option(os.map(|s| substr_from_for(s, &p, &l)));
                                    }
                                    r_array.finish().into()
                                } else {
                                    DataColumn::Constant(DataValue::Null, input_rows)
                                }
                            }
                            DataColumn::Array(l_series) => {
                                let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                                for s_l in izip!(s_series.string()?, l_series.u64()?) {
                                    r_array.append_option(match s_l {
                                        (Some(s), Some(l)) => Some(substr_from_for(s, &p, l)),
                                        _ => None,
                                    });
                                }
                                r_array.finish().into()
                            }
                            _ => DataColumn::Constant(DataValue::Null, input_rows),
                        }
                    } else {
                        let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                        for os in s_series.string()? {
                            r_array.append_option(os.map(|s| substr_from(s, &p)));
                        }
                        r_array.finish().into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #01
            (DataColumn::Constant(DataValue::String(s), _), DataColumn::Array(p_series)) => {
                if let Some(s) = s {
                    if columns.len() > 2 {
                        match columns[2].column().cast_with_type(&DataType::UInt64)? {
                            DataColumn::Constant(DataValue::UInt64(l), _) => {
                                if let Some(l) = l {
                                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                                    for op in p_series.i64()? {
                                        r_array
                                            .append_option(op.map(|p| substr_from_for(&s, p, &l)));
                                    }
                                    r_array.finish().into()
                                } else {
                                    DataColumn::Constant(DataValue::Null, input_rows)
                                }
                            }
                            DataColumn::Array(l_series) => {
                                let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                                for s_l in izip!(p_series.i64()?, l_series.u64()?) {
                                    r_array.append_option(match s_l {
                                        (Some(p), Some(l)) => Some(substr_from_for(&s, p, l)),
                                        _ => None,
                                    });
                                }
                                r_array.finish().into()
                            }
                            _ => DataColumn::Constant(DataValue::Null, input_rows),
                        }
                    } else {
                        let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                        for op in p_series.i64()? {
                            r_array.append_option(op.map(|p| substr_from(&s, p)));
                        }
                        r_array.finish().into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #11
            (DataColumn::Array(s_series), DataColumn::Array(p_series)) => {
                if columns.len() > 2 {
                    match columns[2].column().cast_with_type(&DataType::UInt64)? {
                        DataColumn::Constant(DataValue::UInt64(l), _) => {
                            if let Some(l) = l {
                                let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                                for s_p in izip!(s_series.string()?, p_series.i64()?) {
                                    r_array.append_option(match s_p {
                                        (Some(s), Some(p)) => Some(substr_from_for(s, p, &l)),
                                        _ => None,
                                    });
                                }
                                r_array.finish().into()
                            } else {
                                DataColumn::Constant(DataValue::Null, input_rows)
                            }
                        }
                        DataColumn::Array(l_series) => {
                            let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                            for s_p_l in izip!(s_series.string()?, p_series.i64()?, l_series.u64()?)
                            {
                                r_array.append_option(match s_p_l {
                                    (Some(s), Some(p), Some(l)) => Some(substr_from_for(s, p, l)),
                                    _ => None,
                                });
                            }
                            r_array.finish().into()
                        }
                        _ => DataColumn::Constant(DataValue::Null, input_rows),
                    }
                } else {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for s_p in izip!(s_series.string()?, p_series.i64()?) {
                        r_array.append_option(match s_p {
                            (Some(s), Some(p)) => Some(substr_from(s, p)),
                            _ => None,
                        });
                    }
                    r_array.finish().into()
                }
            }
            _ => DataColumn::Constant(DataValue::Null, input_rows),
        };
        Ok(r_column)
    }
}

impl fmt::Display for SubstringFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

#[inline]
fn substr_from<'a>(str: &'a [u8], pos: &i64) -> &'a [u8] {
    substr(str, pos, &(str.len() as u64))
}

#[inline]
fn substr_from_for<'a>(str: &'a [u8], pos: &i64, len: &u64) -> &'a [u8] {
    substr(str, pos, len)
}

#[inline]
fn substr<'a>(str: &'a [u8], pos: &i64, len: &u64) -> &'a [u8] {
    if *pos > 0 && *pos <= str.len() as i64 {
        let l = str.len() as usize;
        let s = (*pos - 1) as usize;
        let mut e = *len as usize + s;
        if e > l {
            e = l;
        }
        return &str[s..e];
    }
    if *pos < 0 && -(*pos) <= str.len() as i64 {
        let l = str.len() as usize;
        let s = l - -*pos as usize;
        let mut e = *len as usize + s;
        if e > l {
            e = l;
        }
        return &str[s..e];
    }
    &str[0..0]
}
