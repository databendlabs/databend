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
pub struct InsertFunction {
    display_name: String,
}

impl InsertFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(InsertFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }
}

impl Function for InsertFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn num_arguments(&self) -> usize {
        4
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(true)
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
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
        let p_column = columns[1].column().cast_with_type(&DataType::Int64)?;
        let l_column = columns[2].column().cast_with_type(&DataType::Int64)?;
        let ss_column = columns[3].column().cast_with_type(&DataType::String)?;

        let r_column: DataColumn = match (s_column, p_column, l_column, ss_column) {
            // #0000
            (
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::Int64(p), _),
                DataColumn::Constant(DataValue::Int64(l), _),
                DataColumn::Constant(DataValue::String(ss), _),
            ) => {
                if let (Some(s), Some(p), Some(l), Some(ss)) = (s, p, l, ss) {
                    DataColumn::Constant(
                        DataValue::String(Some(instr(&s, &p, &l, &ss))),
                        input_rows,
                    )
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #1000
            (
                DataColumn::Array(s_series),
                DataColumn::Constant(DataValue::Int64(p), _),
                DataColumn::Constant(DataValue::Int64(l), _),
                DataColumn::Constant(DataValue::String(ss), _),
            ) => {
                if let (Some(p), Some(l), Some(ss)) = (p, l, ss) {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for os in s_series.string()? {
                        r_array.append_option(os.map(|s| instr(s, &p, &l, &ss)));
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #0100
            (
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Array(p_series),
                DataColumn::Constant(DataValue::Int64(l), _),
                DataColumn::Constant(DataValue::String(ss), _),
            ) => {
                if let (Some(s), Some(l), Some(ss)) = (s, l, ss) {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for op in p_series.i64()? {
                        r_array.append_option(op.map(|p| instr(&s, p, &l, &ss)));
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #1100
            (
                DataColumn::Array(s_series),
                DataColumn::Array(p_series),
                DataColumn::Constant(DataValue::Int64(l), _),
                DataColumn::Constant(DataValue::String(ss), _),
            ) => {
                if let (Some(l), Some(ss)) = (l, ss) {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for s_p in izip!(s_series.string()?, p_series.i64()?) {
                        r_array.append_option(match s_p {
                            (Some(s), Some(p)) => Some(instr(s, p, &l, &ss)),
                            _ => None,
                        });
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #0010
            (
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::Int64(p), _),
                DataColumn::Array(l_series),
                DataColumn::Constant(DataValue::String(ss), _),
            ) => {
                if let (Some(s), Some(p), Some(ss)) = (s, p, ss) {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for ol in l_series.i64()? {
                        r_array.append_option(ol.map(|l| instr(&s, &p, l, &ss)));
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #1010
            (
                DataColumn::Array(s_series),
                DataColumn::Constant(DataValue::Int64(p), _),
                DataColumn::Array(l_series),
                DataColumn::Constant(DataValue::String(ss), _),
            ) => {
                if let (Some(p), Some(ss)) = (p, ss) {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for s_l in izip!(s_series.string()?, l_series.i64()?) {
                        r_array.append_option(match s_l {
                            (Some(s), Some(l)) => Some(instr(s, &p, l, &ss)),
                            _ => None,
                        });
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #0110
            (
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Array(p_series),
                DataColumn::Array(l_series),
                DataColumn::Constant(DataValue::String(ss), _),
            ) => {
                if let (Some(s), Some(ss)) = (s, ss) {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for p_l in izip!(p_series.i64()?, l_series.i64()?) {
                        r_array.append_option(match p_l {
                            (Some(p), Some(l)) => Some(instr(&s, p, l, &ss)),
                            _ => None,
                        });
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #1110
            (
                DataColumn::Array(s_series),
                DataColumn::Array(p_series),
                DataColumn::Array(l_series),
                DataColumn::Constant(DataValue::String(ss), _),
            ) => {
                if let Some(ss) = ss {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for s_p_l in izip!(s_series.string()?, p_series.i64()?, l_series.i64()?) {
                        r_array.append_option(match s_p_l {
                            (Some(s), Some(p), Some(l)) => Some(instr(&s, p, l, &ss)),
                            _ => None,
                        });
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #0001
            (
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::Int64(p), _),
                DataColumn::Constant(DataValue::Int64(l), _),
                DataColumn::Array(ss_series),
            ) => {
                if let (Some(s), Some(p), Some(l)) = (s, p, l) {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for oss in ss_series.string()? {
                        r_array.append_option(oss.map(|ss| instr(&s, &p, &l, ss)));
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #1001
            (
                DataColumn::Array(s_series),
                DataColumn::Constant(DataValue::Int64(p), _),
                DataColumn::Constant(DataValue::Int64(l), _),
                DataColumn::Array(ss_series),
            ) => {
                if let (Some(p), Some(l)) = (p, l) {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for s_ss in izip!(s_series.string()?, ss_series.string()?) {
                        r_array.append_option(match s_ss {
                            (Some(s), Some(ss)) => Some(instr(s, &p, &l, ss)),
                            _ => None,
                        });
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #0101
            (
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Array(p_series),
                DataColumn::Constant(DataValue::Int64(l), _),
                DataColumn::Array(ss_series),
            ) => {
                if let (Some(s), Some(l)) = (s, l) {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for p_ss in izip!(p_series.i64()?, ss_series.string()?) {
                        r_array.append_option(match p_ss {
                            (Some(p), Some(ss)) => Some(instr(&s, p, &l, ss)),
                            _ => None,
                        });
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #1101
            (
                DataColumn::Array(s_series),
                DataColumn::Array(p_series),
                DataColumn::Constant(DataValue::Int64(l), _),
                DataColumn::Array(ss_series),
            ) => {
                if let Some(l) = l {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for s_p_ss in izip!(s_series.string()?, p_series.i64()?, ss_series.string()?) {
                        r_array.append_option(match s_p_ss {
                            (Some(s), Some(p), Some(ss)) => Some(instr(s, p, &l, ss)),
                            _ => None,
                        });
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #0011
            (
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::Int64(p), _),
                DataColumn::Array(l_series),
                DataColumn::Array(ss_series),
            ) => {
                if let (Some(s), Some(p)) = (s, p) {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for l_ss in izip!(l_series.i64()?, ss_series.string()?) {
                        r_array.append_option(match l_ss {
                            (Some(l), Some(ss)) => Some(instr(&s, &p, l, ss)),
                            _ => None,
                        });
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #1011
            (
                DataColumn::Array(s_series),
                DataColumn::Constant(DataValue::Int64(p), _),
                DataColumn::Array(l_series),
                DataColumn::Array(ss_series),
            ) => {
                if let Some(p) = p {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for s_l_ss in izip!(s_series.string()?, l_series.i64()?, ss_series.string()?) {
                        r_array.append_option(match s_l_ss {
                            (Some(s), Some(l), Some(ss)) => Some(instr(s, &p, l, ss)),
                            _ => None,
                        });
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #0111
            (
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Array(p_series),
                DataColumn::Array(l_series),
                DataColumn::Array(ss_series),
            ) => {
                if let Some(s) = s {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for p_l_ss in izip!(p_series.i64()?, l_series.i64()?, ss_series.string()?) {
                        r_array.append_option(match p_l_ss {
                            (Some(p), Some(l), Some(ss)) => Some(instr(&s, p, l, ss)),
                            _ => None,
                        });
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #1111
            (
                DataColumn::Array(s_series),
                DataColumn::Array(p_series),
                DataColumn::Array(l_series),
                DataColumn::Array(ss_series),
            ) => {
                let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                for s_p_l_ss in izip!(
                    s_series.string()?,
                    p_series.i64()?,
                    l_series.i64()?,
                    ss_series.string()?
                ) {
                    r_array.append_option(match s_p_l_ss {
                        (Some(s), Some(p), Some(l), Some(ss)) => Some(instr(s, p, l, ss)),
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

impl fmt::Display for InsertFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

#[inline]
fn instr<'a>(srcstr: &'a [u8], pos: &i64, len: &i64, substr: &'a [u8]) -> Vec<u8> {
    let sl = srcstr.len() as i64;
    if *pos < 1 || *pos > sl {
        return srcstr.to_vec();
    }
    let p = *pos as usize - 1;
    if *len < 0 || *pos + *len >= sl {
        return [&srcstr[0..p], substr].concat();
    }
    let l = *len as usize;
    return [&srcstr[0..p], substr, &srcstr[p + l..]].concat();
}
