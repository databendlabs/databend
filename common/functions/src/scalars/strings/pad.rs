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
use std::marker::PhantomData;

use common_datavalues::prelude::*;
use common_datavalues::DataTypeAndNullable;
use common_exception::ErrorCode;
use common_exception::Result;
use itertools::izip;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

pub type LeftPadFunction = PadFunction<LeftPad>;
pub type RightPadFunction = PadFunction<RightPad>;

pub trait PadOperator: Send + Sync + Clone + Default + 'static {
    fn apply<'a>(&'a mut self, str: &'a [u8], len: &u64, pad: &'a [u8]) -> &'a [u8];
}

#[derive(Clone, Default)]
pub struct LeftPad {
    buff: Vec<u8>,
}

impl PadOperator for LeftPad {
    #[inline]
    fn apply<'a>(&'a mut self, str: &'a [u8], len: &u64, pad: &'a [u8]) -> &'a [u8] {
        self.buff.clear();
        if *len != 0 {
            let l = *len as usize;
            if l > str.len() {
                let l = l - str.len();
                while self.buff.len() < l {
                    if self.buff.len() + pad.len() <= l {
                        self.buff.extend_from_slice(pad);
                    } else {
                        self.buff.extend_from_slice(&pad[0..l - self.buff.len()])
                    }
                }
                self.buff.extend_from_slice(str);
            } else {
                self.buff.extend_from_slice(&str[0..l]);
            }
        }
        &self.buff
    }
}

#[derive(Clone, Default)]
pub struct RightPad {
    buff: Vec<u8>,
}

impl PadOperator for RightPad {
    #[inline]
    fn apply<'a>(&'a mut self, str: &'a [u8], len: &u64, pad: &'a [u8]) -> &'a [u8] {
        self.buff.clear();
        if *len != 0 {
            let l = *len as usize;
            if l > str.len() {
                self.buff.extend_from_slice(str);
                while self.buff.len() < l {
                    if self.buff.len() + pad.len() <= l {
                        self.buff.extend_from_slice(pad);
                    } else {
                        self.buff.extend_from_slice(&pad[0..l - self.buff.len()])
                    }
                }
            } else {
                self.buff.extend_from_slice(&str[0..l]);
            }
        }
        &self.buff
    }
}

#[derive(Clone)]
pub struct PadFunction<T> {
    display_name: String,
    _marker: PhantomData<T>,
}

impl<T: PadOperator> PadFunction<T> {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
            _marker: PhantomData,
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(3))
    }
}

impl<T: PadOperator> Function for PadFunction<T> {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[DataTypeAndNullable]) -> Result<DataTypeAndNullable> {
        if !args[0].is_numeric() && !args[0].is_string() && !args[0].is_null() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string or null, but got {}",
                args[0]
            )));
        }
        if !args[1].is_unsigned_integer() && !args[1].is_string() && !args[1].is_null() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string or null, but got {}",
                args[1]
            )));
        }
        if !args[2].is_numeric() && !args[2].is_string() && !args[2].is_null() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string or null, but got {}",
                args[2]
            )));
        }

        let nullable = args.iter().any(|arg| arg.is_nullable());
        let dt = DataType::String(nullable);
        Ok(DataTypeAndNullable::create(&dt, nullable))
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let mut op = T::default();

        let s_nullable = columns[0].field().is_nullable();
        let s_column = columns[0]
            .column()
            .cast_with_type(&DataType::String(s_nullable))?;

        let l_nullable = columns[1].field().is_nullable();
        let l_column = columns[1]
            .column()
            .cast_with_type(&DataType::UInt64(l_nullable))?;

        let p_nullable = columns[2].field().is_nullable();
        let p_column = columns[2]
            .column()
            .cast_with_type(&DataType::String(p_nullable))?;

        let r_column: DataColumn = match (s_column, l_column, p_column) {
            // #000
            (
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::UInt64(l), _),
                DataColumn::Constant(DataValue::String(p), _),
            ) => {
                if let (Some(s), Some(l), Some(p)) = (s, l, p) {
                    DataColumn::Constant(
                        DataValue::String(Some(op.apply(&s, &l, &p).to_owned())),
                        input_rows,
                    )
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #100
            (
                DataColumn::Array(s_series),
                DataColumn::Constant(DataValue::UInt64(l), _),
                DataColumn::Constant(DataValue::String(p), _),
            ) => {
                if let (Some(l), Some(p)) = (l, p) {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for s in s_series.string()? {
                        r_array.append_option(s.map(|s| op.apply(s, &l, &p)));
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #010
            (
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Array(l_series),
                DataColumn::Constant(DataValue::String(p), _),
            ) => {
                if let (Some(s), Some(p)) = (s, p) {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for l in l_series.u64()? {
                        r_array.append_option(l.map(|l| op.apply(&s, l, &p)));
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #110
            (
                DataColumn::Array(s_series),
                DataColumn::Array(l_series),
                DataColumn::Constant(DataValue::String(p), _),
            ) => {
                if let Some(p) = p {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for s_l in izip!(s_series.string()?, l_series.u64()?) {
                        r_array.append_option(match s_l {
                            (Some(s), Some(l)) => Some(op.apply(s, l, &p)),
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
                DataColumn::Constant(DataValue::UInt64(l), _),
                DataColumn::Array(p_series),
            ) => {
                if let (Some(s), Some(l)) = (s, l) {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for p in p_series.string()? {
                        r_array.append_option(p.map(|p| op.apply(&s, &l, p)));
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #101
            (
                DataColumn::Array(s_series),
                DataColumn::Constant(DataValue::UInt64(l), _),
                DataColumn::Array(p_series),
            ) => {
                if let Some(l) = l {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for s_p in izip!(s_series.string()?, p_series.string()?) {
                        r_array.append_option(match s_p {
                            (Some(s), Some(p)) => Some(op.apply(s, &l, p)),
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
                DataColumn::Array(l_series),
                DataColumn::Array(p_series),
            ) => {
                if let Some(s) = s {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for l_p in izip!(l_series.u64()?, p_series.string()?) {
                        r_array.append_option(match l_p {
                            (Some(l), Some(p)) => Some(op.apply(&s, l, p)),
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
                DataColumn::Array(l_series),
                DataColumn::Array(p_series),
            ) => {
                let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                for l_p in izip!(s_series.string()?, l_series.u64()?, p_series.string()?) {
                    r_array.append_option(match l_p {
                        (Some(s), Some(l), Some(p)) => Some(op.apply(s, l, p)),
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

impl<F> fmt::Display for PadFunction<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.display_name)
    }
}
