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

use common_arrow::arrow::array::*;
use common_arrow::arrow::buffer::MutableBuffer;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use itertools::izip;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct ExportSetFunction {
    display_name: String,
}

impl ExportSetFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(3, 5),
        )
    }
}

impl Function for ExportSetFunction {
    fn name(&self) -> &str {
        &*self.display_name
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
        if args.len() > 3
            && !args[3].is_integer()
            && args[3] != DataType::String
            && args[3] != DataType::Null
        {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string or null, but got {}",
                args[3]
            )));
        }
        if args.len() > 4
            && !args[4].is_integer()
            && args[4] != DataType::String
            && args[4] != DataType::Null
        {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string or null, but got {}",
                args[4]
            )));
        }

        Ok(DataType::String)
    }

    fn eval(&self, columns: &DataColumnsWithField, r: usize) -> Result<DataColumn> {
        let b_column = columns[0].column().cast_with_type(&DataType::UInt64)?;
        let e_column = columns[1].column().cast_with_type(&DataType::String)?;
        let d_column = columns[2].column().cast_with_type(&DataType::String)?;
        let s_column = if columns.len() > 3 {
            columns[3].column().cast_with_type(&DataType::String)?
        } else {
            DataColumn::Constant(DataValue::String(Some(vec![44])), r)
        };
        let n_column = if columns.len() > 4 {
            columns[4].column().cast_with_type(&DataType::UInt64)?
        } else {
            DataColumn::Constant(DataValue::UInt64(Some(64)), r)
        };

        match (b_column, e_column, d_column, s_column, n_column) {
            (
                DataColumn::Constant(DataValue::UInt64(b), _),
                DataColumn::Constant(DataValue::String(e), _),
                DataColumn::Constant(DataValue::String(d), _),
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::UInt64(n), _),
            ) => {
                if let (Some(b), Some(e), Some(d), Some(s), Some(n)) = (b, e, d, s, n) {
                    let n = std::cmp::min(n, 64) as usize;
                    let buffer = &mut vec![0; len(&e, &d, &s, &n)][..];
                    let l = export_set(&b, &e, &d, &s, &n, buffer);
                    Ok(DataColumn::Constant(
                        DataValue::String(Some(buffer[0..l].to_owned())),
                        r,
                    ))
                } else {
                    Ok(DataColumn::Constant(DataValue::Null, r))
                }
            }
            (
                DataColumn::Constant(DataValue::UInt64(b), _),
                DataColumn::Array(e),
                DataColumn::Constant(DataValue::String(d), _),
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::UInt64(n), _),
            ) => {
                if let (Some(b), Some(d), Some(s), Some(n)) = (b, d, s, n) {
                    let n = std::cmp::min(n, 64) as usize;
                    let e = e.string()?;
                    let validity = e.inner().validity().cloned();
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.inner().values().len(), r * d.len()) + s.len() * r) * n,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for e in e.into_no_null_iter() {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set(&b, e, &d, &s, &n, bytes);
                            offsets.push(offset as i64);
                        }
                        values.set_len(offset);
                        values.shrink_to_fit();
                        let array = BinaryArray::<i64>::from_data_unchecked(
                            BinaryArray::<i64>::default_data_type(),
                            offsets.into(),
                            values.into(),
                            validity,
                        );
                        Ok(DataColumn::from(DFStringArray::from_arrow_array(&array)))
                    }
                } else {
                    Ok(DataColumn::Constant(DataValue::Null, r))
                }
            }
            (
                DataColumn::Constant(DataValue::UInt64(b), _),
                DataColumn::Constant(DataValue::String(e), _),
                DataColumn::Array(d),
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::UInt64(n), _),
            ) => {
                if let (Some(b), Some(e), Some(s), Some(n)) = (b, e, s, n) {
                    let n = std::cmp::min(n, 64) as usize;
                    let d = d.string()?;
                    let validity = d.inner().validity().cloned();
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.len() * r, d.inner().values().len()) + s.len() * r) * n,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for d in d.into_no_null_iter() {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set(&b, &e, d, &s, &n, bytes);
                            offsets.push(offset as i64);
                        }
                        values.set_len(offset);
                        values.shrink_to_fit();
                        let array = BinaryArray::<i64>::from_data_unchecked(
                            BinaryArray::<i64>::default_data_type(),
                            offsets.into(),
                            values.into(),
                            validity,
                        );
                        Ok(DataColumn::from(DFStringArray::from_arrow_array(&array)))
                    }
                } else {
                    Ok(DataColumn::Constant(DataValue::Null, r))
                }
            }
            (
                DataColumn::Constant(DataValue::UInt64(b), _),
                DataColumn::Array(e),
                DataColumn::Array(d),
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::UInt64(n), _),
            ) => {
                if let (Some(b), Some(s), Some(n)) = (b, s, n) {
                    let n = std::cmp::min(n, 64) as usize;
                    let e = e.string()?;
                    let d = d.string()?;
                    let validity = combine_validities(e.inner().validity(), d.inner().validity());
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.inner().values().len(), d.inner().values().len())
                            + s.len() * r)
                            * n,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (e, d) in izip!(e.into_no_null_iter(), d.into_no_null_iter()) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set(&b, e, d, &s, &n, bytes);
                            offsets.push(offset as i64);
                        }
                        values.set_len(offset);
                        values.shrink_to_fit();
                        let array = BinaryArray::<i64>::from_data_unchecked(
                            BinaryArray::<i64>::default_data_type(),
                            offsets.into(),
                            values.into(),
                            validity,
                        );
                        Ok(DataColumn::from(DFStringArray::from_arrow_array(&array)))
                    }
                } else {
                    Ok(DataColumn::Constant(DataValue::Null, r))
                }
            }
            _ => Err(ErrorCode::IllegalDataType(
                "Expected args a const, an expression, an expression, a const and a const",
            )),
        }
    }
}

impl fmt::Display for ExportSetFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

#[inline]
fn export_set<'a>(
    bits: &u64,
    on: &'a [u8],
    off: &'a [u8],
    sep: &'a [u8],
    n: &usize,
    buffer: &mut [u8],
) -> usize {
    let off_len = off.len();
    let on_len = on.len();
    let sep_len = sep.len();

    let mut offset = 0;
    for n in 0..*n {
        if n != 0 {
            let buf = &mut buffer[offset..offset + sep_len];
            buf.copy_from_slice(sep);
            offset += sep_len;
        }
        if (bits >> n & 1) == 0 {
            let buf = &mut buffer[offset..offset + off_len];
            buf.copy_from_slice(off);
            offset += off_len;
        } else {
            let buf = &mut buffer[offset..offset + on_len];
            buf.copy_from_slice(on);
            offset += on_len;
        }
    }
    offset
}

#[inline]
fn len<'a>(on: &'a [u8], off: &'a [u8], sep: &'a [u8], n: &usize) -> usize {
    let off_len = off.len();
    let on_len = on.len();
    let sep_len = sep.len();

    if off_len > on_len {
        (off_len + sep_len) * (*n as usize) - sep_len
    } else {
        (on_len + sep_len) * (*n as usize) - sep_len
    }
}
