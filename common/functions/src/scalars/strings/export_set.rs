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
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }
}

impl Function for ExportSetFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        Some((3, 5))
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

        let r_column: DataColumn = match (b_column, e_column, d_column, s_column, n_column) {
            // #00000
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
                    DataColumn::Constant(DataValue::String(Some(buffer[0..l].to_owned())), r)
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #10000
            (
                DataColumn::Array(b),
                DataColumn::Constant(DataValue::String(e), _),
                DataColumn::Constant(DataValue::String(d), _),
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::UInt64(n), _),
            ) => {
                if let (Some(e), Some(d), Some(s), Some(n)) = (e, d, s, n) {
                    let n = std::cmp::min(n, 64) as usize;
                    let b = b.u64()?.inner();
                    let validity = b.validity().cloned();
                    let mut values: MutableBuffer<u8> =
                        MutableBuffer::with_capacity(r * len(&e, &d, &s, &n));
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for b in b.values().iter() {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set(b, &e, &d, &s, &n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #01000
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #11000
            (
                DataColumn::Array(b),
                DataColumn::Array(e),
                DataColumn::Constant(DataValue::String(d), _),
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::UInt64(n), _),
            ) => {
                if let (Some(d), Some(s), Some(n)) = (d, s, n) {
                    let n = std::cmp::min(n, 64) as usize;
                    let b = b.u64()?;
                    let e = e.string()?;
                    let validity = combine_validities(b.inner().validity(), e.inner().validity());
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.inner().values().len(), r * d.len()) + s.len() * r) * n,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (b, e) in izip!(b.into_no_null_iter(), e.into_no_null_iter()) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set(b, e, &d, &s, &n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #00100
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #10100
            (
                DataColumn::Array(b),
                DataColumn::Constant(DataValue::String(e), _),
                DataColumn::Array(d),
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::UInt64(n), _),
            ) => {
                if let (Some(e), Some(s), Some(n)) = (e, s, n) {
                    let n = std::cmp::min(n, 64) as usize;
                    let b = b.u64()?;
                    let d = d.string()?;
                    let validity = combine_validities(b.inner().validity(), d.inner().validity());
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.len() * r, d.inner().values().len()) + s.len() * r) * n,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (b, d) in izip!(b.into_no_null_iter(), d.into_no_null_iter()) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set(b, &e, d, &s, &n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #01100
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #11100
            (
                DataColumn::Array(b),
                DataColumn::Array(e),
                DataColumn::Array(d),
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::UInt64(n), _),
            ) => {
                if let (Some(s), Some(n)) = (s, n) {
                    let n = std::cmp::min(n, 64) as usize;
                    let b = b.u64()?;
                    let e = e.string()?;
                    let d = d.string()?;
                    let validity = combine_validities(
                        b.inner().validity(),
                        combine_validities(e.inner().validity(), d.inner().validity()).as_ref(),
                    );
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.inner().values().len(), d.inner().values().len())
                            + s.len() * r)
                            * n,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (b, e, d) in izip!(
                            b.into_no_null_iter(),
                            e.into_no_null_iter(),
                            d.into_no_null_iter()
                        ) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set(b, e, d, &s, &n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #00010
            (
                DataColumn::Constant(DataValue::UInt64(b), _),
                DataColumn::Constant(DataValue::String(e), _),
                DataColumn::Constant(DataValue::String(d), _),
                DataColumn::Array(s),
                DataColumn::Constant(DataValue::UInt64(n), _),
            ) => {
                if let (Some(b), Some(e), Some(d), Some(n)) = (b, e, d, n) {
                    let n = std::cmp::min(n, 64) as usize;
                    let s = s.string()?;
                    let validity = s.inner().validity().cloned();
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.len(), d.len()) * r + s.inner().values().len()) * n,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for s in s.into_no_null_iter() {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set(&b, &e, &d, s, &n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #10010
            (
                DataColumn::Array(b),
                DataColumn::Constant(DataValue::String(e), _),
                DataColumn::Constant(DataValue::String(d), _),
                DataColumn::Array(s),
                DataColumn::Constant(DataValue::UInt64(n), _),
            ) => {
                if let (Some(e), Some(d), Some(n)) = (e, d, n) {
                    let n = std::cmp::min(n, 64) as usize;
                    let b = b.u64()?;
                    let s = s.string()?;
                    let validity = combine_validities(b.inner().validity(), s.inner().validity());
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.len(), d.len()) * r + s.inner().values().len()) * n,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (b, s) in izip!(b.into_no_null_iter(), s.into_no_null_iter()) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set(b, &e, &d, s, &n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #01010
            (
                DataColumn::Constant(DataValue::UInt64(b), _),
                DataColumn::Array(e),
                DataColumn::Constant(DataValue::String(d), _),
                DataColumn::Array(s),
                DataColumn::Constant(DataValue::UInt64(n), _),
            ) => {
                if let (Some(b), Some(d), Some(n)) = (b, d, n) {
                    let n = std::cmp::min(n, 64) as usize;
                    let e = e.string()?;
                    let s = s.string()?;
                    let validity = combine_validities(e.inner().validity(), s.inner().validity());
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.inner().values().len(), d.len() * r)
                            + s.inner().values().len())
                            * n,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (e, s) in izip!(e.into_no_null_iter(), s.into_no_null_iter()) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set(&b, e, &d, s, &n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #11010
            (
                DataColumn::Array(b),
                DataColumn::Array(e),
                DataColumn::Constant(DataValue::String(d), _),
                DataColumn::Array(s),
                DataColumn::Constant(DataValue::UInt64(n), _),
            ) => {
                if let (Some(d), Some(n)) = (d, n) {
                    let n = std::cmp::min(n, 64) as usize;
                    let b = b.u64()?;
                    let e = e.string()?;
                    let s = s.string()?;
                    let validity = combine_validities(
                        b.inner().validity(),
                        combine_validities(e.inner().validity(), s.inner().validity()).as_ref(),
                    );
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.inner().values().len(), d.len() * r)
                            + s.inner().values().len())
                            * n,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (b, e, s) in izip!(
                            b.into_no_null_iter(),
                            e.into_no_null_iter(),
                            s.into_no_null_iter()
                        ) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set(b, e, &d, s, &n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #00110
            (
                DataColumn::Constant(DataValue::UInt64(b), _),
                DataColumn::Constant(DataValue::String(e), _),
                DataColumn::Array(d),
                DataColumn::Array(s),
                DataColumn::Constant(DataValue::UInt64(n), _),
            ) => {
                if let (Some(b), Some(e), Some(n)) = (b, e, n) {
                    let n = std::cmp::min(n, 64) as usize;
                    let d = d.string()?;
                    let s = s.string()?;
                    let validity = combine_validities(d.inner().validity(), s.inner().validity());
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.len() * r, d.inner().values().len())
                            + s.inner().values().len())
                            * n,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (d, s) in izip!(d.into_no_null_iter(), s.into_no_null_iter()) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set(&b, &e, d, s, &n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #10110
            (
                DataColumn::Array(b),
                DataColumn::Constant(DataValue::String(e), _),
                DataColumn::Array(d),
                DataColumn::Array(s),
                DataColumn::Constant(DataValue::UInt64(n), _),
            ) => {
                if let (Some(e), Some(n)) = (e, n) {
                    let n = std::cmp::min(n, 64) as usize;
                    let b = b.u64()?;
                    let d = d.string()?;
                    let s = s.string()?;
                    let validity = combine_validities(
                        b.inner().validity(),
                        combine_validities(d.inner().validity(), s.inner().validity()).as_ref(),
                    );
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.len() * r, d.inner().values().len())
                            + s.inner().values().len())
                            * n,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (b, d, s) in izip!(
                            b.into_no_null_iter(),
                            d.into_no_null_iter(),
                            s.into_no_null_iter()
                        ) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set(b, &e, d, s, &n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #01110
            (
                DataColumn::Constant(DataValue::UInt64(b), _),
                DataColumn::Array(e),
                DataColumn::Array(d),
                DataColumn::Array(s),
                DataColumn::Constant(DataValue::UInt64(n), _),
            ) => {
                if let (Some(b), Some(n)) = (b, n) {
                    let n = std::cmp::min(n, 64) as usize;
                    let e = e.string()?;
                    let d = d.string()?;
                    let s = s.string()?;
                    let validity = combine_validities(
                        e.inner().validity(),
                        combine_validities(d.inner().validity(), s.inner().validity()).as_ref(),
                    );
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.inner().values().len(), d.inner().values().len())
                            + s.inner().values().len())
                            * n,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (e, d, s) in izip!(
                            e.into_no_null_iter(),
                            d.into_no_null_iter(),
                            s.into_no_null_iter()
                        ) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set(&b, e, d, s, &n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #11110
            (
                DataColumn::Array(b),
                DataColumn::Array(e),
                DataColumn::Array(d),
                DataColumn::Array(s),
                DataColumn::Constant(DataValue::UInt64(n), _),
            ) => {
                if let Some(n) = n {
                    let n = std::cmp::min(n, 64) as usize;
                    let b = b.u64()?;
                    let e = e.string()?;
                    let d = d.string()?;
                    let s = s.string()?;
                    let validity = combine_validities(
                        combine_validities(b.inner().validity(), e.inner().validity()).as_ref(),
                        combine_validities(d.inner().validity(), s.inner().validity()).as_ref(),
                    );
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.inner().values().len(), d.inner().values().len())
                            + s.inner().values().len())
                            * n,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (b, e, d, s) in izip!(
                            b.into_no_null_iter(),
                            e.into_no_null_iter(),
                            d.into_no_null_iter(),
                            s.into_no_null_iter()
                        ) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set(b, e, d, s, &n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #00001
            (
                DataColumn::Constant(DataValue::UInt64(b), _),
                DataColumn::Constant(DataValue::String(e), _),
                DataColumn::Constant(DataValue::String(d), _),
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Array(n),
            ) => {
                if let (Some(b), Some(e), Some(d), Some(s)) = (b, e, d, s) {
                    let n = n.u64()?;
                    let validity = n.inner().validity().cloned();
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.len(), d.len()) + s.len()) * r * 64,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for n in n.into_no_null_iter() {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set_n(&b, &e, &d, &s, n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #10001
            (
                DataColumn::Array(b),
                DataColumn::Constant(DataValue::String(e), _),
                DataColumn::Constant(DataValue::String(d), _),
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Array(n),
            ) => {
                if let (Some(e), Some(d), Some(s)) = (e, d, s) {
                    let b = b.u64()?;
                    let n = n.u64()?;
                    let validity = combine_validities(b.inner().validity(), n.inner().validity());
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.len(), d.len()) + s.len()) * r * 64,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (b, n) in izip!(b.into_no_null_iter(), n.into_no_null_iter()) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set_n(b, &e, &d, &s, n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #01001
            (
                DataColumn::Constant(DataValue::UInt64(b), _),
                DataColumn::Array(e),
                DataColumn::Constant(DataValue::String(d), _),
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Array(n),
            ) => {
                if let (Some(b), Some(d), Some(s)) = (b, d, s) {
                    let e = e.string()?;
                    let n = n.u64()?;
                    let validity = combine_validities(e.inner().validity(), n.inner().validity());
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.inner().values().len(), r * d.len()) + s.len() * r) * 64,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (e, n) in izip!(e.into_no_null_iter(), n.into_no_null_iter()) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set_n(&b, e, &d, &s, n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #11001
            (
                DataColumn::Array(b),
                DataColumn::Array(e),
                DataColumn::Constant(DataValue::String(d), _),
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Array(n),
            ) => {
                if let (Some(d), Some(s)) = (d, s) {
                    let b = b.u64()?;
                    let e = e.string()?;
                    let n = n.u64()?;
                    let validity = combine_validities(
                        b.inner().validity(),
                        combine_validities(e.inner().validity(), n.inner().validity()).as_ref(),
                    );
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.inner().values().len(), r * d.len()) + s.len() * r) * 64,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (b, e, n) in izip!(
                            b.into_no_null_iter(),
                            e.into_no_null_iter(),
                            n.into_no_null_iter()
                        ) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set_n(b, e, &d, &s, n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #00101
            (
                DataColumn::Constant(DataValue::UInt64(b), _),
                DataColumn::Constant(DataValue::String(e), _),
                DataColumn::Array(d),
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Array(n),
            ) => {
                if let (Some(b), Some(e), Some(s)) = (b, e, s) {
                    let d = d.string()?;
                    let n = n.u64()?;
                    let validity = combine_validities(d.inner().validity(), n.inner().validity());
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.len() * r, d.inner().values().len()) + s.len() * r) * 64,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (d, n) in izip!(d.into_no_null_iter(), n.into_no_null_iter()) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set_n(&b, &e, d, &s, n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #10101
            (
                DataColumn::Array(b),
                DataColumn::Constant(DataValue::String(e), _),
                DataColumn::Array(d),
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Array(n),
            ) => {
                if let (Some(e), Some(s)) = (e, s) {
                    let b = b.u64()?;
                    let d = d.string()?;
                    let n = n.u64()?;
                    let validity = combine_validities(
                        b.inner().validity(),
                        combine_validities(d.inner().validity(), n.inner().validity()).as_ref(),
                    );
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.len() * r, d.inner().values().len()) + s.len() * r) * 64,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (b, d, n) in izip!(
                            b.into_no_null_iter(),
                            d.into_no_null_iter(),
                            n.into_no_null_iter()
                        ) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set_n(b, &e, d, &s, n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #01101
            (
                DataColumn::Constant(DataValue::UInt64(b), _),
                DataColumn::Array(e),
                DataColumn::Array(d),
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Array(n),
            ) => {
                if let (Some(b), Some(s)) = (b, s) {
                    let e = e.string()?;
                    let d = d.string()?;
                    let n = n.u64()?;
                    let validity = combine_validities(
                        e.inner().validity(),
                        combine_validities(d.inner().validity(), n.inner().validity()).as_ref(),
                    );
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.inner().values().len(), d.inner().values().len())
                            + s.len() * r)
                            * 64,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (e, d, n) in izip!(
                            e.into_no_null_iter(),
                            d.into_no_null_iter(),
                            n.into_no_null_iter()
                        ) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set_n(&b, e, d, &s, n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #11101
            (
                DataColumn::Array(b),
                DataColumn::Array(e),
                DataColumn::Array(d),
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Array(n),
            ) => {
                if let Some(s) = s {
                    let b = b.u64()?;
                    let e = e.string()?;
                    let d = d.string()?;
                    let n = n.u64()?;
                    let validity = combine_validities(
                        combine_validities(b.inner().validity(), e.inner().validity()).as_ref(),
                        combine_validities(d.inner().validity(), n.inner().validity()).as_ref(),
                    );
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.inner().values().len(), d.inner().values().len())
                            + s.len() * r)
                            * 64,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (b, e, d, n) in izip!(
                            b.into_no_null_iter(),
                            e.into_no_null_iter(),
                            d.into_no_null_iter(),
                            n.into_no_null_iter()
                        ) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set_n(b, e, d, &s, n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #00011
            (
                DataColumn::Constant(DataValue::UInt64(b), _),
                DataColumn::Constant(DataValue::String(e), _),
                DataColumn::Constant(DataValue::String(d), _),
                DataColumn::Array(s),
                DataColumn::Array(n),
            ) => {
                if let (Some(b), Some(e), Some(d)) = (b, e, d) {
                    let s = s.string()?;
                    let n = n.u64()?;
                    let validity = combine_validities(s.inner().validity(), n.inner().validity());
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.len(), d.len()) * r + s.inner().values().len()) * 64,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (s, n) in izip!(s.into_no_null_iter(), n.into_no_null_iter()) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set_n(&b, &e, &d, s, n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #10011
            (
                DataColumn::Array(b),
                DataColumn::Constant(DataValue::String(e), _),
                DataColumn::Constant(DataValue::String(d), _),
                DataColumn::Array(s),
                DataColumn::Array(n),
            ) => {
                if let (Some(e), Some(d)) = (e, d) {
                    let b = b.u64()?;
                    let s = s.string()?;
                    let n = n.u64()?;
                    let validity = combine_validities(
                        b.inner().validity(),
                        combine_validities(s.inner().validity(), n.inner().validity()).as_ref(),
                    );
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.len(), d.len()) * r + s.inner().values().len()) * 64,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (b, s, n) in izip!(
                            b.into_no_null_iter(),
                            s.into_no_null_iter(),
                            n.into_no_null_iter()
                        ) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set_n(b, &e, &d, s, n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #01011
            (
                DataColumn::Constant(DataValue::UInt64(b), _),
                DataColumn::Array(e),
                DataColumn::Constant(DataValue::String(d), _),
                DataColumn::Array(s),
                DataColumn::Array(n),
            ) => {
                if let (Some(b), Some(d)) = (b, d) {
                    let e = e.string()?;
                    let s = s.string()?;
                    let n = n.u64()?;
                    let validity = combine_validities(
                        e.inner().validity(),
                        combine_validities(s.inner().validity(), n.inner().validity()).as_ref(),
                    );
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.inner().values().len(), d.len() * r)
                            + s.inner().values().len())
                            * 64,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (e, s, n) in izip!(
                            e.into_no_null_iter(),
                            s.into_no_null_iter(),
                            n.into_no_null_iter()
                        ) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set_n(&b, e, &d, s, n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #11011
            (
                DataColumn::Array(b),
                DataColumn::Array(e),
                DataColumn::Constant(DataValue::String(d), _),
                DataColumn::Array(s),
                DataColumn::Array(n),
            ) => {
                if let Some(d) = d {
                    let b = b.u64()?;
                    let e = e.string()?;
                    let s = s.string()?;
                    let n = n.u64()?;
                    let validity = combine_validities(
                        combine_validities(b.inner().validity(), e.inner().validity()).as_ref(),
                        combine_validities(s.inner().validity(), n.inner().validity()).as_ref(),
                    );
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.inner().values().len(), d.len() * r)
                            + s.inner().values().len())
                            * 64,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (b, e, s, n) in izip!(
                            b.into_no_null_iter(),
                            e.into_no_null_iter(),
                            s.into_no_null_iter(),
                            n.into_no_null_iter()
                        ) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set_n(b, e, &d, s, n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #00111
            (
                DataColumn::Constant(DataValue::UInt64(b), _),
                DataColumn::Constant(DataValue::String(e), _),
                DataColumn::Array(d),
                DataColumn::Array(s),
                DataColumn::Array(n),
            ) => {
                if let (Some(b), Some(e)) = (b, e) {
                    let d = d.string()?;
                    let s = s.string()?;
                    let n = n.u64()?;
                    let validity = combine_validities(
                        d.inner().validity(),
                        combine_validities(s.inner().validity(), n.inner().validity()).as_ref(),
                    );
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.len() * r, d.inner().values().len())
                            + s.inner().values().len())
                            * 64,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (d, s, n) in izip!(
                            d.into_no_null_iter(),
                            s.into_no_null_iter(),
                            n.into_no_null_iter()
                        ) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set_n(&b, &e, d, s, n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #10111
            (
                DataColumn::Array(b),
                DataColumn::Constant(DataValue::String(e), _),
                DataColumn::Array(d),
                DataColumn::Array(s),
                DataColumn::Array(n),
            ) => {
                if let Some(e) = e {
                    let b = b.u64()?;
                    let d = d.string()?;
                    let s = s.string()?;
                    let n = n.u64()?;
                    let validity = combine_validities(
                        combine_validities(b.inner().validity(), d.inner().validity()).as_ref(),
                        combine_validities(s.inner().validity(), n.inner().validity()).as_ref(),
                    );
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.len() * r, d.inner().values().len())
                            + s.inner().values().len())
                            * 64,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (b, d, s, n) in izip!(
                            b.into_no_null_iter(),
                            d.into_no_null_iter(),
                            s.into_no_null_iter(),
                            n.into_no_null_iter()
                        ) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set_n(b, &e, d, s, n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #01111
            (
                DataColumn::Constant(DataValue::UInt64(b), _),
                DataColumn::Array(e),
                DataColumn::Array(d),
                DataColumn::Array(s),
                DataColumn::Array(n),
            ) => {
                if let Some(b) = b {
                    let e = e.string()?;
                    let d = d.string()?;
                    let s = s.string()?;
                    let n = n.u64()?;
                    let validity = combine_validities(
                        combine_validities(e.inner().validity(), d.inner().validity()).as_ref(),
                        combine_validities(s.inner().validity(), n.inner().validity()).as_ref(),
                    );
                    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                        (std::cmp::max(e.inner().values().len(), d.inner().values().len())
                            + s.inner().values().len())
                            * 64,
                    );
                    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                    offsets.push(0);
                    let mut offset: usize = 0;
                    unsafe {
                        for (e, d, s, n) in izip!(
                            e.into_no_null_iter(),
                            d.into_no_null_iter(),
                            s.into_no_null_iter(),
                            n.into_no_null_iter()
                        ) {
                            let bytes = std::slice::from_raw_parts_mut(
                                values.as_mut_ptr().add(offset),
                                values.capacity() - offset,
                            );
                            offset += export_set_n(&b, e, d, s, n, bytes);
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
                        DFStringArray::from_arrow_array(&array).into()
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, r)
                }
            }
            // #11111
            (
                DataColumn::Array(b),
                DataColumn::Array(e),
                DataColumn::Array(d),
                DataColumn::Array(s),
                DataColumn::Array(n),
            ) => {
                let b = b.u64()?;
                let e = e.string()?;
                let d = d.string()?;
                let s = s.string()?;
                let n = n.u64()?;
                let validity = combine_validities(
                    b.inner().validity(),
                    combine_validities(
                        combine_validities(e.inner().validity(), d.inner().validity()).as_ref(),
                        combine_validities(s.inner().validity(), n.inner().validity()).as_ref(),
                    )
                    .as_ref(),
                );
                let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(
                    (std::cmp::max(e.inner().values().len(), d.inner().values().len())
                        + s.inner().values().len())
                        * 64,
                );
                let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(r + 1);
                offsets.push(0);
                let mut offset: usize = 0;
                unsafe {
                    for (b, e, d, s, n) in izip!(
                        b.into_no_null_iter(),
                        e.into_no_null_iter(),
                        d.into_no_null_iter(),
                        s.into_no_null_iter(),
                        n.into_no_null_iter()
                    ) {
                        let bytes = std::slice::from_raw_parts_mut(
                            values.as_mut_ptr().add(offset),
                            values.capacity() - offset,
                        );
                        offset += export_set_n(b, e, d, s, n, bytes);
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
                    DFStringArray::from_arrow_array(&array).into()
                }
            }
            _ => DataColumn::Constant(DataValue::Null, r),
        };
        Ok(r_column)
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
fn export_set_n<'a>(
    bits: &u64,
    on: &'a [u8],
    off: &'a [u8],
    sep: &'a [u8],
    n: &u64,
    buffer: &mut [u8],
) -> usize {
    let n = std::cmp::min(n, &64);
    let n = *n as usize;
    export_set(bits, on, off, sep, &n, buffer)
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
