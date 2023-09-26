// Copyright 2021 Datafuse Labs
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

use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::buffer::Buffer;
use common_exception::Result;

use crate::types::array::ArrayColumn;
use crate::types::array::ArrayColumnBuilder;
use crate::types::bitmap::BitmapType;
use crate::types::decimal::DecimalColumn;
use crate::types::map::KvColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::string::StringColumn;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::ArrayType;
use crate::types::BooleanType;
use crate::types::MapType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::ValueType;
use crate::types::VariantType;
use crate::with_decimal_type;
use crate::with_number_mapped_type;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::Value;

pub const BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];

impl DataBlock {
    pub fn take<I>(
        &self,
        indices: &[I],
        string_items_buf: &mut Option<Vec<(u64, usize)>>,
    ) -> Result<Self>
    where
        I: common_arrow::arrow::types::Index,
    {
        if indices.is_empty() {
            return Ok(self.slice(0..0));
        }

        let after_columns = self
            .columns()
            .iter()
            .map(|entry| match &entry.value {
                Value::Scalar(s) => {
                    BlockEntry::new(entry.data_type.clone(), Value::Scalar(s.clone()))
                }
                Value::Column(c) => BlockEntry::new(
                    entry.data_type.clone(),
                    Value::Column(Column::take(c, indices, string_items_buf)),
                ),
            })
            .collect();

        Ok(DataBlock::new_with_meta(
            after_columns,
            indices.len(),
            self.get_meta().cloned(),
        ))
    }
}

impl Column {
    pub fn take<I>(&self, indices: &[I], string_items_buf: &mut Option<Vec<(u64, usize)>>) -> Self
    where I: common_arrow::arrow::types::Index {
        match self {
            Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {
                self.slice(0..indices.len())
            }
            Column::Number(column) => with_number_mapped_type!(|NUM_TYPE| match column {
                NumberColumn::NUM_TYPE(values) => {
                    let builder = Self::take_primitive_types(values, indices);
                    <NumberType<NUM_TYPE>>::upcast_column(<NumberType<NUM_TYPE>>::column_from_vec(
                        builder,
                        &[],
                    ))
                }
            }),
            Column::Decimal(column) => with_decimal_type!(|DECIMAL_TYPE| match column {
                DecimalColumn::DECIMAL_TYPE(values, size) => {
                    let builder = Self::take_primitive_types(values, indices);
                    Column::Decimal(DecimalColumn::DECIMAL_TYPE(builder.into(), *size))
                }
            }),
            Column::Boolean(bm) => Column::Boolean(Self::take_boolean_types(bm, indices)),
            Column::String(column) => StringType::upcast_column(Self::take_string_types(
                column,
                indices,
                string_items_buf.as_mut(),
            )),
            Column::Timestamp(column) => {
                let builder = Self::take_primitive_types(column, indices);
                let ts = <NumberType<i64>>::upcast_column(<NumberType<i64>>::column_from_vec(
                    builder,
                    &[],
                ))
                .into_number()
                .unwrap()
                .into_int64()
                .unwrap();
                Column::Timestamp(ts)
            }
            Column::Date(column) => {
                let builder = Self::take_primitive_types(column, indices);
                let d = <NumberType<i32>>::upcast_column(<NumberType<i32>>::column_from_vec(
                    builder,
                    &[],
                ))
                .into_number()
                .unwrap()
                .into_int32()
                .unwrap();
                Column::Date(d)
            }
            Column::Array(column) => {
                let mut offsets = Vec::with_capacity(indices.len() + 1);
                offsets.push(0);
                let builder = ColumnBuilder::with_capacity(&column.values.data_type(), self.len());
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::take_value_types::<ArrayType<AnyType>, _>(column, builder, indices)
            }
            Column::Map(column) => {
                let mut offsets = Vec::with_capacity(indices.len() + 1);
                offsets.push(0);
                let builder = ColumnBuilder::from_column(
                    ColumnBuilder::with_capacity(&column.values.data_type(), self.len()).build(),
                );
                let (key_builder, val_builder) = match builder {
                    ColumnBuilder::Tuple(fields) => (fields[0].clone(), fields[1].clone()),
                    _ => unreachable!(),
                };
                let builder = KvColumnBuilder {
                    keys: key_builder,
                    values: val_builder,
                };
                let builder = ArrayColumnBuilder { builder, offsets };
                let column = ArrayColumn::try_downcast(column).unwrap();
                Self::take_value_types::<MapType<AnyType, AnyType>, _>(&column, builder, indices)
            }
            Column::Bitmap(column) => BitmapType::upcast_column(Self::take_string_types(
                column,
                indices,
                string_items_buf.as_mut(),
            )),
            Column::Nullable(c) => {
                let column = c.column.take(indices, string_items_buf);
                let validity = Column::Boolean(Self::take_boolean_types(&c.validity, indices));
                Column::Nullable(Box::new(NullableColumn {
                    column,
                    validity: BooleanType::try_downcast_column(&validity).unwrap(),
                }))
            }
            Column::Tuple(fields) => {
                let fields = fields
                    .iter()
                    .map(|c| c.take(indices, string_items_buf))
                    .collect();
                Column::Tuple(fields)
            }
            Column::Variant(column) => VariantType::upcast_column(Self::take_string_types(
                column,
                indices,
                string_items_buf.as_mut(),
            )),
        }
    }

    pub fn take_primitive_types<T, I>(col: &Buffer<T>, indices: &[I]) -> Vec<T>
    where
        T: Copy,
        I: common_arrow::arrow::types::Index,
    {
        let num_rows = indices.len();
        let mut builder: Vec<T> = Vec::with_capacity(num_rows);
        let ptr = builder.as_mut_ptr();
        let col_ptr = col.as_slice().as_ptr();
        for (i, index) in indices.iter().enumerate() {
            // # Safety
            // `i` must be less than `num_rows`.
            unsafe {
                std::ptr::copy_nonoverlapping(col_ptr.add(index.to_usize()), ptr.add(i), 1);
            }
        }
        // # Safety
        // The capacity of `builder` is `num_rows` and we have added `num_rows` elements to `builder` by `ptr`.
        unsafe { builder.set_len(num_rows) };
        builder
    }

    pub fn take_string_types<'a, I>(
        col: &'a StringColumn,
        indices: &[I],
        string_items_buf: Option<&mut Vec<(u64, usize)>>,
    ) -> StringColumn
    where
        I: common_arrow::arrow::types::Index,
    {
        let num_rows = indices.len();

        let mut items: Option<Vec<(u64, usize)>> = match &string_items_buf {
            Some(string_items_buf) if string_items_buf.capacity() >= num_rows => None,
            _ => Some(Vec::with_capacity(num_rows)),
        };
        let items = match items.is_some() {
            true => items.as_mut().unwrap(),
            false => string_items_buf.unwrap(),
        };

        let mut offsets: Vec<u64> = Vec::with_capacity(num_rows + 1);
        offsets.push(0);
        let items_ptr = items.as_mut_ptr();
        let offsets_ptr = unsafe { offsets.as_mut_ptr().add(1) };
        let col_offset = col.offsets().as_slice();
        let col_data_ptr = col.data().as_slice().as_ptr();
        let mut data_size = 0;
        for (i, index) in indices.iter().enumerate() {
            let start = unsafe { *col_offset.get_unchecked(index.to_usize()) } as usize;
            let len = unsafe { *col_offset.get_unchecked(index.to_usize() + 1) as usize } - start;
            data_size += len as u64;
            // # Safety
            // `i` must be less than the capacity of Vec.
            unsafe {
                std::ptr::write(items_ptr.add(i), (col_data_ptr.add(start) as u64, len));
                std::ptr::write(offsets_ptr.add(i), data_size);
            }
        }
        unsafe {
            items.set_len(num_rows);
            offsets.set_len(num_rows + 1);
        }

        let mut data: Vec<u8> = Vec::with_capacity(data_size as usize);
        let data_ptr = data.as_mut_ptr();
        let mut offset = 0;
        for (str_ptr, len) in items.iter() {
            // # Safety
            // `offset` + `len` < `data_size`.
            unsafe {
                std::ptr::copy_nonoverlapping(*str_ptr as *const u8, data_ptr.add(offset), *len);
            }
            offset += len;
        }
        unsafe { data.set_len(offset) };
        StringColumn::new(data.into(), offsets.into())
    }

    pub fn take_boolean_types<I>(col: &Bitmap, indices: &[I]) -> Bitmap
    where I: common_arrow::arrow::types::Index {
        let num_rows = indices.len();
        let capacity = num_rows.saturating_add(7) / 8;
        let mut builder: Vec<u8> = Vec::with_capacity(capacity);
        let ptr = builder.as_mut_ptr();

        let mut pos = 0;
        let mut unset_bits = 0;
        let mut i = 0;
        let mut value = 0;
        for index in indices.iter() {
            if unsafe { col.get_bit_unchecked(index.to_usize()) } {
                value |= BIT_MASK[i % 8];
            } else {
                unset_bits += 1;
            }
            i += 1;
            if i % 8 == 0 {
                // # Safety
                // `pos` must be less than the capacity of builder.
                unsafe { std::ptr::write(ptr.add(pos), value) };
                pos += 1;
                value = 0;
            }
        }
        if i % 8 != 0 {
            // # Safety
            // `pos` must be less than the capacity of builder.
            unsafe { std::ptr::write(ptr.add(pos), value) };
        }
        // # Safety
        // offset(0) + length(num_rows) <= builder.len() * 8.
        unsafe {
            builder.set_len(capacity);
            Bitmap::from_inner(Arc::new(builder.into()), 0, num_rows, unset_bits)
                .ok()
                .unwrap()
        }
    }

    fn take_value_types<T: ValueType, I>(
        col: &T::Column,
        mut builder: T::ColumnBuilder,
        indices: &[I],
    ) -> Column
    where
        I: common_arrow::arrow::types::Index,
    {
        for index in indices {
            T::push_item(&mut builder, unsafe {
                T::index_column_unchecked(col, index.to_usize())
            });
        }
        T::upcast_column(T::build_column(builder))
    }
}
