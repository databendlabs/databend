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

use common_arrow::arrow::bitmap::utils::BitChunkIterExact;
use common_arrow::arrow::bitmap::utils::BitChunksExact;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::buffer::Buffer;
use common_exception::Result;

use crate::kernels::utils::copy_advance_aligned;
use crate::kernels::utils::set_vec_len_by_ptr;
use crate::kernels::utils::store_advance_aligned;
use crate::types::array::ArrayColumn;
use crate::types::array::ArrayColumnBuilder;
use crate::types::decimal::DecimalColumn;
use crate::types::map::KvColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::string::StringColumn;
use crate::types::AnyType;
use crate::types::ArrayType;
use crate::types::BooleanType;
use crate::types::MapType;
use crate::types::ValueType;
use crate::with_decimal_type;
use crate::with_number_type;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::Value;

impl DataBlock {
    pub fn filter_with_bitmap(self, bitmap: &Bitmap) -> Result<DataBlock> {
        if self.num_rows() == 0 {
            return Ok(self);
        }

        let count_zeros = bitmap.unset_bits();
        match count_zeros {
            0 => Ok(self),
            _ => {
                if count_zeros == self.num_rows() {
                    return Ok(self.slice(0..0));
                }
                let after_columns = self
                    .columns()
                    .iter()
                    .map(|entry| match &entry.value {
                        Value::Column(c) => {
                            let value = Value::Column(Column::filter(c, bitmap));
                            BlockEntry::new(entry.data_type.clone(), value)
                        }
                        _ => entry.clone(),
                    })
                    .collect();
                Ok(DataBlock::new(after_columns, self.num_rows() - count_zeros))
            }
        }
    }

    pub fn filter_boolean_value(self, filter: &Value<BooleanType>) -> Result<DataBlock> {
        if self.num_rows() == 0 {
            return Ok(self);
        }

        match filter {
            Value::Scalar(s) => {
                if *s {
                    Ok(self)
                } else {
                    Ok(self.slice(0..0))
                }
            }
            Value::Column(bitmap) => Self::filter_with_bitmap(self, bitmap),
        }
    }
}

impl Column {
    pub fn filter(&self, filter: &Bitmap) -> Column {
        let length = filter.len() - filter.unset_bits();
        if length == self.len() {
            return self.clone();
        }

        match self {
            Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {
                self.slice(0..length)
            }
            Column::Number(column) => with_number_type!(|NUM_TYPE| match column {
                NumberColumn::NUM_TYPE(values) => {
                    Column::Number(NumberColumn::NUM_TYPE(Self::filter_primitive_types(
                        values, filter,
                    )))
                }
            }),
            Column::Decimal(column) => with_decimal_type!(|DECIMAL_TYPE| match column {
                DecimalColumn::DECIMAL_TYPE(values, size) => {
                    Column::Decimal(DecimalColumn::DECIMAL_TYPE(
                        Self::filter_primitive_types(values, filter),
                        *size,
                    ))
                }
            }),
            Column::Boolean(bm) => Self::filter_scalar_types::<BooleanType>(
                bm,
                MutableBitmap::with_capacity(length),
                filter,
            ),
            Column::String(column) => {
                let column = Self::filter_string_scalars(column, filter);
                Column::String(column)
            }
            Column::Timestamp(column) => {
                let ts = Self::filter_primitive_types(column, filter);
                Column::Timestamp(ts)
            }
            Column::Date(column) => {
                let d = Self::filter_primitive_types(column, filter);
                Column::Date(d)
            }
            Column::Array(column) => {
                let mut offsets = Vec::with_capacity(length + 1);
                offsets.push(0);
                let builder = ColumnBuilder::with_capacity(&column.values.data_type(), length);
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::filter_scalar_types::<ArrayType<AnyType>>(column, builder, filter)
            }
            Column::Map(column) => {
                let mut offsets = Vec::with_capacity(length + 1);
                offsets.push(0);
                let builder = ColumnBuilder::from_column(
                    ColumnBuilder::with_capacity(&column.values.data_type(), length).build(),
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
                Self::filter_scalar_types::<MapType<AnyType, AnyType>>(&column, builder, filter)
            }
            Column::Bitmap(column) => {
                let column = Self::filter_string_scalars(column, filter);
                Column::Bitmap(column)
            }

            Column::Nullable(c) => {
                let column = Self::filter(&c.column, filter);
                let validity = Self::filter_scalar_types::<BooleanType>(
                    &c.validity,
                    MutableBitmap::with_capacity(length),
                    filter,
                );
                Column::Nullable(Box::new(NullableColumn {
                    column,
                    validity: BooleanType::try_downcast_column(&validity).unwrap(),
                }))
            }
            Column::Tuple(fields) => {
                let fields = fields.iter().map(|c| c.filter(filter)).collect();
                Column::Tuple(fields)
            }
            Column::Variant(column) => {
                let column = Self::filter_string_scalars(column, filter);
                Column::Variant(column)
            }
        }
    }

    fn filter_scalar_types<T: ValueType>(
        col: &T::Column,
        mut builder: T::ColumnBuilder,
        filter: &Bitmap,
    ) -> Column {
        const CHUNK_SIZE: usize = 64;
        let (mut slice, offset, mut length) = filter.as_slice();
        let mut start_index: usize = 0;

        if offset > 0 {
            let n = 8 - offset;
            start_index += n;
            filter
                .iter()
                .enumerate()
                .take(n)
                .for_each(|(index, is_selected)| {
                    if is_selected {
                        T::push_item(&mut builder, T::index_column(col, index).unwrap());
                    }
                });
            slice = &slice[1..];
            length -= n;
        }

        let mut mask_chunks = BitChunksExact::<u64>::new(slice, length);

        mask_chunks
            .by_ref()
            .enumerate()
            .for_each(|(mask_index, mut mask)| {
                while mask != 0 {
                    let n = mask.trailing_zeros() as usize;
                    let index = mask_index * CHUNK_SIZE + n + start_index;
                    T::push_item(&mut builder, T::index_column(col, index).unwrap());
                    mask = mask & (mask - 1);
                }
            });

        let remainder_start = length - length % CHUNK_SIZE;
        mask_chunks
            .remainder_iter()
            .enumerate()
            .for_each(|(mask_index, is_selected)| {
                if is_selected {
                    let index = mask_index + remainder_start + start_index;
                    T::push_item(&mut builder, T::index_column(col, index).unwrap());
                }
            });

        T::upcast_column(T::build_column(builder))
    }

    // low-level API using unsafe to improve performance
    fn filter_primitive_types<T: Copy>(values: &Buffer<T>, filter: &Bitmap) -> Buffer<T> {
        debug_assert_eq!(values.len(), filter.len());
        let num_rows = filter.len() - filter.unset_bits();
        if num_rows == values.len() {
            return values.clone();
        }

        let mut builder: Vec<T> = Vec::with_capacity(num_rows);
        let mut ptr = builder.as_mut_ptr();
        let mut values_ptr = values.as_slice().as_ptr();
        let (mut slice, offset, mut length) = filter.as_slice();

        unsafe {
            if offset > 0 {
                let mut mask = slice[0];
                while mask != 0 {
                    let n = mask.trailing_zeros() as usize;
                    if n >= offset {
                        copy_advance_aligned(values_ptr.add(n - offset), &mut ptr, 1);
                    }
                    mask = mask & (mask - 1);
                }
                length -= 8 - offset;
                slice = &slice[1..];
                values_ptr = values_ptr.add(8 - offset);
            }

            const CHUNK_SIZE: usize = 64;
            let mut mask_chunks = BitChunksExact::<u64>::new(slice, length);
            let mut continuous_selected = 0;
            for mut mask in mask_chunks.by_ref() {
                if mask == u64::MAX {
                    continuous_selected += CHUNK_SIZE;
                } else {
                    if continuous_selected > 0 {
                        copy_advance_aligned(values_ptr, &mut ptr, continuous_selected);
                        values_ptr = values_ptr.add(continuous_selected);
                        continuous_selected = 0;
                    }
                    while mask != 0 {
                        let n = mask.trailing_zeros() as usize;
                        copy_advance_aligned(values_ptr.add(n), &mut ptr, 1);
                        mask = mask & (mask - 1);
                    }
                    values_ptr = values_ptr.add(CHUNK_SIZE);
                }
            }
            if continuous_selected > 0 {
                copy_advance_aligned(values_ptr, &mut ptr, continuous_selected);
                values_ptr = values_ptr.add(continuous_selected);
            }

            for (i, is_selected) in mask_chunks.remainder_iter().enumerate() {
                if is_selected {
                    copy_advance_aligned(values_ptr.add(i), &mut ptr, 1);
                }
            }

            set_vec_len_by_ptr(&mut builder, ptr);
        }

        builder.into()
    }

    // low-level API using unsafe to improve performance
    fn filter_string_scalars(values: &StringColumn, filter: &Bitmap) -> StringColumn {
        debug_assert_eq!(values.len(), filter.len());
        let num_rows = filter.len() - filter.unset_bits();
        if num_rows == values.len() {
            return values.clone();
        }

        // Each element of `items` is (string pointer(u64), string length).
        let mut items: Vec<(u64, usize)> = Vec::with_capacity(num_rows);
        // [`StringColumn`] consists of [`data`] and [`offset`], we build [`data`] and [`offset`] respectively,
        // and then call `StringColumn::new(data.into(), offsets.into())` to create [`StringColumn`].
        let values_offset = values.offsets().as_slice();
        let values_data_ptr = values.data().as_slice().as_ptr();
        let mut offsets: Vec<u64> = Vec::with_capacity(num_rows + 1);
        let mut offsets_ptr = offsets.as_mut_ptr();
        let mut items_ptr = items.as_mut_ptr();
        let mut data_size = 0;

        // Build [`offset`] and calculate `data_size` required by [`data`].
        unsafe {
            store_advance_aligned::<u64>(0, &mut offsets_ptr);
            let mut idx = 0;
            let (mut slice, offset, mut length) = filter.as_slice();
            if offset > 0 {
                let mut mask = slice[0];
                while mask != 0 {
                    let n = mask.trailing_zeros() as usize;
                    if n >= offset {
                        let start = *values_offset.get_unchecked(n - offset) as usize;
                        let len = *values_offset.get_unchecked(n - offset + 1) as usize - start;
                        data_size += len as u64;
                        store_advance_aligned(data_size, &mut offsets_ptr);
                        store_advance_aligned(
                            (values_data_ptr.add(start) as u64, len),
                            &mut items_ptr,
                        );
                    }
                    mask = mask & (mask - 1);
                }
                length -= 8 - offset;
                slice = &slice[1..];
                idx += 8 - offset;
            }

            const CHUNK_SIZE: usize = 64;
            let mut mask_chunks = BitChunksExact::<u64>::new(slice, length);
            let mut continuous_selected = 0;
            for mut mask in mask_chunks.by_ref() {
                if mask == u64::MAX {
                    continuous_selected += CHUNK_SIZE;
                } else {
                    if continuous_selected > 0 {
                        let start = *values_offset.get_unchecked(idx) as usize;
                        let len = *values_offset.get_unchecked(idx + continuous_selected) as usize
                            - start;
                        store_advance_aligned(
                            (values_data_ptr.add(start) as u64, len),
                            &mut items_ptr,
                        );
                        for i in 0..continuous_selected {
                            data_size += *values_offset.get_unchecked(idx + i + 1)
                                - *values_offset.get_unchecked(idx + i);
                            store_advance_aligned(data_size, &mut offsets_ptr);
                        }
                        idx += continuous_selected;
                        continuous_selected = 0;
                    }
                    while mask != 0 {
                        let n = mask.trailing_zeros() as usize;
                        let start = *values_offset.get_unchecked(idx + n) as usize;
                        let len = *values_offset.get_unchecked(idx + n + 1) as usize - start;
                        data_size += len as u64;
                        store_advance_aligned(
                            (values_data_ptr.add(start) as u64, len),
                            &mut items_ptr,
                        );
                        store_advance_aligned(data_size, &mut offsets_ptr);
                        mask = mask & (mask - 1);
                    }
                    idx += CHUNK_SIZE;
                }
            }
            if continuous_selected > 0 {
                let start = *values_offset.get_unchecked(idx) as usize;
                let len = *values_offset.get_unchecked(idx + continuous_selected) as usize - start;
                store_advance_aligned((values_data_ptr.add(start) as u64, len), &mut items_ptr);
                for i in 0..continuous_selected {
                    data_size += *values_offset.get_unchecked(idx + i + 1)
                        - *values_offset.get_unchecked(idx + i);
                    store_advance_aligned(data_size, &mut offsets_ptr);
                }
                idx += continuous_selected;
            }

            for (i, is_selected) in mask_chunks.remainder_iter().enumerate() {
                if is_selected {
                    let start = *values_offset.get_unchecked(idx + i) as usize;
                    let len = *values_offset.get_unchecked(idx + i + 1) as usize - start;
                    data_size += len as u64;
                    store_advance_aligned((values_data_ptr.add(start) as u64, len), &mut items_ptr);
                    store_advance_aligned(data_size, &mut offsets_ptr);
                }
            }
            set_vec_len_by_ptr(&mut items, items_ptr);
            set_vec_len_by_ptr(&mut offsets, offsets_ptr);
        }

        // Build [`data`].
        let mut data: Vec<u8> = Vec::with_capacity(data_size as usize);
        let mut data_ptr = data.as_mut_ptr();

        unsafe {
            for (str_ptr, len) in items.iter() {
                copy_advance_aligned(*str_ptr as *const u8, &mut data_ptr, *len);
            }
            set_vec_len_by_ptr(&mut data, data_ptr);
        }

        StringColumn::new(data.into(), offsets.into())
    }
}
