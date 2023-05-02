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

use crate::types::array::ArrayColumn;
use crate::types::array::ArrayColumnBuilder;
use crate::types::decimal::DecimalColumn;
use crate::types::map::KvColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::string::StringColumn;
use crate::types::string::StringColumnBuilder;
use crate::types::AnyType;
use crate::types::ArrayType;
use crate::types::BooleanType;
use crate::types::MapType;
use crate::types::ValueType;
use crate::types::VariantType;
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
                            BlockEntry {
                                data_type: entry.data_type.clone(),
                                value,
                            }
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
                let bytes_per_row = column.data.len() / filter.len().max(1);
                let data_capacity = (filter.len() - filter.unset_bits()) * bytes_per_row;

                Self::filter_scalar_types::<VariantType>(
                    column,
                    StringColumnBuilder::with_capacity(length, data_capacity),
                    filter,
                )
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
        let selected = filter.len() - filter.unset_bits();
        if selected == values.len() {
            return values.clone();
        }
        let mut values = values.as_slice();
        let mut new = Vec::<T>::with_capacity(selected);
        let mut dst = new.as_mut_ptr();

        let (mut slice, offset, mut length) = filter.as_slice();
        if offset > 0 {
            // Consume the offset
            let n = 8 - offset;
            values
                .iter()
                .zip(filter.iter())
                .take(n)
                .for_each(|(value, is_selected)| {
                    if is_selected {
                        unsafe {
                            dst.write(*value);
                            dst = dst.add(1);
                        }
                    }
                });
            slice = &slice[1..];
            length -= n;
            values = &values[n..];
        }

        const CHUNK_SIZE: usize = 64;
        let mut chunks = values.chunks_exact(CHUNK_SIZE);
        let mut mask_chunks = BitChunksExact::<u64>::new(slice, length);

        chunks
            .by_ref()
            .zip(mask_chunks.by_ref())
            .for_each(|(chunk, mut mask)| {
                if mask == u64::MAX {
                    unsafe {
                        std::ptr::copy(chunk.as_ptr(), dst, CHUNK_SIZE);
                        dst = dst.add(CHUNK_SIZE);
                    }
                } else {
                    while mask != 0 {
                        let n = mask.trailing_zeros() as usize;
                        unsafe {
                            dst.write(chunk[n]);
                            dst = dst.add(1);
                        }
                        mask = mask & (mask - 1);
                    }
                }
            });

        chunks
            .remainder()
            .iter()
            .zip(mask_chunks.remainder_iter())
            .for_each(|(value, is_selected)| {
                if is_selected {
                    unsafe {
                        dst.write(*value);
                        dst = dst.add(1);
                    }
                }
            });

        unsafe { new.set_len(selected) };
        new.into()
    }

    // low-level API using unsafe to improve performance
    fn filter_string_scalars(values: &StringColumn, filter: &Bitmap) -> StringColumn {
        debug_assert_eq!(values.len(), filter.len());
        let selected = filter.len() - filter.unset_bits();
        if selected == values.len() {
            return values.clone();
        }
        let data = values.data.as_slice();
        let offsets = values.offsets.as_slice();

        let mut res_offsets = Vec::with_capacity(selected + 1);
        res_offsets.push(0);

        let mut res_data = vec![];
        let hint_size = data.len() / (values.len() + 1) * selected;

        static MAX_HINT_SIZE: usize = 1000000000;
        if hint_size < MAX_HINT_SIZE && values.len() < MAX_HINT_SIZE {
            res_data.reserve(hint_size)
        }

        let mut pos = 0;

        let (mut slice, offset, mut length) = filter.as_slice();
        if offset > 0 {
            // Consume the offset
            let n = 8 - offset;
            values
                .iter()
                .zip(filter.iter())
                .take(n)
                .for_each(|(value, is_selected)| {
                    if is_selected {
                        res_data.extend_from_slice(value);
                        res_offsets.push(res_data.len() as u64);
                    }
                });
            slice = &slice[1..];
            length -= n;
            pos += n;
        }

        const CHUNK_SIZE: usize = 64;
        let mut mask_chunks = BitChunksExact::<u64>::new(slice, length);

        for mut mask in mask_chunks.by_ref() {
            if mask == u64::MAX {
                let data = &data[offsets[pos] as usize..offsets[pos + CHUNK_SIZE] as usize];
                res_data.extend_from_slice(data);

                let mut last_len = *res_offsets.last().unwrap();
                for i in 0..CHUNK_SIZE {
                    last_len += offsets[pos + i + 1] - offsets[pos + i];
                    res_offsets.push(last_len);
                }
            } else {
                while mask != 0 {
                    let n = mask.trailing_zeros() as usize;
                    let data = &data[offsets[pos + n] as usize..offsets[pos + n + 1] as usize];
                    res_data.extend_from_slice(data);
                    res_offsets.push(res_data.len() as u64);

                    mask = mask & (mask - 1);
                }
            }
            pos += CHUNK_SIZE;
        }

        for is_select in mask_chunks.remainder_iter() {
            if is_select {
                let data = &data[offsets[pos] as usize..offsets[pos + 1] as usize];
                res_data.extend_from_slice(data);
                res_offsets.push(res_data.len() as u64);
            }
            pos += 1;
        }

        StringColumn {
            data: res_data.into(),
            offsets: res_offsets.into(),
        }
    }
}
