// Copyright 2022 Datafuse Labs.
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
use common_exception::ErrorCode;
use common_exception::Result;

use crate::filter_helper::FilterHelpers;
use crate::types::array::ArrayColumnBuilder;
use crate::types::decimal::DecimalColumn;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::string::StringColumnBuilder;
use crate::types::AnyType;
use crate::types::ArrayType;
use crate::types::BooleanType;
use crate::types::StringType;
use crate::types::ValueType;
use crate::types::VariantType;
use crate::with_decimal_type;
use crate::with_number_type;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::TypeDeserializer;
use crate::Value;

impl DataBlock {
    pub fn filter(self, predicate: &Value<AnyType>) -> Result<DataBlock> {
        if self.num_rows() == 0 {
            return Ok(self);
        }

        let predicate = FilterHelpers::cast_to_nonull_boolean(predicate).ok_or_else(|| {
            ErrorCode::BadDataValueType(format!(
                "Filter predict column does not support type '{:?}'",
                predicate
            ))
        })?;

        self.filter_boolean_value(predicate)
    }

    pub fn filter_with_bitmap(block: DataBlock, bitmap: &Bitmap) -> Result<DataBlock> {
        let count_zeros = bitmap.unset_bits();
        match count_zeros {
            0 => Ok(block),
            _ => {
                if count_zeros == block.num_rows() {
                    return Ok(block.slice(0..0));
                }
                let after_columns = block
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
                Ok(DataBlock::new(
                    after_columns,
                    block.num_rows() - count_zeros,
                ))
            }
        }
    }

    pub fn filter_boolean_value(self, filter: Value<BooleanType>) -> Result<DataBlock> {
        match filter {
            Value::Scalar(s) => {
                if s {
                    Ok(self)
                } else {
                    Ok(self.slice(0..0))
                }
            }
            Value::Column(bitmap) => Self::filter_with_bitmap(self, &bitmap),
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
            Column::Null { .. } | Column::EmptyArray { .. } => self.slice(0..length),
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
                let bytes_per_row = column.data.len() / filter.len().max(1);
                let data_capacity = (filter.len() - filter.unset_bits()) * bytes_per_row;

                Self::filter_scalar_types::<StringType>(
                    column,
                    StringColumnBuilder::with_capacity(length, data_capacity),
                    filter,
                )
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
                let builder = ColumnBuilder::from_column(
                    column
                        .values
                        .data_type()
                        .create_deserializer(length)
                        .finish_to_column(),
                );
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::filter_scalar_types::<ArrayType<AnyType>>(column, builder, filter)
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
            Column::Tuple { fields, .. } => {
                let len = filter.len() - filter.unset_bits();
                let fields = fields.iter().map(|c| c.filter(filter)).collect();
                Column::Tuple { fields, len }
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
        assert_eq!(values.len(), filter.len());
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
}
