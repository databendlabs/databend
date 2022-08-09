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

use crate::types::array::ArrayColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::string::StringColumnBuilder;
use crate::types::AnyType;
use crate::types::ArrayType;
use crate::types::BooleanType;
use crate::types::NullableType;
use crate::types::StringType;
use crate::types::ValueType;
use crate::with_number_type;
use crate::Chunk;
use crate::Column;
use crate::Value;

impl Chunk {
    pub fn filter(self, predicate: &Value<AnyType>) -> Result<Chunk> {
        if self.num_columns() == 0 || self.num_rows() == 0 {
            return Ok(self);
        }

        let predicate = Self::cast_to_nonull_boolean(predicate).ok_or_else(|| {
            ErrorCode::BadDataValueType(format!(
                "Filter predict column does not support type '{:?}'",
                predicate
            ))
        })?;

        match predicate {
            Value::Scalar(s) => {
                if s {
                    Ok(self)
                } else {
                    Ok(Chunk::empty())
                }
            }
            Value::Column(bitmap) => {
                let count_zeros = bitmap.unset_bits();
                match count_zeros {
                    0 => Ok(self),
                    _ => {
                        if count_zeros == self.num_rows() {
                            return Ok(Chunk::empty());
                        }
                        let mut after_columns = Vec::with_capacity(self.num_columns());
                        for value in self.columns() {
                            match value {
                                Value::Scalar(v) => after_columns.push(Value::Scalar(v.clone())),
                                Value::Column(c) => {
                                    after_columns.push(Value::Column(Column::filter(c, &bitmap)))
                                }
                            }
                        }
                        Ok(Chunk::new(after_columns, self.num_rows() - count_zeros))
                    }
                }
            }
        }
    }

    // Must be nullable boolean or boolean value
    fn cast_to_nonull_boolean(predicate: &Value<AnyType>) -> Option<Value<BooleanType>> {
        match predicate {
            Value::Scalar(v) => {
                if let Some(v) = NullableType::<BooleanType>::try_downcast_scalar(&v.as_ref()) {
                    Some(Value::Scalar(v.unwrap_or_default()))
                } else {
                    BooleanType::try_downcast_scalar(&v.as_ref()).map(Value::Scalar)
                }
            }
            Value::Column(c) => {
                if let Some(nb) = NullableType::<BooleanType>::try_downcast_column(c) {
                    let validity = common_arrow::arrow::bitmap::and(&nb.validity, &nb.column);
                    Some(Value::Column(validity))
                } else {
                    BooleanType::try_downcast_column(c).map(Value::Column)
                }
            }
        }
    }
}

impl Column {
    pub fn filter(&self, filter: &Bitmap) -> Column {
        let length = filter.len() - filter.unset_bits();
        if length == self.len() {
            return self.clone();
        }

        with_number_type!(SRC_TYPE, match self {
            Column::SRC_TYPE(values) => {
                Column::SRC_TYPE(Self::filter_primitive_types(values, filter))
            }
            Column::Null { .. } | Column::EmptyArray { .. } => self.slice(0..length),

            Column::Boolean(bm) => Self::filter_scalar_types::<BooleanType>(
                bm,
                MutableBitmap::with_capacity(length),
                filter
            ),
            Column::String(column) => Self::filter_scalar_types::<StringType>(
                column,
                StringColumnBuilder::with_capacity(length, 0),
                filter
            ),
            Column::Array(column) => {
                let mut builder = ArrayColumnBuilder::<AnyType>::from_column(column.slice(0..0));
                builder.reserve(length);
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
        })
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
