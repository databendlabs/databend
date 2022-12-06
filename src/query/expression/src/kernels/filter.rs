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
use crate::types::number::NumberColumn;
use crate::types::number::NumberScalar;
use crate::types::string::StringColumnBuilder;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::ArrayType;
use crate::types::BooleanType;
use crate::types::StringType;
use crate::types::ValueType;
use crate::with_number_mapped_type;
use crate::with_number_type;
use crate::Chunk;
use crate::ChunkEntry;
use crate::Column;
use crate::ColumnIndex;
use crate::Scalar;
use crate::Value;

impl<Index: ColumnIndex> Chunk<Index> {
    pub fn filter(self, predicate: &Value<AnyType>) -> Result<Chunk<Index>> {
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
                    Ok(self.slice(0..0))
                }
            }
            Value::Column(bitmap) => {
                let count_zeros = bitmap.unset_bits();
                match count_zeros {
                    0 => Ok(self),
                    _ => {
                        if count_zeros == self.num_rows() {
                            return Ok(self.slice(0..0));
                        }
                        let after_columns = self
                            .columns()
                            .map(|entry| match &entry.value {
                                Value::Scalar(s) => ChunkEntry {
                                    id: entry.id.clone(),
                                    data_type: entry.data_type.clone(),
                                    value: Value::Scalar(s.clone()),
                                },
                                Value::Column(c) => ChunkEntry {
                                    id: entry.id.clone(),
                                    data_type: entry.data_type.clone(),
                                    value: Value::Column(Column::filter(c, &bitmap)),
                                },
                            })
                            .collect();
                        Ok(Chunk::new(after_columns, self.num_rows() - count_zeros))
                    }
                }
            }
        }
    }

    // Must be numeric, boolean, or string value type
    fn cast_to_nonull_boolean(predicate: &Value<AnyType>) -> Option<Value<BooleanType>> {
        match predicate {
            Value::Scalar(s) => Self::cast_scalar_to_boolean(s).map(Value::Scalar),
            Value::Column(c) => Self::cast_column_to_boolean(c).map(Value::Column),
        }
    }

    fn cast_scalar_to_boolean(s: &Scalar) -> Option<bool> {
        match s {
            Scalar::Number(num) => with_number_mapped_type!(|SRC_TYPE| match num {
                NumberScalar::SRC_TYPE(value) => Some(value != &SRC_TYPE::default()),
            }),
            Scalar::Boolean(value) => Some(*value),
            Scalar::String(value) => Some(!value.is_empty()),
            Scalar::Timestamp(value) => Some(*value != 0),
            Scalar::Date(value) => Some(*value != 0),
            Scalar::Null => Some(false),
            _ => None,
        }
    }

    fn cast_column_to_boolean(c: &Column) -> Option<Bitmap> {
        match c {
            Column::Number(num) => with_number_mapped_type!(|SRC_TYPE| match num {
                NumberColumn::SRC_TYPE(value) => Some(BooleanType::column_from_iter(
                    value.iter().map(|v| v != &SRC_TYPE::default()),
                    &[],
                )),
            }),
            Column::Boolean(value) => Some(value.clone()),
            Column::String(value) => Some(BooleanType::column_from_iter(
                value.iter().map(|s| !s.is_empty()),
                &[],
            )),
            Column::Timestamp(value) => Some(BooleanType::column_from_iter(
                value.iter().map(|v| *v != 0),
                &[],
            )),
            Column::Date(value) => Some(BooleanType::column_from_iter(
                value.iter().map(|v| *v != 0),
                &[],
            )),
            Column::Null { len } => Some(MutableBitmap::from_len_zeroed(*len).into()),
            Column::Nullable(c) => {
                let inner = Self::cast_column_to_boolean(&c.column)?;
                Some((&inner) & (&c.validity))
            }
            _ => None,
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
            Column::Boolean(bm) => Self::filter_scalar_types::<BooleanType>(
                bm,
                MutableBitmap::with_capacity(length),
                filter,
            ),
            Column::String(column) => Self::filter_scalar_types::<StringType>(
                column,
                StringColumnBuilder::with_capacity(length, 0),
                filter,
            ),
            Column::Timestamp(column) => {
                let ts = Self::filter_primitive_types(column, filter);
                Column::Timestamp(ts)
            }
            Column::Date(column) => {
                let d = Self::filter_primitive_types(column, filter);
                Column::Date(d)
            }
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
            Column::Variant(column) => Self::filter_scalar_types::<StringType>(
                column,
                StringColumnBuilder::with_capacity(length, 0),
                filter,
            ),
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
