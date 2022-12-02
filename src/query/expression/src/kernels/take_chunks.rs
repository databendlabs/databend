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

use common_arrow::arrow::compute::merge_sort::MergeSlice;
use itertools::Itertools;

use crate::types::array::ArrayColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::ArrayType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::TimestampType;
use crate::types::ValueType;
use crate::types::VariantType;
use crate::with_number_mapped_type;
use crate::Chunk;
use crate::ChunkEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::Scalar;
use crate::Value;

// Chunk idx, row idx in the chunk, times
pub type ChunkRowIndex = (usize, usize, usize);

impl Chunk {
    pub fn take_chunks(chunks: &[Chunk], indices: &[ChunkRowIndex]) -> Self {
        debug_assert!(!chunks.is_empty());
        debug_assert!(chunks[0].num_columns() > 0);

        let result_size = indices.iter().map(|(_, _, c)| *c).sum();

        let result_columns = (0..chunks[0].num_columns())
            .map(|index| {
                let columns = chunks
                    .iter()
                    .map(|chunk| (chunk.get_by_offset(index), chunk.num_rows()))
                    .collect_vec();

                let id = columns[0].0.id;
                let ty = columns[0].0.data_type.clone();
                if ty.is_null() {
                    return ChunkEntry {
                        id,
                        data_type: ty,
                        value: Value::Scalar(Scalar::Null),
                    };
                }

                // if they are all same scalars
                if matches!(columns[0].0.value, Value::Scalar(_)) {
                    let all_same_scalar = columns.iter().map(|(entry, _)| &entry.value).all_equal();
                    if all_same_scalar {
                        return (*columns[0].0).clone();
                    }
                }

                let full_columns: Vec<Column> = columns
                    .iter()
                    .map(|(entry, rows)| match &entry.value {
                        Value::Scalar(s) => {
                            let builder =
                                ColumnBuilder::repeat(&s.as_ref(), *rows, &entry.data_type);
                            builder.build()
                        }
                        Value::Column(c) => c.clone(),
                    })
                    .collect();

                let column =
                    Column::take_column_indices(&full_columns, ty.clone(), indices, result_size);

                ChunkEntry {
                    id,
                    data_type: ty,
                    value: Value::Column(column),
                }
            })
            .collect();

        Chunk::new(result_columns, result_size)
    }

    pub fn take_by_slice_limit(chunk: &Chunk, slice: (usize, usize), limit: Option<usize>) -> Self {
        let columns = chunk
            .columns()
            .map(|entry| {
                Self::take_column_by_slices_limit(&[entry.clone()], &[(0, slice.0, slice.1)], limit)
            })
            .collect::<Vec<_>>();

        let num_rows = chunk.num_rows().min(slice.1.min(limit.unwrap_or(slice.1)));
        Chunk::new(columns, num_rows)
    }

    pub fn take_by_slices_limit_from_chunks(
        chunks: &[Chunk],
        slices: &[MergeSlice],
        limit: Option<usize>,
    ) -> Self {
        debug_assert!(!chunks.is_empty());
        let total_num_rows: usize = chunks.iter().map(|c| c.num_rows()).sum();
        let result_size: usize = slices.iter().map(|(_, _, c)| *c).sum();
        let result_size = total_num_rows.min(result_size.min(limit.unwrap_or(result_size)));

        if result_size == 0 {
            return Chunk::empty();
        }

        let mut result_columns = Vec::with_capacity(chunks[0].num_columns());

        for index in 0..chunks[0].num_columns() {
            let cols = chunks
                .iter()
                .map(|c| c.get_by_offset(index).clone())
                .collect::<Vec<_>>();

            let merged_col = Self::take_column_by_slices_limit(&cols, slices, limit);

            result_columns.push(merged_col);
        }

        Chunk::new(result_columns, result_size)
    }

    pub fn take_column_by_slices_limit(
        columns: &[ChunkEntry],
        slices: &[MergeSlice],
        limit: Option<usize>,
    ) -> ChunkEntry {
        assert!(!columns.is_empty());
        let ty = &columns[0].data_type;
        let id = &columns[0].id;

        let num_rows = limit
            .unwrap_or(usize::MAX)
            .min(slices.iter().map(|(_, _, c)| *c).sum());
        let mut builder = ColumnBuilder::with_capacity(ty, num_rows);
        let mut remain = num_rows;

        for (index, start, len) in slices {
            let len = (*len).min(remain);
            remain -= len;
            let col = &columns[*index];
            match &col.value {
                Value::Scalar(scalar) => {
                    let other = ColumnBuilder::repeat(&scalar.as_ref(), len, &col.data_type);
                    builder.append(&other);
                }
                Value::Column(c) => {
                    let c = c.slice(*start..(*start + len));
                    let other = ColumnBuilder::from_column(c);
                    builder.append(&other);
                }
            }
            if remain == 0 {
                break;
            }
        }

        let col = builder.build();
        // if they are all same scalars, combine it to a single scalar
        let all_same = col.len() > 0 && col.iter().all(|s| unsafe { s == col.index_unchecked(0) });

        let value = if all_same {
            let s = unsafe { col.index_unchecked(0) };

            Value::Scalar(s.to_owned())
        } else {
            Value::Column(col)
        };

        ChunkEntry {
            id: *id,
            data_type: ty.clone(),
            value,
        }
    }
}

impl Column {
    pub fn take_column_indices(
        columns: &[Column],
        datatype: DataType,
        indices: &[ChunkRowIndex],
        result_size: usize,
    ) -> Column {
        match &columns[0] {
            Column::Null { .. } => Column::Null { len: result_size },
            Column::EmptyArray { .. } => Column::EmptyArray { len: result_size },
            Column::Number(column) => with_number_mapped_type!(|NUM_TYPE| match column {
                NumberColumn::NUM_TYPE(_) => {
                    let builder = NumberType::<NUM_TYPE>::create_builder(result_size, &[]);
                    Self::take_chunk_value_types::<NumberType<NUM_TYPE>>(columns, builder, indices)
                }
            }),
            Column::Boolean(_) => {
                let builder = BooleanType::create_builder(result_size, &[]);
                Self::take_chunk_value_types::<BooleanType>(columns, builder, indices)
            }
            Column::String(_) => {
                let builder = StringType::create_builder(result_size, &[]);
                Self::take_chunk_value_types::<StringType>(columns, builder, indices)
            }
            Column::Timestamp(_) => {
                let builder = TimestampType::create_builder(result_size, &[]);
                Self::take_chunk_value_types::<TimestampType>(columns, builder, indices)
            }
            Column::Date(_) => {
                let builder = DateType::create_builder(result_size, &[]);
                Self::take_chunk_value_types::<DateType>(columns, builder, indices)
            }
            Column::Array(column) => {
                let mut builder = ArrayColumnBuilder::<AnyType>::from_column(column.slice(0..0));
                builder.reserve(result_size);

                Self::take_chunk_value_types::<ArrayType<AnyType>>(columns, builder, indices)
            }
            Column::Nullable(_) => {
                let inner_ty = datatype.as_nullable().unwrap();
                let inner_columns = columns
                    .iter()
                    .map(|c| match c {
                        Column::Nullable(c) => c.column.clone(),
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>();

                let inner_bitmaps = columns
                    .iter()
                    .map(|c| match c {
                        Column::Nullable(c) => Column::Boolean(c.validity.clone()),
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>();

                let inner_column = Self::take_column_indices(
                    &inner_columns,
                    *inner_ty.clone(),
                    indices,
                    result_size,
                );

                let inner_bitmap = Self::take_column_indices(
                    &inner_bitmaps,
                    DataType::Boolean,
                    indices,
                    result_size,
                );

                Column::Nullable(Box::new(NullableColumn {
                    column: inner_column,
                    validity: BooleanType::try_downcast_column(&inner_bitmap).unwrap(),
                }))
            }
            Column::Tuple { .. } => {
                let inner_ty = datatype.as_tuple().unwrap();
                let inner_columns = columns
                    .iter()
                    .map(|c| match c {
                        Column::Tuple { fields, .. } => fields.clone(),
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>();

                let fields: Vec<Column> = inner_ty
                    .iter()
                    .enumerate()
                    .map(|(idx, ty)| {
                        let sub_columns = inner_columns
                            .iter()
                            .map(|c| c[idx].clone())
                            .collect::<Vec<_>>();
                        Self::take_column_indices(&sub_columns, ty.clone(), indices, result_size)
                    })
                    .collect();

                Column::Tuple {
                    fields,
                    len: result_size,
                }
            }
            Column::Variant(_) => {
                let builder = VariantType::create_builder(result_size, &[]);
                Self::take_chunk_value_types::<VariantType>(columns, builder, indices)
            }
        }
    }

    fn take_chunk_value_types<T: ValueType>(
        columns: &[Column],
        mut builder: T::ColumnBuilder,
        indices: &[ChunkRowIndex],
    ) -> Column {
        unsafe {
            for &(chunk_index, row, times) in indices {
                let col = T::try_downcast_column(&columns[chunk_index]).unwrap();
                for _ in 0..times {
                    T::push_item(&mut builder, T::index_column_unchecked(&col, row))
                }
            }
        }
        T::upcast_column(T::build_column(builder))
    }
}
