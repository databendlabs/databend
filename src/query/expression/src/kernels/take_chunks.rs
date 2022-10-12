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

use crate::types::array::ArrayColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::ArrayType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::IntervalType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::TimestampType;
use crate::types::ValueType;
use crate::types::VariantType;
use crate::with_number_mapped_type;
use crate::Chunk;
use crate::Column;
use crate::ColumnBuilder;
use crate::Scalar;
use crate::Value;

// chunk, row, size
pub type ChunkRowIndex = (usize, usize, usize);

impl Chunk {
    pub fn take_chunks(chunks: &[Chunk], indices: &[ChunkRowIndex]) -> Self {
        debug_assert!(!chunks.is_empty());
        let result_size = indices.iter().map(|(_, _, c)| *c).sum();

        let mut result_columns = Vec::with_capacity(chunks[0].num_columns());

        for index in 0..chunks[0].num_columns() {
            let mut columns = Vec::with_capacity(chunks.len());

            for chunk in chunks {
                let c = &chunk.columns()[index];
                columns.push((c.0.clone(), c.1.clone(), chunk.num_rows()));
            }

            let datatype = columns[0].1.clone();
            if datatype.is_null() {
                result_columns.push((Value::Scalar(Scalar::Null), datatype));
                continue;
            }
            // if they are all same scalars
            if columns.iter().all(|c| matches!(&c.0, Value::Scalar(_))) {
                let mut s = None;
                let mut all_same_scalar = true;
                for (value, _, _) in columns.iter() {
                    if let Value::Scalar(scalar) = value {
                        if s.is_none() {
                            s = Some(scalar);
                        } else if s != Some(scalar) {
                            all_same_scalar = false;
                        }
                    }
                }
                if all_same_scalar {
                    result_columns.push((Value::Scalar(s.unwrap().clone()), datatype));
                    continue;
                }
            }

            let full_columns: Vec<Column> = columns
                .iter()
                .map(|(value, ty, rows)| match value {
                    Value::Scalar(s) => {
                        let builder = ColumnBuilder::repeat(&s.as_ref(), *rows, ty);
                        builder.build()
                    }
                    Value::Column(c) => c.clone(),
                })
                .collect();

            let column = Column::take_column_indices(
                &full_columns,
                columns[0].1.clone(),
                indices,
                result_size,
            );
            result_columns.push((Value::Column(column), datatype));
        }

        Chunk::new(result_columns, result_size)
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
            Column::Interval(_) => {
                let builder = IntervalType::create_builder(result_size, &[]);
                Self::take_chunk_value_types::<IntervalType>(columns, builder, indices)
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
