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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::Result;

use crate::types::array::ArrayColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::string::StringColumnBuilder;
use crate::types::AnyType;
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
use crate::ColumnIndex;
use crate::Scalar;
use crate::Value;

impl<Index: ColumnIndex> Chunk<Index> {
    pub fn scatter<I>(&self, indices: &[I], scatter_size: usize) -> Result<Vec<Self>>
    where I: common_arrow::arrow::types::Index {
        let scattered_columns: Vec<Vec<ChunkEntry<Index>>> = self
            .columns()
            .map(|entry| match &entry.value {
                Value::Scalar(s) => {
                    Column::scatter_repeat_scalars::<I>(s, &entry.data_type, indices, scatter_size)
                        .into_iter()
                        .map(|value| ChunkEntry {
                            id: entry.id.clone(),
                            data_type: entry.data_type.clone(),
                            value: Value::Column(value),
                        })
                        .collect()
                }
                Value::Column(c) => c
                    .scatter(&entry.data_type, indices, scatter_size)
                    .into_iter()
                    .map(|value| ChunkEntry {
                        id: entry.id.clone(),
                        data_type: entry.data_type.clone(),
                        value: Value::Column(value),
                    })
                    .collect(),
            })
            .collect();

        let scattered_chunks = (0..scatter_size)
            .map(|scatter_idx| {
                let chunk_columns: Vec<ChunkEntry<Index>> = scattered_columns
                    .iter()
                    .map(|entry| entry[scatter_idx].clone())
                    .collect();
                let num_rows = chunk_columns[0].value.as_column().unwrap().len();
                Chunk::new(chunk_columns, num_rows)
            })
            .collect();

        Ok(scattered_chunks)
    }
}

impl Column {
    pub fn scatter_repeat_scalars<I>(
        scalar: &Scalar,
        data_type: &DataType,
        indices: &[I],
        scatter_size: usize,
    ) -> Vec<Self>
    where
        I: common_arrow::arrow::types::Index,
    {
        let mut vs = vec![0usize; scatter_size];
        for index in indices {
            vs[index.to_usize()] += 1;
        }
        vs.iter()
            .map(|count| ColumnBuilder::repeat(&scalar.as_ref(), *count, data_type).build())
            .collect()
    }

    pub fn scatter<I>(
        &self,
        data_type: &DataType,
        indices: &[I],
        scatter_size: usize,
    ) -> Vec<Self>
    where
        I: common_arrow::arrow::types::Index,
    {
        let length = indices.len();
        match self {
            Column::Null { .. } => {
                Self::scatter_repeat_scalars::<I>(&Scalar::Null, data_type, indices, scatter_size)
            }
            Column::Number(column) => with_number_mapped_type!(|NUM_TYPE| match column {
                NumberColumn::NUM_TYPE(values) => Self::scatter_scalars::<NumberType<NUM_TYPE>, _>(
                    values,
                    Vec::with_capacity(length),
                    indices,
                    scatter_size
                ),
            }),
            Column::EmptyArray { .. } => Self::scatter_repeat_scalars::<I>(
                &Scalar::EmptyArray,
                data_type,
                indices,
                scatter_size,
            ),
            Column::Boolean(bm) => Self::scatter_scalars::<BooleanType, _>(
                bm,
                MutableBitmap::with_capacity(length),
                indices,
                scatter_size,
            ),
            Column::String(column) => Self::scatter_scalars::<StringType, _>(
                column,
                StringColumnBuilder::with_capacity(length, 0),
                indices,
                scatter_size,
            ),
            Column::Timestamp(column) => Self::scatter_scalars::<TimestampType, _>(
                column,
                Vec::with_capacity(length),
                indices,
                scatter_size,
            ),
            Column::Date(column) => Self::scatter_scalars::<DateType, _>(
                column,
                Vec::with_capacity(length),
                indices,
                scatter_size,
            ),
            Column::Array(column) => {
                let mut builder = ArrayColumnBuilder::<AnyType>::from_column(column.slice(0..0));
                builder.reserve(length);
                Self::scatter_scalars::<ArrayType<AnyType>, _>(
                    column,
                    builder,
                    indices,
                    scatter_size,
                )
            }
            Column::Nullable(c) => {
                let columns = c.column.scatter(data_type, indices, scatter_size);
                let validitys = Self::scatter_scalars::<BooleanType, _>(
                    &c.validity,
                    MutableBitmap::with_capacity(length),
                    indices,
                    scatter_size,
                );
                columns
                    .iter()
                    .zip(&validitys)
                    .map(|(column, validity)| {
                        Column::Nullable(Box::new(NullableColumn {
                            column: column.clone(),
                            validity: BooleanType::try_downcast_column(validity).unwrap(),
                        }))
                    })
                    .collect()
            }
            Column::Tuple { fields, .. } => {
                let mut fields_vs: Vec<Vec<Column>> = fields
                    .iter()
                    .map(|c| c.scatter(data_type, indices, scatter_size))
                    .collect();

                (0..scatter_size)
                    .map(|index| {
                        let fields: Vec<Column> = fields_vs
                            .iter_mut()
                            .map(|field| field.remove(index))
                            .collect();
                        Column::Tuple {
                            len: fields.first().map_or(0, |f| f.len()),
                            fields,
                        }
                    })
                    .collect()
            }
            Column::Variant(column) => Self::scatter_scalars::<VariantType, _>(
                column,
                StringColumnBuilder::with_capacity(length, 0),
                indices,
                scatter_size,
            ),
        }
    }

    fn scatter_scalars<T: ValueType, I>(
        col: &T::Column,
        builder: T::ColumnBuilder,
        indices: &[I],
        scatter_size: usize,
    ) -> Vec<Self>
    where
        I: common_arrow::arrow::types::Index,
    {
        let mut builders: Vec<T::ColumnBuilder> =
            std::iter::repeat(builder).take(scatter_size).collect();

        indices
            .iter()
            .zip(T::iter_column(col))
            .for_each(|(index, item)| {
                T::push_item(&mut builders[index.to_usize()], item);
            });
        builders
            .into_iter()
            .map(|b| T::upcast_column(T::build_column(b)))
            .collect()
    }
}
