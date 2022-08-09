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
use common_arrow::arrow::types::Index;
use common_exception::Result;

use crate::types::array::ArrayColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::string::StringColumnBuilder;
use crate::types::AnyType;
use crate::types::ArrayType;
use crate::types::BooleanType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::ValueType;
use crate::with_number_mapped_type;
use crate::Chunk;
use crate::Column;
use crate::Scalar;
use crate::Value;

impl Chunk {
    pub fn scatter<I: Index>(&self, indices: &[I], scatter_size: usize) -> Result<Vec<Self>> {
        let columns_size = self.num_columns();
        let mut scattered_columns: Vec<Vec<Column>> = Vec::with_capacity(scatter_size);

        for column_index in 0..columns_size {
            match &self.columns()[column_index] {
                Value::Scalar(s) => {
                    scattered_columns.push(Column::scatter_repeat_scalars::<I>(
                        s,
                        indices,
                        scatter_size,
                    ));
                }
                Value::Column(c) => {
                    let cs = c.scatter(indices, scatter_size);
                    scattered_columns.push(cs);
                }
            }
        }

        let mut scattered_chunks = Vec::with_capacity(scatter_size);
        for index in 0..scatter_size {
            let mut chunk_columns = vec![];
            let mut size = 0;
            for item in scattered_columns.iter() {
                size = item[index].len();
                chunk_columns.push(Value::Column(item[index].clone()));
            }
            scattered_chunks.push(Chunk::new(chunk_columns, size));
        }

        Ok(scattered_chunks)
    }
}

impl Column {
    pub fn scatter_repeat_scalars<I: Index>(
        scalar: &Scalar,
        indices: &[I],
        scatter_size: usize,
    ) -> Vec<Self> {
        let mut vs = vec![0usize; scatter_size];
        for index in indices {
            vs[index.to_usize()] += 1;
        }
        vs.iter()
            .map(|count| scalar.as_ref().repeat(*count).build())
            .collect()
    }

    pub fn scatter<I: Index>(&self, indices: &[I], scatter_size: usize) -> Vec<Self> {
        let length = indices.len();
        with_number_mapped_type!(SRC_TYPE, match self {
            Column::SRC_TYPE(values) => Self::scatter_scalars::<NumberType<SRC_TYPE>, _>(
                values,
                Vec::with_capacity(length),
                indices,
                scatter_size
            ),
            Column::Null { .. } => {
                Self::scatter_repeat_scalars::<I>(&Scalar::Null, indices, scatter_size)
            }
            Column::EmptyArray { .. } => {
                Self::scatter_repeat_scalars::<I>(&Scalar::EmptyArray, indices, scatter_size)
            }
            Column::Boolean(bm) => Self::scatter_scalars::<BooleanType, _>(
                bm,
                MutableBitmap::with_capacity(length),
                indices,
                scatter_size
            ),

            Column::String(column) => Self::scatter_scalars::<StringType, _>(
                column,
                StringColumnBuilder::with_capacity(length, 0),
                indices,
                scatter_size
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
                let columns = c.column.scatter(indices, scatter_size);
                let validitys = Self::scatter_scalars::<BooleanType, _>(
                    &c.validity,
                    MutableBitmap::with_capacity(length),
                    indices,
                    scatter_size,
                );
                columns
                    .iter()
                    .zip(validitys.iter())
                    .map(|(column, validity)| {
                        Column::Nullable(Box::new(NullableColumn {
                            column: column.clone(),
                            validity: BooleanType::try_downcast_column(validity).unwrap(),
                        }))
                    })
                    .collect()
            }
            Column::Tuple { fields, .. } => {
                let fields_vs: Vec<Vec<Column>> = fields
                    .iter()
                    .map(|c| c.scatter(indices, scatter_size))
                    .collect();

                (0..scatter_size)
                    .map(|index| {
                        let mut columns = Vec::with_capacity(fields.len());
                        let mut len = 0;
                        for field in fields_vs.iter() {
                            len = field[index].len();
                            columns.push(field[index].clone());
                        }

                        Column::Tuple {
                            fields: columns,
                            len,
                        }
                    })
                    .collect()
            }
        })
    }

    fn scatter_scalars<T: ValueType, I: Index>(
        col: &T::Column,
        builder: T::ColumnBuilder,
        indices: &[I],
        scatter_size: usize,
    ) -> Vec<Self> {
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
