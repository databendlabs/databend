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

use common_exception::Result;

use crate::types::array::ArrayColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::ArrayType;
use crate::types::BooleanType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::ValueType;
use crate::types::VariantType;
use crate::with_number_mapped_type;
use crate::Chunk;
use crate::ChunkEntry;
use crate::Column;
use crate::ColumnIndex;
use crate::Value;

impl<Index: ColumnIndex> Chunk<Index> {
    pub fn take<I>(self, indices: &[I]) -> Result<Self>
    where I: common_arrow::arrow::types::Index {
        if indices.is_empty() {
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
                    value: Value::Column(Column::take(c, indices)),
                },
            })
            .collect();

        Ok(Chunk::new(after_columns, indices.len()))
    }
}

impl Column {
    pub fn take<I>(&self, indices: &[I]) -> Self
    where I: common_arrow::arrow::types::Index {
        let length = indices.len();
        match self {
            Column::Null { .. } | Column::EmptyArray { .. } => self.slice(0..length),
            Column::Number(column) => with_number_mapped_type!(|NUM_TYPE| match column {
                NumberColumn::NUM_TYPE(values) =>
                    Self::take_arg_types::<NumberType<NUM_TYPE>, _>(values, indices),
            }),
            Column::Boolean(bm) => Self::take_arg_types::<BooleanType, _>(bm, indices),
            Column::String(column) => Self::take_arg_types::<StringType, _>(column, indices),
            Column::Timestamp(column) => {
                let ts = Self::take_arg_types::<NumberType<i64>, _>(column, indices)
                    .into_number()
                    .unwrap()
                    .into_int64()
                    .unwrap();
                Column::Timestamp(ts)
            }
            Column::Date(column) => {
                let d = Self::take_arg_types::<NumberType<i32>, _>(column, indices)
                    .into_number()
                    .unwrap()
                    .into_int32()
                    .unwrap();
                Column::Date(d)
            }
            Column::Array(column) => {
                let mut builder = ArrayColumnBuilder::<AnyType>::from_column(column.slice(0..0));
                builder.reserve(length);
                Self::take_value_types::<ArrayType<AnyType>, _>(column, builder, indices)
            }
            Column::Nullable(c) => {
                let column = c.column.take(indices);
                let validity = Self::take_arg_types::<BooleanType, _>(&c.validity, indices);
                Column::Nullable(Box::new(NullableColumn {
                    column,
                    validity: BooleanType::try_downcast_column(&validity).unwrap(),
                }))
            }
            Column::Tuple { fields, .. } => {
                let fields = fields.iter().map(|c| c.take(indices)).collect();
                Column::Tuple {
                    fields,
                    len: indices.len(),
                }
            }
            Column::Variant(column) => Self::take_arg_types::<VariantType, _>(column, indices),
        }
    }

    fn take_arg_types<T: ArgType, I>(col: &T::Column, indices: &[I]) -> Column
    where I: common_arrow::arrow::types::Index {
        let col = T::column_from_ref_iter(
            indices
                .iter()
                .map(|index| unsafe { T::index_column_unchecked(col, index.to_usize()) }),
            &[],
        );
        T::upcast_column(col)
    }

    fn take_value_types<T: ValueType, I>(
        col: &T::Column,
        mut builder: T::ColumnBuilder,
        indices: &[I],
    ) -> Column
    where
        I: common_arrow::arrow::types::Index,
    {
        unsafe {
            for index in indices {
                T::push_item(
                    &mut builder,
                    T::index_column_unchecked(col, index.to_usize()),
                )
            }
        }
        T::upcast_column(T::build_column(builder))
    }
}
