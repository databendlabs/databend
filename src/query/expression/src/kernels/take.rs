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

use common_arrow::arrow::types::Index;
use common_exception::Result;

use crate::types::array::ArrayColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::timestamp::TimestampColumn;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::ArrayType;
use crate::types::BooleanType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::ValueType;
use crate::with_number_mapped_type;
use crate::Chunk;
use crate::Column;
use crate::Value;

impl Chunk {
    pub fn take<I: Index>(self, indices: &[I]) -> Result<Self> {
        if indices.is_empty() {
            return Ok(self.slice(0..0));
        }

        let mut after_columns = Vec::with_capacity(self.num_columns());
        for value in self.columns() {
            match value {
                Value::Scalar(v) => after_columns.push(Value::Scalar(v.clone())),
                Value::Column(c) => after_columns.push(Value::Column(Column::take(c, indices))),
            }
        }
        Ok(Chunk::new(after_columns, indices.len()))
    }
}

impl Column {
    pub fn take<I: Index>(&self, indices: &[I]) -> Self {
        let length = indices.len();
        with_number_mapped_type!(NUM_TYPE, match self {
            Column::NUM_TYPE(values) => {
                Self::take_arg_types::<NumberType<NUM_TYPE>, _>(values, indices)
            }
            Column::Null { .. } | Column::EmptyArray { .. } => self.slice(0..length),

            Column::Boolean(bm) => Self::take_arg_types::<BooleanType, _>(bm, indices),
            Column::String(column) => Self::take_arg_types::<StringType, _>(column, indices),
            Column::Timestamp(column) => {
                let ts = Self::take_arg_types::<NumberType<i64>, _>(&column.ts, indices)
                    .into_int64()
                    .unwrap();
                Column::Timestamp(TimestampColumn {
                    ts,
                    precision: column.precision,
                })
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
        })
    }

    fn take_arg_types<T: ArgType, I: Index>(col: &T::Column, indices: &[I]) -> Column {
        let col = T::column_from_ref_iter(
            indices
                .iter()
                .map(|index| unsafe { T::index_column_unchecked(col, index.to_usize()) }),
            &[],
        );
        T::upcast_column(col)
    }

    fn take_value_types<T: ValueType, I: Index>(
        col: &T::Column,
        mut builder: T::ColumnBuilder,
        indices: &[I],
    ) -> Column {
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
