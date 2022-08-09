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
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::types::Index;
use common_exception::Result;

use crate::types::array::ArrayColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::string::StringColumnBuilder;
use crate::types::AnyType;
use crate::types::ArrayType;
use crate::types::BooleanType;
use crate::types::StringType;
use crate::types::ValueType;
use crate::with_number_type;
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
        with_number_type!(SRC_TYPE, match self {
            Column::SRC_TYPE(values) => {
                Column::SRC_TYPE(Self::take_primitives(values, indices))
            }
            Column::Null { .. } | Column::EmptyArray { .. } => self.slice(0..length),

            Column::Boolean(bm) => Self::take_scalars::<BooleanType, _>(
                bm,
                MutableBitmap::with_capacity(length),
                indices
            ),
            Column::String(column) => Self::take_scalars::<StringType, _>(
                column,
                StringColumnBuilder::with_capacity(length, 0),
                indices
            ),
            Column::Array(column) => {
                let mut builder = ArrayColumnBuilder::<AnyType>::from_column(column.slice(0..0));
                builder.reserve(length);
                Self::take_scalars::<ArrayType<AnyType>, _>(column, builder, indices)
            }
            Column::Nullable(c) => {
                let column = c.column.take(indices);
                let validity = Self::take_scalars::<BooleanType, _>(
                    &c.validity,
                    MutableBitmap::with_capacity(length),
                    indices,
                );
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

    fn take_scalars<T: ValueType, I: Index>(
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

    fn take_primitives<T: Copy, I: Index>(col: &Buffer<T>, indices: &[I]) -> Buffer<T> {
        let mut vs: Vec<T> = Vec::with_capacity(indices.len());
        let mut dst = vs.as_mut_ptr();
        for index in indices {
            unsafe {
                let e = col[index.to_usize()];
                dst.write(e);
                dst = dst.add(1);
            }
        }
        unsafe { vs.set_len(indices.len()) };
        vs.into()
    }
}
