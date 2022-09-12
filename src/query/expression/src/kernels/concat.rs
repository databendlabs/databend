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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::types::array::ArrayColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::string::StringColumnBuilder;
use crate::types::timestamp::TimestampColumnBuilder;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::ArrayType;
use crate::types::BooleanType;
use crate::types::EmptyArrayType;
use crate::types::NullType;
use crate::types::NullableType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::TimestampType;
use crate::types::ValueType;
use crate::with_number_mapped_type;
use crate::Chunk;
use crate::Column;
use crate::Value;

impl Chunk {
    pub fn concat(chunks: &[Chunk]) -> Result<Chunk> {
        if chunks.is_empty() {
            return Err(ErrorCode::EmptyData("Can't concat empty chunks"));
        }

        if chunks.len() == 1 {
            return Ok(chunks[0].clone());
        }

        let num_rows = chunks.iter().map(|c| c.num_rows()).sum();
        let mut concat_columns = Vec::with_capacity(chunks[0].num_columns());
        for i in 0..chunks[0].num_columns() {
            let mut columns = Vec::with_capacity(chunks.len());
            for chunk in chunks.iter() {
                let c = &chunk.columns()[i];
                match c {
                    Value::Scalar(s) => {
                        let builder = s.as_ref().repeat(chunk.num_rows());
                        let col = builder.build();
                        columns.push(col);
                    }
                    Value::Column(c) => columns.push(c.clone()),
                }
            }
            let c = Column::concat(&columns);
            concat_columns.push(Value::Column(c));
        }
        Ok(Chunk::new(concat_columns, num_rows))
    }
}

impl Column {
    pub fn concat(columns: &[Column]) -> Column {
        if columns.len() == 1 {
            return columns[0].clone();
        }
        let capacity = columns.iter().map(|c| c.len()).sum();

        match &columns[0] {
            Column::Null { .. } => Self::concat_arg_types::<NullType>(columns),
            Column::EmptyArray { .. } => Self::concat_arg_types::<EmptyArrayType>(columns),
            Column::Number(col) => with_number_mapped_type!(NUM_TYPE, match col {
                NumberColumn::NUM_TYPE(_) => {
                    Self::concat_arg_types::<NumberType<NUM_TYPE>>(columns)
                }
            }),
            Column::Boolean(_) => Self::concat_arg_types::<BooleanType>(columns),
            Column::String(_) => {
                let data_capacity = columns.iter().map(|c| c.memory_size() - c.len() * 8).sum();
                let builder = StringColumnBuilder::with_capacity(capacity, data_capacity);
                Self::concat_value_types::<StringType>(builder, columns)
            }
            Column::Timestamp(_) => {
                let builder = TimestampColumnBuilder::with_capacity(capacity);
                Self::concat_value_types::<TimestampType>(builder, columns)
            }
            Column::Array(col) => {
                let mut builder = ArrayColumnBuilder::<AnyType>::from_column(col.slice(0..0));
                builder.reserve(capacity);
                Self::concat_value_types::<ArrayType<AnyType>>(builder, columns)
            }
            Column::Nullable(_) => {
                let mut bitmaps = Vec::with_capacity(columns.len());
                let mut inners = Vec::with_capacity(columns.len());
                for c in columns {
                    let nullable_column = NullableType::<AnyType>::try_downcast_column(c).unwrap();
                    inners.push(nullable_column.column);
                    bitmaps.push(Column::Boolean(nullable_column.validity));
                }

                let column = Self::concat(&inners);
                let validity = Self::concat_arg_types::<BooleanType>(&bitmaps);
                let validity = BooleanType::try_downcast_column(&validity).unwrap();

                Column::Nullable(Box::new(NullableColumn { column, validity }))
            }
            Column::Tuple { fields, .. } => {
                let fields = (0..fields.len())
                    .map(|idx| {
                        let cs: Vec<Column> = columns
                            .iter()
                            .map(|col| col.as_tuple().unwrap().0[idx].clone())
                            .collect();
                        Self::concat(&cs)
                    })
                    .collect();
                Column::Tuple {
                    fields,
                    len: capacity,
                }
            }
        }
    }

    fn concat_arg_types<T: ArgType>(columns: &[Column]) -> Column {
        let columns: Vec<T::Column> = columns
            .iter()
            .map(|c| T::try_downcast_column(c).unwrap())
            .collect();
        let iter = columns.iter().flat_map(|c| T::iter_column(c));
        let result = T::column_from_ref_iter(iter, &[]);
        T::upcast_column(result)
    }

    fn concat_value_types<T: ValueType>(
        mut builder: T::ColumnBuilder,
        columns: &[Column],
    ) -> Column {
        let columns: Vec<T::Column> = columns
            .iter()
            .map(|c| T::try_downcast_column(c).unwrap())
            .collect();
        for col in columns {
            for item in T::iter_column(&col) {
                T::push_item(&mut builder, item)
            }
        }
        T::upcast_column(T::build_column(builder))
    }
}
