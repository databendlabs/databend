// Copyright 2021 Datafuse Labs
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

use crate::types::array::ArrayColumn;
use crate::types::array::ArrayColumnBuilder;
use crate::types::bitmap::BitmapType;
use crate::types::decimal::Decimal128Type;
use crate::types::decimal::Decimal256Type;
use crate::types::decimal::DecimalColumn;
use crate::types::map::KvColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::ArrayType;
use crate::types::BooleanType;
use crate::types::MapType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::ValueType;
use crate::types::VariantType;
use crate::with_number_mapped_type;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::Value;

impl DataBlock {
    pub fn take_compacted_indices(&self, indices: &[(u32, u32)], row_num: usize) -> Result<Self> {
        if indices.is_empty() {
            return Ok(self.slice(0..0));
        }

        // Each item in the `indices` consists of an `index` and a `cnt`, the sum
        // of the `cnt` must be equal to the `row_num`.
        debug_assert_eq!(
            indices.iter().fold(0, |acc, &(_, x)| acc + x as usize),
            row_num
        );

        let after_columns = self
            .columns()
            .iter()
            .map(|entry| match &entry.value {
                Value::Scalar(s) => BlockEntry {
                    data_type: entry.data_type.clone(),
                    value: Value::Scalar(s.clone()),
                },
                Value::Column(c) => BlockEntry {
                    data_type: entry.data_type.clone(),
                    value: Value::Column(Column::take_compacted_indices(c, indices, row_num)),
                },
            })
            .collect();

        Ok(DataBlock::new(after_columns, row_num))
    }
}

impl Column {
    pub fn take_compacted_indices(&self, indices: &[(u32, u32)], row_num: usize) -> Self {
        match self {
            Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {
                self.slice(0..row_num)
            }
            Column::Number(column) => with_number_mapped_type!(|NUM_TYPE| match column {
                NumberColumn::NUM_TYPE(values) =>
                    Self::take_compacted_arg_types::<NumberType<NUM_TYPE>>(values, indices, row_num),
            }),
            Column::Decimal(column) => match column {
                DecimalColumn::Decimal128(values, size) => {
                    let mut builder = Decimal128Type::create_builder(row_num, &[]);
                    for (index, cnt) in indices {
                        let item = unsafe {
                            Decimal128Type::index_column_unchecked(values, *index as usize)
                        };
                        for _ in 0..*cnt {
                            Decimal128Type::push_item(&mut builder, item);
                        }
                    }
                    let column = Decimal128Type::build_column(builder);
                    Column::Decimal(DecimalColumn::Decimal128(column, *size))
                }
                DecimalColumn::Decimal256(values, size) => {
                    let mut builder = Decimal256Type::create_builder(row_num, &[]);
                    for (index, cnt) in indices {
                        let item = unsafe {
                            Decimal256Type::index_column_unchecked(values, *index as usize)
                        };
                        for _ in 0..*cnt {
                            Decimal256Type::push_item(&mut builder, item);
                        }
                    }
                    let column = Decimal256Type::build_column(builder);
                    Column::Decimal(DecimalColumn::Decimal256(column, *size))
                }
            },
            Column::Boolean(bm) => {
                Self::take_compacted_arg_types::<BooleanType>(bm, indices, row_num)
            }
            Column::String(column) => {
                Self::take_compacted_arg_types::<StringType>(column, indices, row_num)
            }
            Column::Timestamp(column) => {
                let ts =
                    Self::take_compacted_arg_types::<NumberType<i64>>(column, indices, row_num)
                        .into_number()
                        .unwrap()
                        .into_int64()
                        .unwrap();
                Column::Timestamp(ts)
            }
            Column::Date(column) => {
                let d = Self::take_compacted_arg_types::<NumberType<i32>>(column, indices, row_num)
                    .into_number()
                    .unwrap()
                    .into_int32()
                    .unwrap();
                Column::Date(d)
            }
            Column::Array(column) => {
                let mut offsets = Vec::with_capacity(row_num + 1);
                offsets.push(0);
                let builder = ColumnBuilder::with_capacity(&column.values.data_type(), row_num);
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::take_compacted_value_types::<ArrayType<AnyType>>(column, builder, indices)
            }
            Column::Map(column) => {
                let mut offsets = Vec::with_capacity(row_num + 1);
                offsets.push(0);
                let builder = ColumnBuilder::from_column(
                    ColumnBuilder::with_capacity(&column.values.data_type(), row_num).build(),
                );
                let (key_builder, val_builder) = match builder {
                    ColumnBuilder::Tuple(fields) => (fields[0].clone(), fields[1].clone()),
                    _ => unreachable!(),
                };
                let builder = KvColumnBuilder {
                    keys: key_builder,
                    values: val_builder,
                };
                let builder = ArrayColumnBuilder { builder, offsets };
                let column = ArrayColumn::try_downcast(column).unwrap();
                Self::take_compacted_value_types::<MapType<AnyType, AnyType>>(
                    &column, builder, indices,
                )
            }
            Column::Bitmap(column) => {
                Self::take_compacted_arg_types::<BitmapType>(column, indices, row_num)
            }
            Column::Nullable(c) => {
                let column = c.column.take_compacted_indices(indices, row_num);
                let validity =
                    Self::take_compacted_arg_types::<BooleanType>(&c.validity, indices, row_num);
                Column::Nullable(Box::new(NullableColumn {
                    column,
                    validity: BooleanType::try_downcast_column(&validity).unwrap(),
                }))
            }
            Column::Tuple(fields) => {
                let fields = fields
                    .iter()
                    .map(|c| c.take_compacted_indices(indices, row_num))
                    .collect();
                Column::Tuple(fields)
            }
            Column::Variant(column) => {
                Self::take_compacted_arg_types::<VariantType>(column, indices, row_num)
            }
        }
    }

    fn take_compacted_arg_types<T: ArgType>(
        col: &T::Column,
        indices: &[(u32, u32)],
        row_num: usize,
    ) -> Column {
        let mut builder = T::create_builder(row_num, &[]);
        for (index, cnt) in indices {
            for _ in 0..*cnt {
                T::push_item(&mut builder, unsafe {
                    T::index_column_unchecked(col, *index as usize)
                });
            }
        }
        let column = T::build_column(builder);
        T::upcast_column(column)
    }

    fn take_compacted_value_types<T: ValueType>(
        col: &T::Column,
        mut builder: T::ColumnBuilder,
        indices: &[(u32, u32)],
    ) -> Column {
        for (index, cnt) in indices {
            for _ in 0..*cnt {
                T::push_item(&mut builder, unsafe {
                    T::index_column_unchecked(col, *index as usize)
                });
            }
        }
        T::upcast_column(T::build_column(builder))
    }
}
