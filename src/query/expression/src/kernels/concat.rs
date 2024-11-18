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

use std::iter::TrustedLen;
use std::sync::Arc;

use arrow_array::Array;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use ethnum::i256;
use itertools::Itertools;

use crate::types::array::ArrayColumnBuilder;
use crate::types::decimal::Decimal;
use crate::types::decimal::DecimalColumn;
use crate::types::map::KvColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::AnyType;
use crate::types::ArrayType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::DecimalType;
use crate::types::MapType;
use crate::types::NumberType;
use crate::types::TimestampType;
use crate::types::ValueType;
use crate::with_decimal_mapped_type;
use crate::with_number_mapped_type;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::Value;

impl DataBlock {
    pub fn concat(blocks: &[DataBlock]) -> Result<DataBlock> {
        if blocks.is_empty() {
            return Err(ErrorCode::EmptyData("Can't concat empty blocks"));
        }
        let block_refs = blocks.iter().collect::<Vec<_>>();

        if blocks.len() == 1 {
            return Ok(blocks[0].clone());
        }

        let num_columns = blocks[0].num_columns();
        let mut concat_columns = Vec::with_capacity(num_columns);
        for i in 0..num_columns {
            concat_columns.push(BlockEntry::new(
                blocks[0].get_by_offset(i).data_type.clone(),
                Self::concat_columns(&block_refs, i)?,
            ))
        }

        let num_rows = blocks.iter().map(|c| c.num_rows()).sum();

        Ok(DataBlock::new(concat_columns, num_rows))
    }

    pub fn concat_columns(blocks: &[&DataBlock], column_index: usize) -> Result<Value<AnyType>> {
        debug_assert!(
            blocks
                .iter()
                .map(|block| &block.get_by_offset(column_index).data_type)
                .all_equal()
        );

        let entry0 = blocks[0].get_by_offset(column_index);
        if matches!(entry0.value, Value::Scalar(_))
            && blocks
                .iter()
                .all(|b| b.get_by_offset(column_index) == entry0)
        {
            return Ok(entry0.value.clone());
        }

        let columns_iter = blocks.iter().map(|block| {
            let entry = &block.get_by_offset(column_index);
            match &entry.value {
                Value::Scalar(s) => {
                    ColumnBuilder::repeat(&s.as_ref(), block.num_rows(), &entry.data_type).build()
                }
                Value::Column(c) => c.clone(),
            }
        });
        Ok(Value::Column(Column::concat_columns(columns_iter)?))
    }
}

impl Column {
    pub fn concat_columns<I: Iterator<Item = Column> + TrustedLen + Clone>(
        columns: I,
    ) -> Result<Column> {
        let (_, size) = columns.size_hint();
        match size {
            Some(0) => Err(ErrorCode::EmptyData("Can't concat empty columns")),
            _ => Self::concat_columns_impl(columns),
        }
    }

    pub fn concat_columns_impl<I: Iterator<Item = Column> + TrustedLen + Clone>(
        columns: I,
    ) -> Result<Column> {
        let mut columns_iter_clone = columns.clone();
        let first_column = match columns_iter_clone.next() {
            // Even if `columns.size_hint()`'s upper bound is `Some(a)` (a != 0),
            // it's possible that `columns`'s len is 0.
            None => return Err(ErrorCode::EmptyData("Can't concat empty columns")),
            Some(col) => col,
        };
        let capacity = columns_iter_clone.fold(first_column.len(), |acc, x| acc + x.len());
        let column = match first_column {
            Column::Null { .. } => Column::Null { len: capacity },
            Column::EmptyArray { .. } => Column::EmptyArray { len: capacity },
            Column::EmptyMap { .. } => Column::EmptyMap { len: capacity },
            Column::Number(col) => with_number_mapped_type!(|NUM_TYPE| match col {
                NumberColumn::NUM_TYPE(_) => {
                    type NType = NumberType<NUM_TYPE>;
                    let buffer = Self::concat_primitive_types(
                        columns.map(|col| NType::try_downcast_column(&col).unwrap()),
                        capacity,
                    );
                    NType::upcast_column(buffer)
                }
            }),
            Column::Decimal(col) => with_decimal_mapped_type!(|DECIMAL_TYPE| match col {
                DecimalColumn::DECIMAL_TYPE(_, size) => {
                    type DType = DecimalType<DECIMAL_TYPE>;
                    let buffer = Self::concat_primitive_types(
                        columns.map(|col| DType::try_downcast_column(&col).unwrap()),
                        capacity,
                    );
                    DECIMAL_TYPE::upcast_column(buffer, size)
                }
            }),
            Column::Boolean(_) => Column::Boolean(Self::concat_boolean_types(
                columns.map(|col| col.into_boolean().unwrap()),
                capacity,
            )),
            Column::Timestamp(_) => {
                let buffer = Self::concat_primitive_types(
                    columns.map(|col| TimestampType::try_downcast_column(&col).unwrap()),
                    capacity,
                );
                Column::Timestamp(buffer)
            }
            Column::Date(_) => {
                let buffer = Self::concat_primitive_types(
                    columns.map(|col| DateType::try_downcast_column(&col).unwrap()),
                    capacity,
                );
                Column::Date(buffer)
            }
            Column::Array(col) => {
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                let builder = ColumnBuilder::with_capacity(&col.values.data_type(), capacity);
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::concat_value_types::<ArrayType<AnyType>>(builder, columns)
            }
            Column::Map(col) => {
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                let builder = ColumnBuilder::from_column(
                    ColumnBuilder::with_capacity(&col.values.data_type(), capacity).build(),
                );
                let (key_builder, val_builder) = match builder {
                    ColumnBuilder::Tuple(fields) => (fields[0].clone(), fields[1].clone()),
                    ty => unreachable!("ty: {}", ty.data_type()),
                };
                let builder = KvColumnBuilder {
                    keys: key_builder,
                    values: val_builder,
                };
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::concat_value_types::<MapType<AnyType, AnyType>>(builder, columns)
            }
            Column::Nullable(_) => {
                let column: Vec<Column> = columns
                    .clone()
                    .map(|col| col.into_nullable().unwrap().column)
                    .collect();
                let column = Self::concat_columns_impl(column.into_iter())?;
                let validity = Column::Boolean(Self::concat_boolean_types(
                    columns.map(|col| col.into_nullable().unwrap().validity),
                    capacity,
                ));
                let validity = BooleanType::try_downcast_column(&validity).unwrap();
                NullableColumn::new_column(column, validity)
            }
            Column::Tuple(fields) => {
                let fields = (0..fields.len())
                    .map(|idx| {
                        let column: Vec<Column> = columns
                            .clone()
                            .map(|col| col.into_tuple().unwrap()[idx].clone())
                            .collect();
                        Self::concat_columns_impl(column.into_iter())
                    })
                    .collect::<Result<_>>()?;
                Column::Tuple(fields)
            }
            Column::Variant(_)
            | Column::Geometry(_)
            | Column::Geography(_)
            | Column::Binary(_)
            | Column::String(_)
            | Column::Bitmap(_) => {
                Self::concat_use_arrow(columns, first_column.data_type(), capacity)
            }
        };
        Ok(column)
    }

    pub fn concat_primitive_types<T>(
        cols: impl Iterator<Item = Buffer<T>>,
        num_rows: usize,
    ) -> Buffer<T>
    where
        T: Copy,
    {
        let mut builder: Vec<T> = Vec::with_capacity(num_rows);
        for col in cols {
            builder.extend(col.iter());
        }
        builder.into()
    }

    pub fn concat_use_arrow(
        cols: impl Iterator<Item = Column>,
        data_type: DataType,
        _num_rows: usize,
    ) -> Column {
        let arrays: Vec<Arc<dyn Array>> = cols.map(|c| c.into_arrow_rs()).collect();
        let arrays = arrays.iter().map(|c| c.as_ref()).collect::<Vec<_>>();
        let result = arrow_select::concat::concat(&arrays).unwrap();
        Column::from_arrow_rs(result, &data_type).unwrap()
    }

    pub fn concat_boolean_types(bitmaps: impl Iterator<Item = Bitmap>, num_rows: usize) -> Bitmap {
        let cols = bitmaps.map(|bitmap| Column::Boolean(bitmap));
        Self::concat_use_arrow(cols, DataType::Boolean, num_rows)
            .into_boolean()
            .unwrap()
    }

    fn concat_value_types<T: ValueType>(
        mut builder: T::ColumnBuilder,
        columns: impl Iterator<Item = Column>,
    ) -> Column {
        let columns = columns.map(|c| T::try_downcast_column(&c).unwrap());
        for col in columns {
            T::append_column(&mut builder, &col);
        }
        T::upcast_column(T::build_column(builder))
    }
}
