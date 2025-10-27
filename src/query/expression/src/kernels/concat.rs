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
use itertools::Itertools;

use crate::types::array::ArrayColumnBuilder;
use crate::types::decimal::Decimal;
use crate::types::decimal::DecimalColumn;
use crate::types::i256;
use crate::types::map::KvColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::opaque::OpaqueType;
use crate::types::timestamp_tz::TimestampTzType;
use crate::types::vector::VectorColumnBuilder;
use crate::types::AccessType;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::ArrayType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::DecimalType;
use crate::types::IntervalType;
use crate::types::MapType;
use crate::types::NumberType;
use crate::types::TimestampType;
use crate::types::ValueType;
use crate::types::VectorColumn;
use crate::types::VectorDataType;
use crate::with_decimal_mapped_type;
use crate::with_number_mapped_type;
use crate::with_opaque_size;
use crate::with_vector_number_type;
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

        if blocks.len() == 1 {
            return Ok(blocks[0].clone());
        }

        let num_rows = blocks.iter().map(|c| c.num_rows()).sum();
        let num_columns = blocks[0].num_columns();
        let mut concat_columns = Vec::with_capacity(num_columns);
        for i in 0..num_columns {
            let entries = blocks
                .iter()
                .map(|b| b.get_by_offset(i))
                .collect::<Vec<_>>();
            concat_columns.push(BlockEntry::new(Self::concat_entries(&entries)?, || {
                (blocks[0].data_type(i), num_rows)
            }))
        }

        Ok(DataBlock::new(concat_columns, num_rows))
    }

    fn concat_entries(entries: &[&BlockEntry]) -> Result<Value<AnyType>> {
        debug_assert!(entries.iter().map(|entry| entry.data_type()).all_equal());

        let entry0 = entries[0];
        if entry0.as_scalar().is_some() && entries.iter().all_equal() {
            return Ok(entry0.value());
        }

        let columns_iter = entries.iter().map(|entry| entry.to_column());
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

    fn concat_columns_impl<I: Iterator<Item = Column> + TrustedLen + Clone>(
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
        let data_type = first_column.data_type();
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
            Column::Interval(_) => {
                let buffer = Self::concat_primitive_types(
                    columns.map(|col| IntervalType::try_downcast_column(&col).unwrap()),
                    capacity,
                );
                Column::Interval(buffer)
            }
            Column::TimestampTz(_) => {
                let buffer = Self::concat_primitive_types(
                    columns.map(|col| TimestampTzType::try_downcast_column(&col).unwrap()),
                    capacity,
                );
                Column::TimestampTz(buffer)
            }
            Column::Opaque(first) => {
                with_opaque_size!(|N| match first.size() {
                    N => Self::concat_opaque_column::<_, N>(columns, capacity),
                    _ => unreachable!("Unsupported Opaque size: {}", first.size()),
                })
            }
            Column::Array(col) => {
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                let builder = ColumnBuilder::with_capacity(&col.values().data_type(), capacity);
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::concat_value_types::<ArrayType<AnyType>>(builder, columns, &data_type)
            }
            Column::Map(col) => {
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                let builder = ColumnBuilder::from_column(
                    ColumnBuilder::with_capacity(&col.values().data_type(), capacity).build(),
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
                Self::concat_value_types::<MapType<AnyType, AnyType>>(builder, columns, &data_type)
            }
            Column::Nullable(_) => {
                let (inner, validity): (Vec<_>, Vec<_>) = columns
                    .map(|col| col.into_nullable().unwrap().destructure())
                    .unzip(); // break the recursion type
                let column = Self::concat_columns_impl(inner.into_iter())?;
                let validity = Self::concat_boolean_types(validity.into_iter(), capacity);
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
            Column::Vector(col) => with_vector_number_type!(|NUM_TYPE| match col {
                VectorColumn::NUM_TYPE((_, dimension)) => {
                    let vector_ty = VectorDataType::NUM_TYPE(dimension as u64);
                    let mut builder = VectorColumnBuilder::with_capacity(&vector_ty, capacity);
                    for column in columns {
                        let vector_column = column.as_vector().unwrap();
                        if vector_column.dimension() as usize != dimension {
                            return Err(ErrorCode::Internal(format!(
                                "Can't concat vector columns with different dimensions, {} and {}",
                                dimension,
                                vector_column.dimension()
                            )));
                        }
                        builder.append_column(&vector_column);
                    }
                    Column::Vector(builder.build())
                }
            }),
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

    fn concat_opaque_column<I, const N: usize>(columns: I, capacity: usize) -> Column
    where I: Iterator<Item = Column> + TrustedLen + Clone {
        let buffer = Self::concat_primitive_types(
            columns.map(|col| OpaqueType::<N>::try_downcast_column(&col).unwrap()),
            capacity,
        );
        OpaqueType::<N>::upcast_column_with_type(buffer, &DataType::Opaque(N))
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
        let cols = bitmaps.map(Column::Boolean);
        Self::concat_use_arrow(cols, DataType::Boolean, num_rows)
            .into_boolean()
            .unwrap()
    }

    fn concat_value_types<T: ValueType>(
        mut builder: T::ColumnBuilder,
        columns: impl Iterator<Item = Column>,
        data_type: &DataType,
    ) -> Column {
        let columns = columns.map(|c| T::try_downcast_column(&c).unwrap());
        for col in columns {
            T::append_column(&mut builder, &col);
        }
        T::upcast_column_with_type(T::build_column(builder), data_type)
    }
}
