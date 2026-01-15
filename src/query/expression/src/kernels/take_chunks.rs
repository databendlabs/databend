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

use std::sync::Arc;

use binary::BinaryColumnBuilder;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_hashtable::RowPtr;
use itertools::Itertools;
use string::StringColumnBuilder;

use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::ColumnVec;
use crate::ColumnView;
use crate::DataBlock;
use crate::RepeatIndex;
use crate::Scalar;
use crate::TakeIndex;
use crate::Value;
use crate::block_vec::DataBlockVec;
use crate::kernels::take::BIT_MASK;
use crate::types::array::ArrayColumnBuilder;
use crate::types::binary::BinaryColumn;
use crate::types::bitmap::BitmapType;
use crate::types::decimal::DecimalColumn;
use crate::types::decimal::DecimalColumnVec;
use crate::types::geography::GeographyColumn;
use crate::types::geometry::GeometryType;
use crate::types::map::KvColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::nullable::NullableColumnVec;
use crate::types::number::NumberColumn;
use crate::types::opaque::OpaqueColumn;
use crate::types::opaque::OpaqueColumnVec;
use crate::types::opaque::OpaqueType;
use crate::types::string::StringColumn;
use crate::types::timestamp_tz::TimestampTzType;
use crate::types::vector::VectorColumnBuilder;
use crate::types::*;
use crate::with_decimal_type;
use crate::with_number_mapped_type;
use crate::with_opaque_mapped_type;
use crate::with_opaque_size;
use crate::with_opaque_type;
use crate::with_vector_number_type;

// Block idx, row idx in the block, repeat times
pub type BlockRowIndex = (u32, u32, usize);
// // Block idx, row idx
pub type BlockIndex = (u32, u32);

// block_index, start, len
pub type MergeSlice = (usize, usize, usize);

impl DataBlock {
    pub fn take_blocks(blocks: &[DataBlock], indices: &[BlockIndex]) -> Self {
        debug_assert!(!blocks.is_empty());

        let num_columns = blocks[0].num_columns();
        let result_size = indices.len();

        let result_columns = (0..num_columns)
            .map(|index| {
                let entries = blocks
                    .iter()
                    .map(|block| block.get_by_offset(index))
                    .collect_vec();

                let ty = entries[0].data_type();
                if ty.is_null() {
                    return BlockEntry::new_const_column(ty, Scalar::Null, result_size);
                }

                if let Some((scalar, data_type, _)) = entries[0].as_const() {
                    let all_same_scalar =
                        entries.iter().copied().map(BlockEntry::value).all_equal();
                    if all_same_scalar {
                        return BlockEntry::new_const_column(
                            data_type.clone(),
                            scalar.clone(),
                            result_size,
                        );
                    }
                }

                let full_columns: Vec<_> =
                    entries.iter().copied().map(BlockEntry::to_column).collect();

                Column::take_column_indices(&full_columns, indices).into()
            })
            .collect();

        DataBlock::new(result_columns, result_size)
    }

    pub fn take_column_vec(
        build_columns: &[ColumnVec],
        build_columns_data_type: &[DataType],
        indices: &[RowPtr],
    ) -> Self {
        let result_size = indices.len();
        let result_entries =
            build_columns
                .iter()
                .zip(build_columns_data_type)
                .map(|(columns, data_type)| {
                    Column::take_column_vec_indices(columns, data_type.clone(), indices).into()
                });
        DataBlock::from_iter(result_entries, result_size)
    }

    pub fn take_by_slices_limit_from_blocks(
        blocks: &[DataBlock],
        slices: &[MergeSlice],
        limit: Option<usize>,
    ) -> Self {
        debug_assert!(!blocks.is_empty());
        let num_rows = limit
            .unwrap_or(usize::MAX)
            .min(slices.iter().map(|(_, _, c)| *c).sum());
        let result_size = num_rows.min(blocks.iter().map(|c| c.num_rows()).sum());

        let mut builders = Self::builders(&blocks[0], num_rows);

        let mut remain = num_rows;
        for (block_index, start, len) in slices {
            let block_columns = blocks[*block_index].columns();
            let len = (*len).min(remain);
            remain -= len;

            for (col_index, builder) in builders.iter_mut().enumerate() {
                Self::push_to_builder(builder, *start, len, &block_columns[col_index].value());
            }
            if remain == 0 {
                break;
            }
        }

        Self::build_block(builders, result_size)
    }

    fn builders(block: &DataBlock, num_rows: usize) -> Vec<ColumnBuilder> {
        block
            .columns()
            .iter()
            .map(|col| ColumnBuilder::with_capacity(&col.data_type(), num_rows))
            .collect::<Vec<_>>()
    }

    fn build_block(builders: Vec<ColumnBuilder>, num_rows: usize) -> DataBlock {
        let result_columns = builders
            .into_iter()
            .map(|b| b.build().into())
            .collect::<Vec<_>>();
        DataBlock::new(result_columns, num_rows)
    }

    fn push_to_builder(
        builder: &mut ColumnBuilder,
        start: usize,
        len: usize,
        value: &Value<AnyType>,
    ) {
        match value {
            Value::Scalar(scalar) => {
                builder.push_repeat(&scalar.as_ref(), len);
            }
            Value::Column(c) => {
                let c = c.slice(start..(start + len));
                builder.append_column(&c);
            }
        }
    }
}

impl Column {
    pub fn take_column_indices(columns: &[Column], indices: &[BlockIndex]) -> Column {
        let result_size = indices.len();
        let data_type = columns[0].data_type();
        match &columns[0] {
            Column::Null { .. } => Column::Null { len: result_size },
            Column::EmptyArray { .. } => Column::EmptyArray { len: result_size },
            Column::EmptyMap { .. } => Column::EmptyMap { len: result_size },
            Column::Number(column) => with_number_mapped_type!(|NUM_TYPE| match column {
                NumberColumn::NUM_TYPE(_) => {
                    let builder = NumberType::<NUM_TYPE>::create_builder(result_size, &[]);
                    Self::take_block_value_types::<NumberType<NUM_TYPE>>(
                        columns, &data_type, builder, indices,
                    )
                }
            }),
            Column::Decimal(column) => with_decimal_type!(|DECIMAL_TYPE| match column {
                DecimalColumn::DECIMAL_TYPE(_, size) => {
                    let columns = columns
                        .iter()
                        .map(|col| match col {
                            Column::Decimal(DecimalColumn::DECIMAL_TYPE(col, _)) => col,
                            _ => unreachable!(),
                        })
                        .collect_vec();
                    let mut builder = Vec::with_capacity(result_size);
                    for &(block_index, row) in indices {
                        let val =
                            unsafe { columns[block_index as usize].get_unchecked(row as usize) };
                        builder.push(*val);
                    }
                    Column::Decimal(DecimalColumn::DECIMAL_TYPE(builder.into(), *size))
                }
            }),
            Column::Boolean(_) => {
                let builder = BooleanType::create_builder(result_size, &[]);
                Self::take_block_value_types::<BooleanType>(columns, &data_type, builder, indices)
            }
            Column::Binary(_) => {
                let builder = BinaryType::create_builder(result_size, &[]);
                Self::take_block_value_types::<BinaryType>(columns, &data_type, builder, indices)
            }
            Column::String(_) => {
                let builder = StringType::create_builder(result_size, &[]);
                Self::take_block_value_types::<StringType>(columns, &data_type, builder, indices)
            }
            Column::Timestamp(_) => {
                let builder = TimestampType::create_builder(result_size, &[]);
                Self::take_block_value_types::<TimestampType>(columns, &data_type, builder, indices)
            }
            Column::TimestampTz(_) => {
                let builder = TimestampTzType::create_builder(result_size, &[]);
                Self::take_block_value_types::<TimestampTzType>(
                    columns, &data_type, builder, indices,
                )
            }
            Column::Date(_) => {
                let builder = DateType::create_builder(result_size, &[]);
                Self::take_block_value_types::<DateType>(columns, &data_type, builder, indices)
            }
            Column::Interval(_) => {
                let builder = IntervalType::create_builder(result_size, &[]);
                Self::take_block_value_types::<IntervalType>(columns, &data_type, builder, indices)
            }
            Column::Array(column) => {
                let mut offsets = Vec::with_capacity(result_size + 1);
                offsets.push(0);
                let builder =
                    ColumnBuilder::with_capacity(&column.values().data_type(), result_size);
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::take_block_value_types::<ArrayType<AnyType>>(
                    columns, &data_type, builder, indices,
                )
            }
            Column::Map(column) => {
                let mut offsets = Vec::with_capacity(result_size + 1);
                offsets.push(0);
                let builder = ColumnBuilder::from_column(
                    ColumnBuilder::with_capacity(&column.values().data_type(), result_size).build(),
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
                Self::take_block_value_types::<MapType<AnyType, AnyType>>(
                    columns, &data_type, builder, indices,
                )
            }
            Column::Bitmap(_) => {
                let builder = BitmapType::create_builder(result_size, &[]);
                Self::take_block_value_types::<BitmapType>(columns, &data_type, builder, indices)
            }
            Column::Nullable(_) => {
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

                let inner_column = Self::take_column_indices(&inner_columns, indices);
                let inner_bitmap = Self::take_column_indices(&inner_bitmaps, indices);
                NullableColumn::new_column(
                    inner_column,
                    BooleanType::try_downcast_column(&inner_bitmap).unwrap(),
                )
            }
            Column::Tuple(first) => {
                let fields = (0..first.len())
                    .map(|idx| {
                        let sub_columns = columns
                            .iter()
                            .map(|c| c.as_tuple().unwrap()[idx].clone())
                            .collect::<Vec<_>>();
                        Self::take_column_indices(&sub_columns, indices)
                    })
                    .collect();
                Column::Tuple(fields)
            }
            Column::Variant(_) => {
                let builder = VariantType::create_builder(result_size, &[]);
                Self::take_block_value_types::<VariantType>(columns, &data_type, builder, indices)
            }
            Column::Geometry(_) => {
                let builder = GeometryType::create_builder(result_size, &[]);
                Self::take_block_value_types::<GeometryType>(columns, &data_type, builder, indices)
            }
            Column::Geography(_) => {
                let builder = GeographyType::create_builder(result_size, &[]);
                Self::take_block_value_types::<GeographyType>(columns, &data_type, builder, indices)
            }
            Column::Vector(col) => with_vector_number_type!(|NUM_TYPE| match col {
                VectorColumn::NUM_TYPE((_, dimension)) => {
                    let vector_ty = VectorDataType::NUM_TYPE(*dimension as u64);
                    let mut builder = VectorColumnBuilder::with_capacity(&vector_ty, result_size);

                    let columns = columns
                        .iter()
                        .map(|col| col.as_vector().unwrap())
                        .collect_vec();

                    for &(block_index, row) in indices {
                        let val =
                            unsafe { columns[block_index as usize].index_unchecked(row as usize) };
                        builder.push(&val);
                    }
                    Column::Vector(builder.build())
                }
            }),
            Column::Opaque(first) => {
                with_opaque_size!(|N| match first.size() {
                    N => {
                        let builder = Vec::with_capacity(result_size);
                        Self::take_block_value_types::<OpaqueType<N>>(
                            columns, &data_type, builder, indices,
                        )
                    }
                    _ => unreachable!("Unsupported Opaque size: {}", first.size()),
                })
            }
        }
    }

    pub fn take_downcast_column_vec(columns: &[Column]) -> ColumnVec {
        match &columns[0] {
            Column::Null { .. } => ColumnVec::Null,
            Column::EmptyArray { .. } => ColumnVec::EmptyArray,
            Column::EmptyMap { .. } => ColumnVec::EmptyMap,
            Column::Number(column) => match column {
                NumberColumn::UInt8(_) => {
                    let columns = columns
                        .iter()
                        .map(|col| <NumberType<u8>>::try_downcast_column(col).unwrap())
                        .collect_vec();
                    ColumnVec::Number(NumberColumnVec::UInt8(columns))
                }
                NumberColumn::UInt16(_) => {
                    let columns = columns
                        .iter()
                        .map(|col| <NumberType<u16>>::try_downcast_column(col).unwrap())
                        .collect_vec();
                    ColumnVec::Number(NumberColumnVec::UInt16(columns))
                }
                NumberColumn::UInt32(_) => {
                    let columns = columns
                        .iter()
                        .map(|col| <NumberType<u32>>::try_downcast_column(col).unwrap())
                        .collect_vec();
                    ColumnVec::Number(NumberColumnVec::UInt32(columns))
                }
                NumberColumn::UInt64(_) => {
                    let columns = columns
                        .iter()
                        .map(|col| <NumberType<u64>>::try_downcast_column(col).unwrap())
                        .collect_vec();
                    ColumnVec::Number(NumberColumnVec::UInt64(columns))
                }
                NumberColumn::Int8(_) => {
                    let columns = columns
                        .iter()
                        .map(|col| <NumberType<i8>>::try_downcast_column(col).unwrap())
                        .collect_vec();
                    ColumnVec::Number(NumberColumnVec::Int8(columns))
                }
                NumberColumn::Int16(_) => {
                    let columns = columns
                        .iter()
                        .map(|col| <NumberType<i16>>::try_downcast_column(col).unwrap())
                        .collect_vec();
                    ColumnVec::Number(NumberColumnVec::Int16(columns))
                }
                NumberColumn::Int32(_) => {
                    let columns = columns
                        .iter()
                        .map(|col| <NumberType<i32>>::try_downcast_column(col).unwrap())
                        .collect_vec();
                    ColumnVec::Number(NumberColumnVec::Int32(columns))
                }
                NumberColumn::Int64(_) => {
                    let columns = columns
                        .iter()
                        .map(|col| <NumberType<i64>>::try_downcast_column(col).unwrap())
                        .collect_vec();
                    ColumnVec::Number(NumberColumnVec::Int64(columns))
                }
                NumberColumn::Float32(_) => {
                    let columns = columns
                        .iter()
                        .map(|col| <NumberType<F32>>::try_downcast_column(col).unwrap())
                        .collect_vec();
                    ColumnVec::Number(NumberColumnVec::Float32(columns))
                }
                NumberColumn::Float64(_) => {
                    let columns = columns
                        .iter()
                        .map(|col| <NumberType<F64>>::try_downcast_column(col).unwrap())
                        .collect_vec();
                    ColumnVec::Number(NumberColumnVec::Float64(columns))
                }
            },
            Column::Decimal(column) => with_decimal_type!(|DECIMAL_TYPE| match column {
                DecimalColumn::DECIMAL_TYPE(_, size) => {
                    let columns = columns
                        .iter()
                        .map(|col| match col {
                            Column::Decimal(DecimalColumn::DECIMAL_TYPE(col, _)) => col.clone(),
                            _ => unreachable!(),
                        })
                        .collect_vec();
                    ColumnVec::Decimal(DecimalColumnVec::DECIMAL_TYPE(columns, *size))
                }
            }),
            Column::Boolean(_) => {
                let columns = columns
                    .iter()
                    .map(|col| BooleanType::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::Boolean(columns)
            }
            Column::Binary(_) => {
                let columns = columns
                    .iter()
                    .map(|col| BinaryType::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::Binary(columns)
            }
            Column::String(_) => {
                let columns = columns
                    .iter()
                    .map(|col| StringType::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::String(columns)
            }
            Column::Timestamp(_) => {
                let columns = columns
                    .iter()
                    .map(|col| TimestampType::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::Timestamp(columns)
            }
            Column::TimestampTz(_) => {
                let columns = columns
                    .iter()
                    .map(|col| TimestampTzType::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::TimestampTz(columns)
            }
            Column::Interval(_) => {
                let columns = columns
                    .iter()
                    .map(|col| IntervalType::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::Interval(columns)
            }
            Column::Date(_) => {
                let columns = columns
                    .iter()
                    .map(|col| DateType::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::Date(columns)
            }
            Column::Array(_) => {
                let columns = columns
                    .iter()
                    .map(|col| <ArrayType<AnyType>>::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::Array(columns)
            }
            Column::Map(_) => {
                let columns = columns
                    .iter()
                    .map(|col| <MapType<AnyType, AnyType>>::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::Map(columns)
            }
            Column::Bitmap(_) => {
                let columns = columns
                    .iter()
                    .map(|col| BitmapType::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::Bitmap(columns)
            }
            Column::Nullable(_) => {
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

                let column = Self::take_downcast_column_vec(&inner_columns);
                let validity = Self::take_downcast_column_vec(&inner_bitmaps);
                ColumnVec::Nullable(Box::new(NullableColumnVec { column, validity }))
            }
            Column::Tuple(first) => {
                let fields: Vec<ColumnVec> = (0..first.len())
                    .map(|idx| {
                        let sub_columns = columns
                            .iter()
                            .map(|c| c.as_tuple().unwrap()[idx].clone())
                            .collect::<Vec<_>>();
                        Self::take_downcast_column_vec(&sub_columns)
                    })
                    .collect();
                ColumnVec::Tuple(fields)
            }
            Column::Variant(_) => {
                let columns = columns
                    .iter()
                    .map(|col| VariantType::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::Variant(columns)
            }
            Column::Geometry(_) => {
                let columns = columns
                    .iter()
                    .map(|col| GeometryType::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::Geometry(columns)
            }
            Column::Geography(_) => {
                let columns = columns
                    .iter()
                    .map(|col| GeographyType::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::Geography(columns)
            }
            Column::Vector(col) => with_vector_number_type!(|NUM_TYPE| match col {
                VectorColumn::NUM_TYPE((_, dimension)) => {
                    let columns = columns
                        .iter()
                        .map(|col| match col {
                            Column::Vector(VectorColumn::NUM_TYPE((col, _))) => col.clone(),
                            _ => unreachable!(),
                        })
                        .collect_vec();
                    ColumnVec::Vector(VectorColumnVec::NUM_TYPE((columns, *dimension)))
                }
            }),
            Column::Opaque(_) => {
                let columns = columns
                    .iter()
                    .map(|col| match col {
                        Column::Opaque(col) => col.clone(),
                        _ => unreachable!(),
                    })
                    .collect_vec();
                with_opaque_type!(|T| match &columns[0] {
                    OpaqueColumn::T(_) => ColumnVec::Opaque(OpaqueColumnVec::T(
                        columns
                            .into_iter()
                            .map(|col| match col {
                                OpaqueColumn::T(col) => col,
                                _ => unreachable!(),
                            })
                            .collect(),
                    )),
                })
            }
        }
    }

    fn take_column_vec_indices(
        columns: &ColumnVec,
        data_type: DataType,
        indices: &[RowPtr],
    ) -> Column {
        let result_size = indices.len();
        match &columns {
            ColumnVec::Null => Column::Null { len: result_size },
            ColumnVec::EmptyArray => Column::EmptyArray { len: result_size },
            ColumnVec::EmptyMap => Column::EmptyMap { len: result_size },
            ColumnVec::Number(column) => with_number_mapped_type!(|NUM_TYPE| match column {
                NumberColumnVec::NUM_TYPE(columns) => {
                    let builder = Self::take_block_vec_primitive_types(columns.as_slice(), indices);
                    NumberType::<NUM_TYPE>::upcast_column(builder.into())
                }
            }),
            ColumnVec::Decimal(column) => with_decimal_type!(|DECIMAL_TYPE| match column {
                DecimalColumnVec::DECIMAL_TYPE(columns, size) => {
                    let builder = Self::take_block_vec_primitive_types(columns.as_slice(), indices);
                    Column::Decimal(DecimalColumn::DECIMAL_TYPE(builder.into(), *size))
                }
            }),
            ColumnVec::Boolean(columns) => Column::Boolean(Self::take_block_vec_boolean_types(
                columns.as_slice(),
                indices,
            )),
            ColumnVec::Binary(columns) => Column::Binary(Self::take_block_vec_binary_types(
                columns.as_slice(),
                indices,
            )),
            ColumnVec::String(columns) => Column::String(Self::take_block_vec_string_types(
                columns.as_slice(),
                indices,
            )),
            ColumnVec::Timestamp(columns) => {
                let builder = Self::take_block_vec_primitive_types(columns.as_slice(), indices);
                Column::Timestamp(builder.into())
            }
            ColumnVec::TimestampTz(columns) => {
                let builder = Self::take_block_vec_primitive_types(columns.as_slice(), indices);
                Column::TimestampTz(builder.into())
            }
            ColumnVec::Date(columns) => {
                let builder = Self::take_block_vec_primitive_types(columns.as_slice(), indices);
                Column::Date(builder.into())
            }
            ColumnVec::Interval(columns) => {
                let builder = Self::take_block_vec_primitive_types(columns.as_slice(), indices);
                Column::Interval(builder.into())
            }
            ColumnVec::Array(columns) => {
                let item_type = data_type.as_array().unwrap();
                let mut offsets = Vec::with_capacity(result_size + 1);
                offsets.push(0);
                let builder = ColumnBuilder::with_capacity(item_type, result_size);
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::take_block_vec_value_types::<ArrayType<AnyType>>(
                    columns, builder, indices, &data_type,
                )
            }
            ColumnVec::Map(columns) => {
                let kv_type = data_type.as_map().unwrap();

                let mut offsets = Vec::with_capacity(result_size + 1);
                offsets.push(0);
                let builder = ColumnBuilder::from_column(
                    ColumnBuilder::with_capacity(kv_type, result_size).build(),
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
                Self::take_block_vec_value_types::<MapType<AnyType, AnyType>>(
                    columns, builder, indices, &data_type,
                )
            }
            ColumnVec::Bitmap(columns) => BitmapType::upcast_column(
                Self::take_block_vec_binary_types(columns.as_slice(), indices),
            ),
            ColumnVec::Nullable(columns) => {
                let inner_data_type = data_type.as_nullable().unwrap();
                let inner_column = Self::take_column_vec_indices(
                    &columns.column,
                    *inner_data_type.clone(),
                    indices,
                );

                let inner_bitmap =
                    Self::take_column_vec_indices(&columns.validity, DataType::Boolean, indices);

                NullableColumn::new_column(
                    inner_column,
                    BooleanType::try_downcast_column(&inner_bitmap).unwrap(),
                )
            }
            ColumnVec::Tuple(columns) => {
                let inner_data_type = data_type.as_tuple().unwrap();
                let fields: Vec<Column> = inner_data_type
                    .iter()
                    .enumerate()
                    .map(|(idx, ty)| {
                        Self::take_column_vec_indices(&columns[idx], ty.clone(), indices)
                    })
                    .collect();

                Column::Tuple(fields)
            }
            ColumnVec::Variant(columns) => VariantType::upcast_column(
                Self::take_block_vec_binary_types(columns.as_slice(), indices),
            ),
            ColumnVec::Geometry(columns) => GeometryType::upcast_column(
                Self::take_block_vec_binary_types(columns.as_slice(), indices),
            ),
            ColumnVec::Geography(columns) => {
                let columns = columns.iter().map(|x| x.0.clone()).collect::<Vec<_>>();
                GeographyType::upcast_column(GeographyColumn(Self::take_block_vec_binary_types(
                    columns.as_slice(),
                    indices,
                )))
            }
            ColumnVec::Vector(columns) => with_vector_number_type!(|NUM_TYPE| match columns {
                VectorColumnVec::NUM_TYPE((values, dimension)) => {
                    let vector_ty = VectorDataType::NUM_TYPE(*dimension as u64);
                    let mut builder = VectorColumnBuilder::with_capacity(&vector_ty, indices.len());
                    let columns = values
                        .into_iter()
                        .map(|vals| VectorColumn::NUM_TYPE((vals.clone(), *dimension)))
                        .collect_vec();
                    for row_ptr in indices {
                        let val = unsafe {
                            columns[row_ptr.chunk_index as usize]
                                .index_unchecked(row_ptr.row_index as usize)
                        };
                        builder.push(&val);
                    }
                    Column::Vector(builder.build())
                }
            }),
            ColumnVec::Opaque(columns) => {
                with_opaque_mapped_type!(|T| match columns {
                    OpaqueColumnVec::T(columns) => {
                        let builder =
                            Self::take_block_vec_primitive_types(columns.as_slice(), indices);
                        OpaqueType::<T>::upcast_column(builder.into())
                    }
                })
            }
        }
    }

    fn take_block_vec_primitive_types<L, T>(col: &L, indices: &[RowPtr]) -> Vec<T>
    where
        L: ItemList<Buffer<T>> + ?Sized,
        T: Copy,
    {
        let num_rows = indices.len();
        let mut builder: Vec<T> = Vec::with_capacity(num_rows);
        builder.extend(
            indices
                .iter()
                .map(|row_ptr| col[row_ptr.chunk_index as usize][row_ptr.row_index as usize]),
        );
        builder
    }

    // TODO: reuse the buffer by `SELECTIVITY_THRESHOLD`
    fn take_block_vec_binary_types<L>(col: &L, indices: &[RowPtr]) -> BinaryColumn
    where L: ItemList<BinaryColumn> + ?Sized {
        let mut builder = BinaryColumnBuilder::with_capacity(indices.len(), 0);
        for row_ptr in indices {
            let item = unsafe {
                col[row_ptr.chunk_index as usize].index_unchecked(row_ptr.row_index as usize)
            };
            builder.put_slice(item);
            builder.commit_row();
        }
        builder.build()
    }

    fn take_block_vec_string_types<L>(col: &L, indices: &[RowPtr]) -> StringColumn
    where L: ItemList<StringColumn> + ?Sized {
        let mut builder = StringColumnBuilder::with_capacity(indices.len());
        for row_ptr in indices {
            unsafe {
                builder.put_and_commit(
                    col[row_ptr.chunk_index as usize].index_unchecked(row_ptr.row_index as usize),
                );
            }
        }
        builder.build()
    }

    fn take_block_vec_boolean_types<L>(col: &L, indices: &[RowPtr]) -> Bitmap
    where L: ItemList<Bitmap> + ?Sized {
        let num_rows = indices.len();
        // Fast path: avoid iterating column to generate a new bitmap.
        // If all [`Bitmap`] are all_true or all_false and `num_rows <= bitmap.len()`,
        // we can just slice it.
        let mut total_len = 0;
        let mut unset_bits = 0;
        for bitmap in col.iter() {
            unset_bits += bitmap.null_count();
            total_len += bitmap.len();
        }
        if unset_bits == total_len || unset_bits == 0 {
            // Goes fast path.
            for bitmap in col.iter() {
                if num_rows <= bitmap.len() {
                    let mut bitmap = bitmap.clone();
                    bitmap.slice(0, num_rows);
                    return bitmap;
                }
            }
        }

        let capacity = num_rows.saturating_add(7) / 8;
        let mut builder: Vec<u8> = Vec::with_capacity(capacity);
        let mut unset_bits = 0;
        let mut value = 0;
        let mut i = 0;

        for row_ptr in indices.iter() {
            if col[row_ptr.chunk_index as usize].get_bit(row_ptr.row_index as usize) {
                value |= BIT_MASK[i % 8];
            } else {
                unset_bits += 1;
            }
            i += 1;
            if i % 8 == 0 {
                builder.push(value);
                value = 0;
            }
        }
        if i % 8 != 0 {
            builder.push(value);
        }

        unsafe {
            Bitmap::from_inner(Arc::new(builder.into()), 0, num_rows, unset_bits)
                .ok()
                .unwrap()
        }
    }

    fn take_block_vec_value_types<T: ValueType>(
        columns: &[T::Column],
        mut builder: T::ColumnBuilder,
        indices: &[RowPtr],
        data_type: &DataType,
    ) -> Column {
        for row_ptr in indices {
            let val = unsafe {
                T::index_column_unchecked(
                    &columns[row_ptr.chunk_index as usize],
                    row_ptr.row_index as usize,
                )
            };
            T::push_item(&mut builder, val.clone());
        }
        T::upcast_column_with_type(T::build_column(builder), data_type)
    }

    fn take_block_value_types<T: ValueType>(
        columns: &[Column],
        data_type: &DataType,
        mut builder: T::ColumnBuilder,
        indices: &[BlockIndex],
    ) -> Column {
        let columns = columns
            .iter()
            .map(|col| T::try_downcast_column(col).unwrap())
            .collect_vec();
        for &(block_index, row) in indices {
            let val =
                unsafe { T::index_column_unchecked(&columns[block_index as usize], row as usize) };
            T::push_item(&mut builder, val.clone());
        }
        T::upcast_column_with_type(T::build_column(builder), data_type)
    }
}

pub trait ItemList<T>: std::ops::Index<usize, Output = T> {
    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a T>
    where T: 'a;
    fn len(&self) -> usize;
}

impl<T> ItemList<T> for [T] {
    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a T>
    where T: 'a {
        self.iter()
    }

    fn len(&self) -> usize {
        self.len()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ChunkIndexItem {
    Single { block: u32, n: u32 },
    Repeat { block: u32, count: u32 },
    Range { block: u32, len: u32 },
}

impl ChunkIndexItem {
    pub fn block(&self) -> u32 {
        match *self {
            ChunkIndexItem::Single { block, .. } => block,
            ChunkIndexItem::Repeat { block, .. } => block,
            ChunkIndexItem::Range { block, .. } => block,
        }
    }

    pub fn size(&self) -> u32 {
        match *self {
            ChunkIndexItem::Single { n, .. } => n,
            ChunkIndexItem::Repeat { count, .. } => count,
            ChunkIndexItem::Range { len, .. } => len,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ChunkIndex {
    rows: Vec<u32>,
    chunks: Vec<ChunkIndexItem>,
    total: u32,
}

impl ChunkIndex {
    pub fn push_single(&mut self, block: u32, row: u32) {
        self.rows.push(row);
        self.chunks.push(ChunkIndexItem::Single { block, n: 1 });
        self.total += 1;
    }

    pub fn push_range(&mut self, block: u32, row: u32, len: u32) {
        debug_assert!(len > 0);
        self.rows.push(row);
        self.chunks.push(ChunkIndexItem::Range { block, len });
        self.total += len;
    }

    pub fn push_merge_range(&mut self, block: u32, row: u32, len: u32) {
        if len == 1 {
            self.push_merge(block, row);
            return;
        }

        match self.chunks.last_mut() {
            Some(ChunkIndexItem::Range {
                block: pre_block,
                len: pre_len,
            }) if *pre_block == block && *self.rows.last().unwrap() + *pre_len == row => {
                self.total += len;
                *pre_len += len;
            }
            Some(
                item @ &mut ChunkIndexItem::Single {
                    block: pre_block,
                    n: 1,
                },
            ) if pre_block == block && *self.rows.last().unwrap() + 1 == row => {
                self.total += len;
                *item = ChunkIndexItem::Range {
                    block,
                    len: len + 1,
                }
            }
            _ => self.push_range(block, row, len),
        }
    }

    pub fn push_repeat(&mut self, block: u32, row: u32, count: u32) {
        self.rows.push(row);
        self.chunks.push(ChunkIndexItem::Repeat { block, count });
        self.total += count;
    }

    pub fn push_merge(&mut self, block: u32, row: u32) {
        let new_block = block;
        let new_row = row;
        self.total += 1;
        match self.chunks.last_mut() {
            Some(ChunkIndexItem::Repeat { block, count })
                if *block == new_block && *self.rows.last().unwrap() == new_row =>
            {
                *count += 1;
            }
            Some(ChunkIndexItem::Range { block, len })
                if *block == new_block && *self.rows.last().unwrap() + *len == new_row =>
            {
                *len += 1;
            }
            Some(item @ &mut ChunkIndexItem::Single { block, n: 1 })
                if block == new_block && *self.rows.last().unwrap() == new_row =>
            {
                *item = ChunkIndexItem::Repeat { block, count: 2 }
            }
            Some(item @ &mut ChunkIndexItem::Single { block, n: 1 })
                if block == new_block && new_row == *self.rows.last().unwrap() + 1 =>
            {
                *item = ChunkIndexItem::Range { block, len: 2 }
            }
            Some(ChunkIndexItem::Single { block, n }) if *block == new_block => {
                self.rows.push(new_row);
                *n += 1;
            }
            _ => {
                self.rows.push(row);
                self.chunks.push(ChunkIndexItem::Single {
                    block: new_block,
                    n: 1,
                });
            }
        }
    }

    pub fn iter_chunk(&self) -> ChunkIndexIter<'_> {
        ChunkIndexIter {
            rows: &self.rows,
            row_pos: 0,
            chunks: &self.chunks,
            chunk_pos: 0,
        }
    }

    pub fn num_rows(&self) -> usize {
        self.total as _
    }

    pub fn clear(&mut self) {
        self.rows.clear();
        self.chunks.clear();
        self.total = 0
    }

    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    pub fn check_valid(&self) -> bool {
        if self.chunks.iter().any(|item| item.size() == 0) {
            return false;
        }
        self.chunks.iter().map(|item| item.size()).sum::<u32>() == self.total
    }
}

pub enum Chunk<'a> {
    Single { block: u32, rows: &'a [u32] },
    Repeat { block: u32, rows: RepeatIndex },
    Range { block: u32, row: u32, len: u32 },
}

impl Chunk<'_> {
    pub fn block(&self) -> u32 {
        match *self {
            Chunk::Single { block, .. } => block,
            Chunk::Repeat { block, .. } => block,
            Chunk::Range { block, .. } => block,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Chunk::Single { rows, .. } => rows.len(),
            Chunk::Repeat { rows, .. } => rows.len(),
            Chunk::Range { len, .. } => *len as _,
        }
    }

    pub fn max_row(&self) -> u32 {
        match self {
            Chunk::Single { rows, .. } => rows.iter().copied().max().unwrap(),
            Chunk::Repeat { rows, .. } => rows.row,
            Chunk::Range { row, len, .. } => row + len,
        }
    }
}

pub struct ChunkIndexIter<'a> {
    rows: &'a [u32],
    row_pos: usize,
    chunks: &'a [ChunkIndexItem],
    chunk_pos: usize,
}

impl<'a> Iterator for ChunkIndexIter<'a> {
    type Item = Chunk<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.chunks.get(self.chunk_pos)?;
        self.chunk_pos += 1;

        match item {
            ChunkIndexItem::Single { block, n } => {
                let start = self.row_pos;
                let end = start + (*n as usize);
                self.row_pos = end;
                Some(Chunk::Single {
                    block: *block,
                    rows: &self.rows[start..end],
                })
            }
            ChunkIndexItem::Repeat { block, count } => {
                let row = self.rows[self.row_pos];
                self.row_pos += 1;
                Some(Chunk::Repeat {
                    block: *block,
                    rows: RepeatIndex { row, count: *count },
                })
            }
            ChunkIndexItem::Range { block, len } => {
                let row = self.rows[self.row_pos];
                self.row_pos += 1;
                Some(Chunk::Range {
                    block: *block,
                    row,
                    len: *len,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flatten_index(index: &ChunkIndex) -> Vec<(u32, u32)> {
        let mut flattened = Vec::with_capacity(index.total as _);
        for chunk in index.iter_chunk() {
            match chunk {
                Chunk::Single { block, rows } => {
                    flattened.extend(rows.iter().map(|row| (block, *row)));
                }
                Chunk::Repeat {
                    block,
                    rows: RepeatIndex { row, count },
                } => {
                    flattened.extend(std::iter::repeat_n((block, row), count as _));
                }
                Chunk::Range { block, row, len } => {
                    flattened.extend((0..len).map(|delta| (block, row + delta)));
                }
            }
        }
        flattened
    }

    #[test]
    fn test_chunk_index_push_merge() {
        let mut index = ChunkIndex::default();

        let input = vec![
            (0, 0),
            (0, 1),
            (0, 1),
            (0, 2),
            (1, 10),
            (1, 11),
            (1, 12),
            (1, 12),
            (2, 20),
            (3, 30),
            (3, 30),
            (3, 30),
            (4, 100),
            (4, 102),
        ];

        for &(block, row) in &input {
            index.push_merge(block, row);
            assert!(index.check_valid());
        }

        assert_eq!(index.total as usize, input.len());
        let flattened = flatten_index(&index);
        assert_eq!(flattened, input);
    }

    #[test]
    fn test_chunk_index_push_merge_range() {
        let mut index = ChunkIndex::default();

        index.push_merge_range(0, 3, 1);
        index.push_merge_range(0, 4, 1);
        index.push_merge_range(0, 5, 2);
        index.push_merge_range(0, 3, 1);
        index.push_merge_range(0, 3, 1);

        assert!(index.check_valid());

        let flattened = flatten_index(&index);
        assert_eq!(flattened, vec![
            (0, 3),
            (0, 4),
            (0, 5),
            (0, 6),
            (0, 3),
            (0, 3),
        ])
    }
}
