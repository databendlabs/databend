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
use ethnum::I256;
use itertools::Itertools;

use crate::types::array::ArrayColumn;
use crate::types::array::ArrayColumnBuilder;
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
use crate::with_decimal_type;
use crate::with_number_mapped_type;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::Value;

impl DataBlock {
    pub fn take<I>(&self, indices: &[I]) -> Result<Self>
    where I: common_arrow::arrow::types::Index {
        if indices.is_empty() {
            return Ok(self.slice(0..0));
        }

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
                    value: Value::Column(Column::take(c, indices)),
                },
            })
            .collect();

        Ok(DataBlock::new(after_columns, indices.len()))
    }

    pub fn take_by_compressd_indices(
        &self,
        indices: &[(u32, u32)],
        indices_len: usize,
        probe_num: usize,
    ) -> Result<Self> {
        if indices.is_empty() {
            return Ok(self.slice(0..0));
        }

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
                    value: Value::Column(Column::take_by_compressd_indices(
                        c,
                        indices,
                        indices_len,
                        probe_num,
                    )),
                },
            })
            .collect();

        Ok(DataBlock::new(after_columns, probe_num))
    }
}

impl Column {
    pub fn take<I>(&self, indices: &[I]) -> Self
    where I: common_arrow::arrow::types::Index {
        let length = indices.len();
        match self {
            Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {
                self.slice(0..length)
            }
            Column::Number(column) => with_number_mapped_type!(|NUM_TYPE| match column {
                NumberColumn::NUM_TYPE(values) =>
                    Self::take_arg_types::<NumberType<NUM_TYPE>, _>(values, indices),
            }),
            Column::Decimal(column) => with_decimal_type!(|DECIMAL_TYPE| match column {
                DecimalColumn::DECIMAL_TYPE(values, size) => {
                    let builder = indices
                        .iter()
                        .map(|index| unsafe { *values.get_unchecked(index.to_usize()) })
                        .collect_vec();
                    Column::Decimal(DecimalColumn::DECIMAL_TYPE(builder.into(), *size))
                }
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
                let mut offsets = Vec::with_capacity(length + 1);
                offsets.push(0);
                let builder = ColumnBuilder::with_capacity(&column.values.data_type(), self.len());
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::take_value_types::<ArrayType<AnyType>, _>(column, builder, indices)
            }
            Column::Map(column) => {
                let mut offsets = Vec::with_capacity(length + 1);
                offsets.push(0);
                let builder = ColumnBuilder::from_column(
                    ColumnBuilder::with_capacity(&column.values.data_type(), self.len()).build(),
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
                Self::take_value_types::<MapType<AnyType, AnyType>, _>(&column, builder, indices)
            }
            Column::Nullable(c) => {
                let column = c.column.take(indices);
                let validity = Self::take_arg_types::<BooleanType, _>(&c.validity, indices);
                Column::Nullable(Box::new(NullableColumn {
                    column,
                    validity: BooleanType::try_downcast_column(&validity).unwrap(),
                }))
            }
            Column::Tuple(fields) => {
                let fields = fields.iter().map(|c| c.take(indices)).collect();
                Column::Tuple(fields)
            }
            Column::Variant(column) => Self::take_arg_types::<VariantType, _>(column, indices),
        }
    }

    pub fn take_by_compressd_indices(
        &self,
        indices: &[(u32, u32)],
        indices_len: usize,
        row_num: usize,
    ) -> Self {
        let length = indices.len();
        match self {
            Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {
                self.slice(0..length)
            }
            Column::Number(column) => {
                with_number_mapped_type!(|NUM_TYPE| match column {
                    NumberColumn::NUM_TYPE(values) => Self::take_arg_types_by_compressd_indices::<
                        NumberType<NUM_TYPE>,
                    >(
                        values, indices, indices_len, row_num
                    ),
                })
            }
            Column::Decimal(column) => with_decimal_type!(|DECIMAL_TYPE| match column {
                DecimalColumn::Decimal128(values, size) => {
                    let mut builder: Vec<i128> = Vec::with_capacity(row_num);
                    let builder_ptr = builder.as_mut_ptr();
                    let col_ptr = values.as_ptr();
                    let mut offset = 0;
                    let mut remain;
                    let mut power;
                    let mut idx = 0;
                    while idx < indices_len {
                        let (index, cnt) = indices[idx];
                        idx += 1;
                        if cnt == 1 {
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    col_ptr.add(index as usize),
                                    builder_ptr.add(offset),
                                    1,
                                );
                            }
                            offset += 1;
                            continue;
                        }
                        // Using the doubling method to copy memory.
                        let base_offset = offset;
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                col_ptr.add(index as usize),
                                builder_ptr.add(base_offset),
                                1,
                            );
                        }
                        remain = cnt as usize;
                        // Since cnt > 0, then 31 - cnt.leading_zeros() >= 0.
                        let max_segment = 1 << (31 - cnt.leading_zeros());
                        let mut cur_segment = 1;
                        while cur_segment < max_segment {
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    builder_ptr.add(base_offset),
                                    builder_ptr.add(base_offset + cur_segment),
                                    cur_segment,
                                );
                            }
                            cur_segment <<= 1;
                        }
                        remain -= max_segment;
                        offset += max_segment;
                        power = 0;
                        while remain > 0 {
                            if remain & 1 == 1 {
                                let cur_segment = 1 << power;
                                unsafe {
                                    std::ptr::copy_nonoverlapping(
                                        builder_ptr.add(base_offset),
                                        builder_ptr.add(offset),
                                        cur_segment,
                                    );
                                }
                                offset += cur_segment;
                            }
                            power += 1;
                            remain >>= 1;
                        }
                    }
                    unsafe {
                        builder.set_len(offset);
                    }
                    Column::Decimal(DecimalColumn::Decimal128(builder.into(), *size))
                }
                DecimalColumn::Decimal256(values, size) => {
                    let mut builder: Vec<I256> = Vec::with_capacity(row_num);
                    let builder_ptr = builder.as_mut_ptr();
                    let col_ptr = values.as_ptr();
                    let mut offset = 0;
                    let mut remain;
                    let mut power;
                    let mut idx = 0;
                    while idx < indices_len {
                        let (index, cnt) = indices[idx];
                        idx += 1;
                        if cnt == 1 {
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    col_ptr.add(index as usize),
                                    builder_ptr.add(offset),
                                    1,
                                );
                            }
                            offset += 1;
                            continue;
                        }
                        // Using the doubling method to copy memory.
                        let base_offset = offset;
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                col_ptr.add(index as usize),
                                builder_ptr.add(base_offset),
                                1,
                            );
                        }
                        remain = cnt as usize;
                        // Since cnt > 0, then 31 - cnt.leading_zeros() >= 0.
                        let max_segment = 1 << (31 - cnt.leading_zeros());
                        let mut cur_segment = 1;
                        while cur_segment < max_segment {
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    builder_ptr.add(base_offset),
                                    builder_ptr.add(base_offset + cur_segment),
                                    cur_segment,
                                );
                            }
                            cur_segment <<= 1;
                        }
                        remain -= max_segment;
                        offset += max_segment;
                        power = 0;
                        while remain > 0 {
                            if remain & 1 == 1 {
                                let cur_segment = 1 << power;
                                unsafe {
                                    std::ptr::copy_nonoverlapping(
                                        builder_ptr.add(base_offset),
                                        builder_ptr.add(offset),
                                        cur_segment,
                                    );
                                }
                                offset += cur_segment;
                            }
                            power += 1;
                            remain >>= 1;
                        }
                    }
                    unsafe {
                        builder.set_len(offset);
                    }
                    Column::Decimal(DecimalColumn::Decimal256(builder.into(), *size))
                }
            }),
            Column::Boolean(bm) => Self::take_arg_types_by_compressd_indices::<BooleanType>(
                bm,
                indices,
                indices_len,
                row_num,
            ),
            Column::String(column) => Self::take_arg_types_by_compressd_indices::<StringType>(
                column,
                indices,
                indices_len,
                row_num,
            ),
            Column::Timestamp(column) => {
                let ts = Self::take_arg_types_by_compressd_indices::<NumberType<i64>>(
                    column,
                    indices,
                    indices_len,
                    row_num,
                )
                .into_number()
                .unwrap()
                .into_int64()
                .unwrap();
                Column::Timestamp(ts)
            }
            Column::Date(column) => {
                let d = Self::take_arg_types_by_compressd_indices::<NumberType<i32>>(
                    column,
                    indices,
                    indices_len,
                    row_num,
                )
                .into_number()
                .unwrap()
                .into_int32()
                .unwrap();
                Column::Date(d)
            }
            Column::Array(column) => {
                let mut offsets = Vec::with_capacity(length + 1);
                offsets.push(0);
                let builder = ColumnBuilder::with_capacity(&column.values.data_type(), self.len());
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::take_value_types_by_compressd_indices::<ArrayType<AnyType>>(
                    column,
                    builder,
                    indices,
                    indices_len,
                )
            }
            Column::Map(column) => {
                let mut offsets = Vec::with_capacity(length + 1);
                offsets.push(0);
                let builder = ColumnBuilder::from_column(
                    ColumnBuilder::with_capacity(&column.values.data_type(), self.len()).build(),
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
                Self::take_value_types_by_compressd_indices::<MapType<AnyType, AnyType>>(
                    &column,
                    builder,
                    indices,
                    indices_len,
                )
            }
            Column::Nullable(c) => {
                let column = c
                    .column
                    .take_by_compressd_indices(indices, indices_len, row_num);
                let validity = Self::take_arg_types_by_compressd_indices::<BooleanType>(
                    &c.validity,
                    indices,
                    indices_len,
                    row_num,
                );
                Column::Nullable(Box::new(NullableColumn {
                    column,
                    validity: BooleanType::try_downcast_column(&validity).unwrap(),
                }))
            }
            Column::Tuple(fields) => {
                let fields = fields
                    .iter()
                    .map(|c| c.take_by_compressd_indices(indices, indices_len, row_num))
                    .collect();
                Column::Tuple(fields)
            }
            Column::Variant(column) => Self::take_arg_types_by_compressd_indices::<StringType>(
                column,
                indices,
                indices_len,
                row_num,
            ),
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

    fn take_arg_types_by_compressd_indices<T: ArgType>(
        col: &T::Column,
        indices: &[(u32, u32)],
        indices_len: usize,
        row_num: usize,
    ) -> Column {
        T::upcast_column(unsafe {
            T::take_by_compressd_indices(col, indices, indices_len, row_num)
        })
    }

    fn take_value_types_by_compressd_indices<T: ValueType>(
        col: &T::Column,
        mut builder: T::ColumnBuilder,
        indices: &[(u32, u32)],
        indices_len: usize,
    ) -> Column {
        unsafe {
            let mut idx = 0;
            while idx < indices_len {
                let (index, cnt) = indices[idx];
                idx += 1;
                for _ in 0..cnt {
                    T::push_item(&mut builder, T::index_column_unchecked(col, index as usize));
                }
            }
        }
        T::upcast_column(T::build_column(builder))
    }
}
