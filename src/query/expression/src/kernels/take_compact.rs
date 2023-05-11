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

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::buffer::Buffer;
use common_exception::Result;

use crate::types::array::ArrayColumn;
use crate::types::array::ArrayColumnBuilder;
use crate::types::bitmap::BitmapType;
use crate::types::decimal::DecimalColumn;
use crate::types::map::KvColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::string::StringColumn;
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
    pub fn take_compacted_indices(&self, indices: &[(u32, u32)], row_num: usize) -> Result<Self> {
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
                    value: Value::Column(Column::take_compacted_indices(c, indices, row_num)),
                },
            })
            .collect();

        Ok(DataBlock::new(after_columns, row_num))
    }
}

impl Column {
    pub fn take_compacted_indices(&self, indices: &[(u32, u32)], row_num: usize) -> Self {
        let length = indices.len();
        match self {
            Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {
                self.slice(0..length)
            }
            Column::Number(column) => {
                with_number_mapped_type!(|NUM_TYPE| match column {
                    NumberColumn::NUM_TYPE(values) => {
                        let builder = Self::take_primitive_types(values, indices, row_num);
                        <NumberType<NUM_TYPE>>::upcast_column(
                            <NumberType<NUM_TYPE>>::column_from_vec(builder, &[]),
                        )
                    }
                })
            }
            Column::Decimal(column) => with_decimal_type!(|DECIMAL_TYPE| match column {
                DecimalColumn::Decimal128(values, size) => {
                    let builder = Self::take_primitive_types(values, indices, row_num);
                    Column::Decimal(DecimalColumn::Decimal128(builder.into(), *size))
                }
                DecimalColumn::Decimal256(values, size) => {
                    let builder = Self::take_primitive_types(values, indices, row_num);
                    Column::Decimal(DecimalColumn::Decimal256(builder.into(), *size))
                }
            }),
            Column::Boolean(bm) => {
                BooleanType::upcast_column(Self::take_bool_types(bm, indices, row_num))
            }
            Column::String(column) => {
                StringType::upcast_column(Self::take_string_types(column, indices, row_num))
            }
            Column::Timestamp(column) => {
                let builder = Self::take_primitive_types(column, indices, row_num);
                let ts = <NumberType<i64>>::upcast_column(<NumberType<i64>>::column_from_vec(
                    builder,
                    &[],
                ))
                .into_number()
                .unwrap()
                .into_int64()
                .unwrap();
                Column::Timestamp(ts)
            }
            Column::Date(column) => {
                let builder = Self::take_primitive_types(column, indices, row_num);
                let d = <NumberType<i32>>::upcast_column(<NumberType<i32>>::column_from_vec(
                    builder,
                    &[],
                ))
                .into_number()
                .unwrap()
                .into_int32()
                .unwrap();
                Column::Date(d)
            }
            Column::Array(column) => {
                let mut offsets = Vec::with_capacity(length + 1);
                offsets.push(0);
                let builder = ColumnBuilder::with_capacity(&column.values.data_type(), row_num);
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::take_scalar_types::<ArrayType<AnyType>>(column, builder, indices, row_num)
            }
            Column::Map(column) => {
                let mut offsets = Vec::with_capacity(length + 1);
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
                Self::take_scalar_types::<MapType<AnyType, AnyType>>(
                    &column, builder, indices, row_num,
                )
            }
            Column::Bitmap(column) => {
                BitmapType::upcast_column(Self::take_string_types(column, indices, row_num))
            }
            Column::Nullable(c) => {
                let column = c.column.take_compacted_indices(indices, row_num);
                let validity = BooleanType::upcast_column(Self::take_bool_types(
                    &c.validity,
                    indices,
                    row_num,
                ));
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
                VariantType::upcast_column(Self::take_string_types(column, indices, row_num))
            }
        }
    }

    pub fn take_primitive_types<T>(
        col: &Buffer<T>,
        indices: &[(u32, u32)],
        row_num: usize,
    ) -> Vec<T> {
        // Each item in the `indices` consists of an `index` and a `cnt`, the sum
        // of the `cnt` must be equal to the `row_num`.
        debug_assert_eq!(
            indices.iter().fold(0, |acc, &(_, x)| acc + x as usize),
            row_num
        );

        let mut builder: Vec<T> = Vec::with_capacity(row_num);
        let builder_ptr = builder.as_mut_ptr();
        let col_ptr = col.as_ptr();

        let mut offset = 0;
        let mut remain;
        for (index, cnt) in indices {
            if *cnt == 1 {
                // # Safety
                // offset + 1 <= row_num
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        col_ptr.add(*index as usize),
                        builder_ptr.add(offset),
                        1,
                    )
                };
                offset += 1;
                continue;
            }
            // Using the doubling method to copy the max segment memory.
            // [___________] => [x__________] => [xx_________] => [xxxx_______] => [xxxxxxxx___]
            let base_offset = offset;
            // # Safety
            // base_offset + 1 <= row_num
            unsafe {
                std::ptr::copy_nonoverlapping(
                    col_ptr.add(*index as usize),
                    builder_ptr.add(base_offset),
                    1,
                )
            };
            remain = *cnt as usize;
            // Since cnt > 0, then 31 - cnt.leading_zeros() >= 0.
            let max_segment = 1 << (31 - cnt.leading_zeros());
            let mut cur_segment = 1;
            while cur_segment < max_segment {
                // # Safety
                // base_offset + 2 * cur_segment <= row_num
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        builder_ptr.add(base_offset),
                        builder_ptr.add(base_offset + cur_segment),
                        cur_segment,
                    )
                };
                cur_segment <<= 1;
            }
            remain -= max_segment;
            offset += max_segment;

            if remain > 0 {
                // Copy the remaining memory directly.
                // [xxxxxxxxxx____] => [xxxxxxxxxxxxxx]
                //  ^^^^ ---> ^^^^
                // # Safety
                // max_segment > remain and offset + remain <= row_num
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        builder_ptr.add(base_offset),
                        builder_ptr.add(offset),
                        remain,
                    )
                };
                offset += remain;
            }
        }
        // # Safety
        // `offset` is equal to `row_num`
        unsafe { builder.set_len(offset) };

        builder
    }

    pub fn take_string_types<'a>(
        col: &'a StringColumn,
        indices: &[(u32, u32)],
        row_num: usize,
    ) -> StringColumn {
        // Each item in the `indices` consists of an `index` and a `cnt`, the sum
        // of the `cnt` must be equal to the `row_num`.
        debug_assert_eq!(
            indices.iter().fold(0, |acc, &(_, x)| acc + x as usize),
            row_num
        );

        let mut data_capacity: u64 = 0;
        let mut res_offsets: Vec<u64> = Vec::with_capacity(row_num + 1);
        res_offsets.push(0);
        for (index, cnt) in indices {
            // # Safety
            // the out-of-bounds `index` for `col` in indices is *[undefined behavior]*
            let col = unsafe { col.index_unchecked(*index as usize) };
            for _ in 0..*cnt {
                data_capacity += col.len() as u64;
                res_offsets.push(data_capacity);
            }
        }

        let mut res_data: Vec<u8> = Vec::with_capacity(data_capacity as usize);
        let res_data_ptr = res_data.as_mut_ptr();

        let col_data_ptr = col.data.as_ptr();
        let col_offset = &col.offsets;

        let mut offset = 0;
        let mut remain;
        for (index, cnt) in indices {
            let len =
                col_offset[*index as usize + 1] as usize - col_offset[*index as usize] as usize;
            if *cnt == 1 {
                // # Safety
                // offset + len <= data_capacity
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        col_data_ptr.add(col_offset[*index as usize] as usize),
                        res_data_ptr.add(offset),
                        len,
                    )
                };
                offset += len;
                continue;
            }

            // Using the doubling method to copy the max segment memory.
            // [___________] => [x__________] => [xx_________] => [xxxx_______] => [xxxxxxxx___]
            let base_offset = offset;
            // # Safety
            // base_offset + len <= data_capacity
            unsafe {
                std::ptr::copy_nonoverlapping(
                    col_data_ptr.add(col_offset[*index as usize] as usize),
                    res_data_ptr.add(base_offset),
                    len,
                )
            };
            remain = *cnt as usize;
            // Since cnt > 0, then 31 - cnt.leading_zeros() >= 0.
            let max_bit_num = 1 << (31 - cnt.leading_zeros());
            let max_segment = max_bit_num * len;
            let mut cur_segment = len;
            while cur_segment < max_segment {
                unsafe {
                    // # Safety
                    // offset + 2 * cur_segment <= data_capacity
                    std::ptr::copy_nonoverlapping(
                        res_data_ptr.add(base_offset),
                        res_data_ptr.add(base_offset + cur_segment),
                        cur_segment,
                    )
                };
                cur_segment <<= 1;
            }
            remain -= max_bit_num;
            offset += max_segment;

            if remain > 0 {
                // Copy the remaining memory directly.
                // [xxxxxxxxxx____] => [xxxxxxxxxxxxxx]
                //  ^^^^ ---> ^^^^
                // # Safety
                // max_segment > remain * len and offset + remain * len <= data_capacity
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        res_data_ptr.add(base_offset),
                        res_data_ptr.add(offset),
                        remain * len,
                    )
                };
                offset += remain * len;
            }
        }
        // # Safety
        // `offset` is equal to `data_capacity`
        unsafe { res_data.set_len(offset) };

        StringColumn {
            data: res_data.into(),
            offsets: res_offsets.into(),
        }
    }

    pub fn take_bool_types(col: &Bitmap, indices: &[(u32, u32)], row_num: usize) -> Bitmap {
        // Each item in the `indices` consists of an `index` and a `cnt`, the sum
        // of the `cnt` must be equal to the `row_num`.
        debug_assert_eq!(
            indices.iter().fold(0, |acc, &(_, x)| acc + x as usize),
            row_num
        );
        let mut col_builder = MutableBitmap::with_capacity((row_num).saturating_add(7) / 8);
        for (index, cnt) in indices {
            // # Safety
            // the out-of-bounds `index` for `col` in indices is *[undefined behavior]*
            let val = unsafe { col.get_bit_unchecked(*index as usize) };
            for _ in 0..*cnt {
                col_builder.push(val);
            }
        }
        BooleanType::build_column(col_builder)
    }

    pub fn take_scalar_types<T: ValueType>(
        col: &T::Column,
        mut builder: T::ColumnBuilder,
        indices: &[(u32, u32)],
        row_num: usize,
    ) -> Column {
        // Each item in the `indices` consists of an `index` and a `cnt`, the sum
        // of the `cnt` must be equal to the `row_num`.
        debug_assert_eq!(
            indices.iter().fold(0, |acc, &(_, x)| acc + x as usize),
            row_num
        );
        for (index, cnt) in indices {
            for _ in 0..*cnt {
                T::push_item(
                    &mut builder,
                    // # Safety
                    // the out-of-bounds `index` for `col` in indices is *[undefined behavior]*
                    unsafe { T::index_column_unchecked(col, *index as usize) },
                );
            }
        }
        T::upcast_column(T::build_column(builder))
    }
}
