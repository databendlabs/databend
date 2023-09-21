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
    pub fn take_compacted_indices(&self, indices: &[(u32, u32)], num_rows: usize) -> Result<Self> {
        if indices.is_empty() {
            return Ok(self.slice(0..0));
        }

        // Each item in the `indices` consists of an `index` and a `cnt`, the sum
        // of the `cnt` must be equal to the `num_rows`.
        debug_assert_eq!(
            indices.iter().fold(0, |acc, &(_, x)| acc + x as usize),
            num_rows
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
                    value: Value::Column(Column::take_compacted_indices(c, indices, num_rows)),
                },
            })
            .collect();

        Ok(DataBlock::new_with_meta(
            after_columns,
            num_rows,
            self.get_meta().cloned(),
        ))
    }
}

impl Column {
    pub fn take_compacted_indices(&self, indices: &[(u32, u32)], num_rows: usize) -> Self {
        match self {
            Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {
                self.slice(0..num_rows)
            }
            Column::Number(column) => with_number_mapped_type!(|NUM_TYPE| match column {
                NumberColumn::NUM_TYPE(values) => {
                    let builder = Self::take_compacted_primitive_types(values, indices, num_rows);
                    <NumberType<NUM_TYPE>>::upcast_column(<NumberType<NUM_TYPE>>::column_from_vec(
                        builder,
                        &[],
                    ))
                }
            }),
            Column::Decimal(column) => with_decimal_type!(|DECIMAL_TYPE| match column {
                DecimalColumn::DECIMAL_TYPE(values, size) => {
                    let builder = Self::take_compacted_primitive_types(values, indices, num_rows);
                    Column::Decimal(DecimalColumn::DECIMAL_TYPE(builder.into(), *size))
                }
            }),
            Column::Boolean(bm) => {
                Self::take_compacted_arg_types::<BooleanType>(bm, indices, num_rows)
            }
            Column::String(column) => StringType::upcast_column(Self::take_compact_string_types(
                column, indices, num_rows,
            )),
            Column::Timestamp(column) => {
                let builder = Self::take_compacted_primitive_types(column, indices, num_rows);
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
                let builder = Self::take_compacted_primitive_types(column, indices, num_rows);
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
                let mut offsets = Vec::with_capacity(num_rows + 1);
                offsets.push(0);
                let builder = ColumnBuilder::with_capacity(&column.values.data_type(), num_rows);
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::take_compacted_value_types::<ArrayType<AnyType>>(column, builder, indices)
            }
            Column::Map(column) => {
                let mut offsets = Vec::with_capacity(num_rows + 1);
                offsets.push(0);
                let builder = ColumnBuilder::from_column(
                    ColumnBuilder::with_capacity(&column.values.data_type(), num_rows).build(),
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
            Column::Bitmap(column) => BitmapType::upcast_column(Self::take_compact_string_types(
                column, indices, num_rows,
            )),
            Column::Nullable(c) => {
                let column = c.column.take_compacted_indices(indices, num_rows);
                let validity =
                    Self::take_compacted_arg_types::<BooleanType>(&c.validity, indices, num_rows);
                Column::Nullable(Box::new(NullableColumn {
                    column,
                    validity: BooleanType::try_downcast_column(&validity).unwrap(),
                }))
            }
            Column::Tuple(fields) => {
                let fields = fields
                    .iter()
                    .map(|c| c.take_compacted_indices(indices, num_rows))
                    .collect();
                Column::Tuple(fields)
            }
            Column::Variant(column) => VariantType::upcast_column(Self::take_compact_string_types(
                column, indices, num_rows,
            )),
        }
    }

    pub fn take_compacted_primitive_types<T>(
        col: &Buffer<T>,
        indices: &[(u32, u32)],
        num_rows: usize,
    ) -> Vec<T>
    where
        T: Copy,
    {
        let mut builder: Vec<T> = Vec::with_capacity(num_rows);
        let ptr = builder.as_mut_ptr();
        let mut offset = 0;
        let mut remain;
        let col_ptr = col.as_slice().as_ptr();
        for (index, cnt) in indices.iter() {
            if *cnt == 1 {
                // # Safety
                // offset + 1 <= num_rows
                unsafe {
                    std::ptr::copy_nonoverlapping(col_ptr.add(*index as usize), ptr.add(offset), 1)
                };
                offset += 1;
                continue;
            }
            // Using the doubling method to copy the max segment memory.
            // [___________] => [x__________] => [xx_________] => [xxxx_______] => [xxxxxxxx___]
            let base_offset = offset;
            // # Safety
            // base_offset + 1 <= num_rows
            unsafe {
                std::ptr::copy_nonoverlapping(col_ptr.add(*index as usize), ptr.add(offset), 1)
            };
            remain = *cnt as usize;
            // Since cnt > 0, then 31 - cnt.leading_zeros() >= 0.
            let max_segment = 1 << (31 - cnt.leading_zeros());
            let mut cur_segment = 1;
            while cur_segment < max_segment {
                // # Safety
                // base_offset + 2 * cur_segment <= num_rows
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        ptr.add(base_offset),
                        ptr.add(base_offset + cur_segment),
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
                // max_segment > remain and offset + remain <= num_rows
                unsafe {
                    std::ptr::copy_nonoverlapping(ptr.add(base_offset), ptr.add(offset), remain)
                };
                offset += remain;
            }
        }
        // # Safety
        // `offset` is equal to `num_rows`
        unsafe { builder.set_len(offset) };

        builder
    }

    pub fn take_compact_string_types<'a>(
        col: &'a StringColumn,
        indices: &[(u32, u32)],
        num_rows: usize,
    ) -> StringColumn {
        let mut items: Vec<(&[u8], u32)> = Vec::with_capacity(indices.len());
        let mut offsets: Vec<u64> = Vec::with_capacity(num_rows + 1);
        offsets.push(0);
        let items_ptr = items.as_mut_ptr();
        let offsets_ptr = unsafe { offsets.as_mut_ptr().add(1) };

        let mut data_size = 0;
        let mut offset_idx = 0;
        for (items_idx, (index, cnt)) in indices.iter().enumerate() {
            let item = unsafe { col.index_unchecked(*index as usize) };
            // # Safety
            // items_idx < indices.len() and offset_idx < num_rows + 1
            unsafe {
                std::ptr::write(items_ptr.add(items_idx), (item, *cnt));
                for _ in 0..*cnt {
                    data_size += item.len() as u64;
                    std::ptr::write(offsets_ptr.add(offset_idx), data_size);
                    offset_idx += 1;
                }
            }
        }
        unsafe {
            items.set_len(indices.len());
            offsets.set_len(num_rows + 1);
        }

        let mut data: Vec<u8> = Vec::with_capacity(data_size as usize);
        let data_ptr = data.as_mut_ptr();
        let mut offset = 0;
        let mut remain;
        for (item, cnt) in items {
            let len = item.len();
            if cnt == 1 {
                // # Safety
                // offset + len <= data_capacity
                unsafe {
                    std::ptr::copy_nonoverlapping(item.as_ptr(), data_ptr.add(offset), len);
                }
                offset += len;
                continue;
            }

            // Using the doubling method to copy the max segment memory.
            // [___________] => [x__________] => [xx_________] => [xxxx_______] => [xxxxxxxx___]
            let base_offset = offset;
            // # Safety
            // base_offset + len <= data_capacity
            unsafe {
                std::ptr::copy_nonoverlapping(item.as_ptr(), data_ptr.add(offset), len);
            }
            remain = cnt as usize;
            // Since cnt > 0, then 31 - cnt.leading_zeros() >= 0.
            let max_bit_num = 1 << (31 - cnt.leading_zeros());
            let max_segment = max_bit_num * len;
            let mut cur_segment = len;
            while cur_segment < max_segment {
                unsafe {
                    // # Safety
                    // offset + 2 * cur_segment <= data_capacity
                    std::ptr::copy_nonoverlapping(
                        data_ptr.add(base_offset),
                        data_ptr.add(base_offset + cur_segment),
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
                        data_ptr.add(base_offset),
                        data_ptr.add(offset),
                        remain * len,
                    )
                };
                offset += remain * len;
            }
        }
        // # Safety
        // `offset` is equal to `data_capacity`
        unsafe { data.set_len(offset) };
        StringColumn::new(data.into(), offsets.into())
    }

    fn take_compacted_arg_types<T: ArgType>(
        col: &T::Column,
        indices: &[(u32, u32)],
        num_rows: usize,
    ) -> Column {
        let mut builder = T::create_builder(num_rows, &[]);
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
