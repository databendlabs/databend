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

use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_exception::Result;

use crate::kernels::utils::copy_advance_aligned;
use crate::kernels::utils::set_vec_len_by_ptr;
use crate::kernels::utils::store_advance_aligned;
use crate::types::binary::BinaryColumn;
use crate::types::nullable::NullableColumn;
use crate::types::string::StringColumn;
use crate::types::*;
use crate::visitor::ValueVisitor;
use crate::BlockEntry;
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

        let mut taker = TakeCompactVisitor::new(indices, num_rows);
        let after_columns = self
            .columns()
            .iter()
            .map(|entry| {
                taker.visit_value(entry.value.clone())?;
                let result = taker.result.take().unwrap();
                Ok(BlockEntry {
                    value: result,
                    data_type: entry.data_type.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(DataBlock::new_with_meta(
            after_columns,
            num_rows,
            self.get_meta().cloned(),
        ))
    }
}

struct TakeCompactVisitor<'a> {
    indices: &'a [(u32, u32)],
    num_rows: usize,
    result: Option<Value<AnyType>>,
}

impl<'a> TakeCompactVisitor<'a> {
    fn new(indices: &'a [(u32, u32)], num_rows: usize) -> Self {
        Self {
            indices,
            num_rows,
            result: None,
        }
    }
}

impl<'a> ValueVisitor for TakeCompactVisitor<'a> {
    fn visit_scalar(&mut self, scalar: crate::Scalar) -> Result<()> {
        self.result = Some(Value::Scalar(scalar));
        Ok(())
    }

    fn visit_nullable(&mut self, column: Box<NullableColumn<AnyType>>) -> Result<()> {
        self.visit_boolean(column.validity.clone())?;
        let validity =
            BooleanType::try_downcast_column(self.result.take().unwrap().as_column().unwrap())
                .unwrap();

        self.visit_column(column.column)?;
        let result = self.result.take().unwrap();
        let result = result.as_column().unwrap();
        self.result = Some(Value::Column(NullableColumn::new_column(
            result.clone(),
            validity,
        )));
        Ok(())
    }

    fn visit_typed_column<T: ValueType>(&mut self, column: <T as ValueType>::Column) -> Result<()> {
        let c = T::upcast_column(column.clone());
        let builder = ColumnBuilder::with_capacity(&c.data_type(), c.len());
        let mut builder = T::try_downcast_owned_builder(builder).unwrap();

        for (index, cnt) in self.indices {
            for _ in 0..*cnt {
                T::push_item(&mut builder, unsafe {
                    T::index_column_unchecked(&column, *index as usize)
                });
            }
        }
        self.result = Some(Value::Column(T::upcast_column(T::build_column(builder))));

        Ok(())
    }

    fn visit_number<T: Number>(
        &mut self,
        buffer: <NumberType<T> as ValueType>::Column,
    ) -> Result<()> {
        self.result = Some(Value::Column(NumberType::<T>::upcast_column(
            self.take_primitive_types(buffer),
        )));
        Ok(())
    }

    fn visit_timestamp(&mut self, buffer: Buffer<i64>) -> Result<()> {
        self.result = Some(Value::Column(TimestampType::upcast_column(
            self.take_primitive_types(buffer),
        )));
        Ok(())
    }

    fn visit_date(&mut self, buffer: Buffer<i32>) -> Result<()> {
        self.result = Some(Value::Column(DateType::upcast_column(
            self.take_primitive_types(buffer),
        )));
        Ok(())
    }

    fn visit_decimal<T: crate::types::Decimal>(
        &mut self,
        buffer: Buffer<T>,
        size: DecimalSize,
    ) -> Result<()> {
        self.result = Some(Value::Column(T::upcast_column(
            self.take_primitive_types(buffer),
            size,
        )));
        Ok(())
    }

    fn visit_binary(&mut self, col: BinaryColumn) -> Result<()> {
        self.result = Some(Value::Column(BinaryType::upcast_column(
            self.take_binary_types(&col),
        )));
        Ok(())
    }

    fn visit_string(&mut self, column: StringColumn) -> Result<()> {
        let column: BinaryColumn = column.into();
        self.result = Some(Value::Column(StringType::upcast_column(unsafe {
            StringColumn::from_binary_unchecked(self.take_binary_types(&column))
        })));
        Ok(())
    }

    fn visit_variant(&mut self, column: BinaryColumn) -> Result<()> {
        self.result = Some(Value::Column(VariantType::upcast_column(
            self.take_binary_types(&column),
        )));
        Ok(())
    }
}

impl<'a> TakeCompactVisitor<'a> {
    fn take_primitive_types<T: Copy>(&mut self, buffer: Buffer<T>) -> Buffer<T> {
        let col_ptr = buffer.as_slice().as_ptr();
        let mut builder: Vec<T> = Vec::with_capacity(self.num_rows);
        let mut ptr = builder.as_mut_ptr();
        let mut remain;

        unsafe {
            for (index, cnt) in self.indices.iter() {
                if *cnt == 1 {
                    copy_advance_aligned(col_ptr.add(*index as usize), &mut ptr, 1);
                    continue;
                }

                // Using the doubling method to copy the max segment memory.
                // [___________] => [x__________] => [xx_________] => [xxxx_______] => [xxxxxxxx___]
                // Since cnt > 0, then 31 - cnt.leading_zeros() >= 0.
                let max_segment = 1 << (31 - cnt.leading_zeros());
                let base_ptr = ptr;
                copy_advance_aligned(col_ptr.add(*index as usize), &mut ptr, 1);
                let mut cur_segment = 1;
                while cur_segment < max_segment {
                    copy_advance_aligned(base_ptr, &mut ptr, cur_segment);
                    cur_segment <<= 1;
                }

                // Copy the remaining memory directly.
                // [xxxxxxxxxx____] => [xxxxxxxxxxxxxx]
                //  ^^^^ ---> ^^^^
                remain = *cnt as usize - max_segment;
                if remain > 0 {
                    copy_advance_aligned(base_ptr, &mut ptr, remain);
                }
            }
            set_vec_len_by_ptr(&mut builder, ptr);
        }

        builder.into()
    }

    fn take_binary_types(&mut self, col: &BinaryColumn) -> BinaryColumn {
        // Each element of `items` is (string(&[u8]), repeat times).
        let mut items = Vec::with_capacity(self.indices.len());
        let mut items_ptr = items.as_mut_ptr();

        // [`BinaryColumn`] consists of [`data`] and [`offset`], we build [`data`] and [`offset`] respectively,
        // and then call `BinaryColumn::new(data.into(), offsets.into())` to create [`BinaryColumn`].
        let mut offsets = Vec::with_capacity(self.num_rows + 1);
        let mut offsets_ptr = offsets.as_mut_ptr();
        let mut data_size = 0;

        // Build [`offset`] and calculate `data_size` required by [`data`].
        unsafe {
            store_advance_aligned::<u64>(0, &mut offsets_ptr);
            for (index, cnt) in self.indices.iter() {
                let item = col.index_unchecked(*index as usize);
                store_advance_aligned((item, *cnt), &mut items_ptr);
                for _ in 0..*cnt {
                    data_size += item.len() as u64;
                    store_advance_aligned(data_size, &mut offsets_ptr);
                }
            }
            set_vec_len_by_ptr(&mut offsets, offsets_ptr);
            set_vec_len_by_ptr(&mut items, items_ptr);
        }

        // Build [`data`].
        let mut data: Vec<u8> = Vec::with_capacity(data_size as usize);
        let mut data_ptr = data.as_mut_ptr();
        let mut remain;

        unsafe {
            for (item, cnt) in items {
                let len = item.len();
                if cnt == 1 {
                    copy_advance_aligned(item.as_ptr(), &mut data_ptr, len);
                    continue;
                }

                // Using the doubling method to copy the max segment memory.
                // [___________] => [x__________] => [xx_________] => [xxxx_______] => [xxxxxxxx___]
                // Since cnt > 0, then 31 - cnt.leading_zeros() >= 0.
                let max_bit_num = 1 << (31 - cnt.leading_zeros());
                let max_segment = max_bit_num * len;
                let base_data_ptr = data_ptr;
                copy_advance_aligned(item.as_ptr(), &mut data_ptr, len);
                let mut cur_segment = len;
                while cur_segment < max_segment {
                    copy_advance_aligned(base_data_ptr, &mut data_ptr, cur_segment);
                    cur_segment <<= 1;
                }

                // Copy the remaining memory directly.
                // [xxxxxxxxxx____] => [xxxxxxxxxxxxxx]
                //  ^^^^ ---> ^^^^
                remain = cnt as usize - max_bit_num;
                if remain > 0 {
                    copy_advance_aligned(base_data_ptr, &mut data_ptr, remain * len);
                }
            }
            set_vec_len_by_ptr(&mut data, data_ptr);
        }

        BinaryColumn::new(data.into(), offsets.into())
    }
}
