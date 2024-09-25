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

use core::ops::Range;
use std::sync::Arc;

use arrow::buffer::BooleanBuffer;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_exception::Result;

use crate::copy_continuous_bits;
use crate::kernels::take::BIT_MASK;
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
    // Generate a new `DataBlock` by the specified indices ranges.
    pub fn take_ranges(self, ranges: &[Range<u32>], num_rows: usize) -> Result<DataBlock> {
        debug_assert_eq!(
            ranges
                .iter()
                .map(|range| range.end - range.start)
                .sum::<u32>() as usize,
            num_rows
        );

        let mut taker = TakeRangeVisitor::new(ranges, num_rows);
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

struct TakeRangeVisitor<'a> {
    ranges: &'a [Range<u32>],
    num_rows: usize,
    result: Option<Value<AnyType>>,
}

impl<'a> TakeRangeVisitor<'a> {
    fn new(ranges: &'a [Range<u32>], num_rows: usize) -> Self {
        Self {
            ranges,
            num_rows,
            result: None,
        }
    }
}

impl<'a> ValueVisitor for TakeRangeVisitor<'a> {
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

        for range in self.ranges {
            for index in range.start as usize..range.end as usize {
                T::push_item(&mut builder, unsafe {
                    T::index_column_unchecked(&column, index)
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

    fn visit_boolean(&mut self, bitmap: BooleanBuffer) -> Result<()> {
        let capacity = self.num_rows.saturating_add(7) / 8;
        let mut builder: Vec<u8> = Vec::with_capacity(capacity);
        let mut builder_ptr = builder.as_mut_ptr();
        let mut builder_idx = 0;
        let mut unset_bits = 0;
        let mut buf = 0;

        let (bitmap_slice, bitmap_offset, _) = bitmap.as_slice();
        unsafe {
            for range in self.ranges {
                let mut start = range.start as usize;
                let end = range.end as usize;
                if builder_idx % 8 != 0 {
                    while start < end {
                        if bitmap.get_bit_unchecked(start) {
                            buf |= BIT_MASK[builder_idx % 8];
                        } else {
                            unset_bits += 1;
                        }
                        builder_idx += 1;
                        start += 1;
                        if builder_idx % 8 == 0 {
                            store_advance_aligned(buf, &mut builder_ptr);
                            buf = 0;
                            break;
                        }
                    }
                }
                let remaining = end - start;
                if remaining > 0 {
                    let (cur_buf, cur_unset_bits) = copy_continuous_bits(
                        &mut builder_ptr,
                        bitmap_slice,
                        builder_idx,
                        start + bitmap_offset,
                        remaining,
                    );
                    builder_idx += remaining;
                    unset_bits += cur_unset_bits;
                    buf = cur_buf;
                }
            }

            if builder_idx % 8 != 0 {
                store_advance_aligned(buf, &mut builder_ptr);
            }

            set_vec_len_by_ptr(&mut builder, builder_ptr);
            let bitmap = Bitmap::from_inner(Arc::new(builder.into()), 0, self.num_rows, unset_bits)
                .ok()
                .unwrap();
            self.result = Some(Value::Column(BooleanType::upcast_column(bitmap)));
            Ok(())
        }
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

impl<'a> TakeRangeVisitor<'a> {
    fn take_primitive_types<T: Copy>(&mut self, buffer: Buffer<T>) -> Buffer<T> {
        let mut builder: Vec<T> = Vec::with_capacity(self.num_rows);
        let values = buffer.as_slice();
        for range in self.ranges {
            builder.extend(&values[range.start as usize..range.end as usize]);
        }
        builder.into()
    }

    fn take_binary_types(&mut self, values: &BinaryColumn) -> BinaryColumn {
        let mut offsets: Vec<u64> = Vec::with_capacity(self.num_rows + 1);
        let mut data_size = 0;

        let value_data = values.data().as_slice();
        let values_offset = values.offsets().as_slice();
        // Build [`offset`] and calculate `data_size` required by [`data`].
        offsets.push(0);
        for range in self.ranges {
            let mut offset_start = values_offset[range.start as usize];
            for offset_end in values_offset[range.start as usize + 1..range.end as usize + 1].iter()
            {
                data_size += offset_end - offset_start;
                offset_start = *offset_end;
                offsets.push(data_size);
            }
        }

        // Build [`data`].
        let mut data: Vec<u8> = Vec::with_capacity(data_size as usize);
        let mut data_ptr = data.as_mut_ptr();

        unsafe {
            for range in self.ranges {
                let col_data = &value_data[values_offset[range.start as usize] as usize
                    ..values_offset[range.end as usize] as usize];
                copy_advance_aligned(col_data.as_ptr(), &mut data_ptr, col_data.len());
            }
            set_vec_len_by_ptr(&mut data, data_ptr);
        }

        BinaryColumn::new(data.into(), offsets.into())
    }
}
