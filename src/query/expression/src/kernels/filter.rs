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
use databend_common_arrow::arrow::bitmap::utils::BitChunkIterExact;
use databend_common_arrow::arrow::bitmap::utils::BitChunksExact;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::bitmap::TrueIdxIter;
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
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::Value;

impl DataBlock {
    pub fn filter_with_bitmap(self, bitmap: &Bitmap) -> Result<DataBlock> {
        if self.num_rows() == 0 {
            return Ok(self);
        }

        let count_zeros = bitmap.unset_bits();
        match count_zeros {
            0 => Ok(self),
            _ => {
                if count_zeros == self.num_rows() {
                    return Ok(self.slice(0..0));
                }

                let mut filter_visitor = FilterVisitor::new(bitmap);

                let after_columns = self
                    .columns()
                    .iter()
                    .map(|entry| {
                        filter_visitor.visit_value(entry.value.clone())?;
                        let result = filter_visitor.result.take().unwrap();
                        Ok(BlockEntry {
                            value: result,
                            data_type: entry.data_type.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(DataBlock::new_with_meta(
                    after_columns,
                    self.num_rows() - count_zeros,
                    self.get_meta().cloned(),
                ))
            }
        }
    }

    pub fn filter_boolean_value(self, filter: &Value<BooleanType>) -> Result<DataBlock> {
        if self.num_rows() == 0 {
            return Ok(self);
        }

        match filter {
            Value::Scalar(s) => {
                if *s {
                    Ok(self)
                } else {
                    Ok(self.slice(0..0))
                }
            }
            Value::Column(bitmap) => Self::filter_with_bitmap(self, bitmap),
        }
    }
}

impl Column {
    pub fn filter(&self, bitmap: &Bitmap) -> Column {
        let mut filter_visitor = FilterVisitor::new(bitmap);
        filter_visitor
            .visit_value(Value::Column(self.clone()))
            .unwrap();
        filter_visitor
            .result
            .take()
            .unwrap()
            .as_column()
            .unwrap()
            .clone()
    }
}

struct FilterVisitor<'a> {
    filter: &'a Bitmap,
    result: Option<Value<AnyType>>,
    num_rows: usize,
    original_rows: usize,
}

impl<'a> FilterVisitor<'a> {
    pub fn new(filter: &'a Bitmap) -> Self {
        let num_rows = filter.len() - filter.unset_bits();
        Self {
            filter,
            result: None,
            num_rows,
            original_rows: filter.len(),
        }
    }
}

impl<'a> ValueVisitor for FilterVisitor<'a> {
    fn visit_value(&mut self, value: Value<AnyType>) -> Result<()> {
        match value {
            Value::Scalar(c) => self.visit_scalar(c),
            Value::Column(c) => {
                assert!(c.len() == self.original_rows);

                if c.len() == self.num_rows || c.len() == 0 {
                    self.result = Some(Value::Column(c));
                } else if self.num_rows == 0 {
                    self.result = Some(Value::Column(c.slice(0..0)));
                } else {
                    self.visit_column(c)?;
                }
                Ok(())
            }
        }
    }

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

        const CHUNK_SIZE: usize = 64;
        let (mut slice, offset, mut length) = self.filter.as_slice();
        let mut start_index: usize = 0;
        if offset > 0 {
            // If `offset` > 0, the valid bits of this byte start at `offset`, and the
            // max num of valid bits is `8 - offset`, but we also need to ensure that
            // we cannot iterate more than `length` bits.
            let n = std::cmp::min(8 - offset, length);
            start_index += n;
            self.filter
                .iter()
                .enumerate()
                .take(n)
                .for_each(|(index, is_selected)| {
                    if is_selected {
                        T::push_item(&mut builder, T::index_column(&column, index).unwrap());
                    }
                });
            slice = &slice[1..];
            length = if length >= n { length - n } else { 0 };
        }

        let mut mask_chunks = BitChunksExact::<u64>::new(slice, length);

        mask_chunks
            .by_ref()
            .enumerate()
            .for_each(|(mask_index, mut mask)| {
                while mask != 0 {
                    let n = mask.trailing_zeros() as usize;
                    let index = mask_index * CHUNK_SIZE + n + start_index;
                    T::push_item(&mut builder, T::index_column(&column, index).unwrap());
                    mask = mask & (mask - 1);
                }
            });

        let remainder_start = length - length % CHUNK_SIZE;
        mask_chunks
            .remainder_iter()
            .enumerate()
            .for_each(|(mask_index, is_selected)| {
                if is_selected {
                    let index = mask_index + remainder_start + start_index;
                    T::push_item(&mut builder, T::index_column(&column, index).unwrap());
                }
            });

        self.result = Some(Value::Column(T::upcast_column(T::build_column(builder))));
        Ok(())
    }

    fn visit_number<T: Number>(
        &mut self,
        buffer: <NumberType<T> as ValueType>::Column,
    ) -> Result<()> {
        self.result = Some(Value::Column(NumberType::<T>::upcast_column(
            self.filter_primitive_types(buffer),
        )));
        Ok(())
    }

    fn visit_timestamp(&mut self, buffer: Buffer<i64>) -> Result<()> {
        self.result = Some(Value::Column(TimestampType::upcast_column(
            self.filter_primitive_types(buffer),
        )));
        Ok(())
    }

    fn visit_date(&mut self, buffer: Buffer<i32>) -> Result<()> {
        self.result = Some(Value::Column(DateType::upcast_column(
            self.filter_primitive_types(buffer),
        )));
        Ok(())
    }

    fn visit_decimal<T: crate::types::Decimal>(
        &mut self,
        buffer: Buffer<T>,
        size: DecimalSize,
    ) -> Result<()> {
        self.result = Some(Value::Column(T::upcast_column(
            self.filter_primitive_types(buffer),
            size,
        )));
        Ok(())
    }

    fn visit_boolean(&mut self, mut bitmap: Bitmap) -> Result<()> {
        // faster path for all bits set
        if bitmap.unset_bits() == 0 {
            bitmap.slice(0, self.num_rows);
            self.result = Some(Value::Column(BooleanType::upcast_column(bitmap)));
            return Ok(());
        }

        let capacity = self.num_rows.saturating_add(7) / 8;
        let mut builder: Vec<u8> = Vec::with_capacity(capacity);
        let mut builder_ptr = builder.as_mut_ptr();
        let mut builder_idx = 0;
        let mut unset_bits = 0;
        let mut buf = 0;

        let (bitmap_slice, bitmap_offset, _) = bitmap.as_slice();
        let mut bitmap_idx = 0;

        let (mut filter_slice, filter_offset, mut filter_length) = self.filter.as_slice();
        unsafe {
            if filter_offset > 0 {
                let mut mask = filter_slice[0];
                while mask != 0 {
                    let n = mask.trailing_zeros() as usize;
                    // If `filter_length` > 0, the valid bits of this byte start at `filter_offset`, we also
                    // need to ensure that we cannot iterate more than `filter_length` bits.
                    if n >= filter_offset && n < filter_offset + filter_length {
                        if bitmap.get_bit_unchecked(n - filter_offset) {
                            buf |= BIT_MASK[builder_idx % 8];
                        } else {
                            unset_bits += 1;
                        }
                        builder_idx += 1;
                    }
                    mask = mask & (mask - 1);
                }
                let bits_to_align = 8 - filter_offset;
                filter_length = if filter_length >= bits_to_align {
                    filter_length - bits_to_align
                } else {
                    0
                };
                filter_slice = &filter_slice[1..];
                bitmap_idx += bits_to_align;
            }

            const CHUNK_SIZE: usize = 64;
            let mut mask_chunks = BitChunksExact::<u64>::new(filter_slice, filter_length);
            let mut continuous_selected = 0;
            for mut mask in mask_chunks.by_ref() {
                if mask == u64::MAX {
                    continuous_selected += CHUNK_SIZE;
                } else {
                    if continuous_selected > 0 {
                        if builder_idx % 8 != 0 {
                            while continuous_selected > 0 {
                                if bitmap.get_bit_unchecked(bitmap_idx) {
                                    buf |= BIT_MASK[builder_idx % 8];
                                } else {
                                    unset_bits += 1;
                                }
                                bitmap_idx += 1;
                                builder_idx += 1;
                                continuous_selected -= 1;
                                if builder_idx % 8 == 0 {
                                    store_advance_aligned(buf, &mut builder_ptr);
                                    buf = 0;
                                    break;
                                }
                            }
                        }

                        if continuous_selected > 0 {
                            let (cur_buf, cur_unset_bits) = copy_continuous_bits(
                                &mut builder_ptr,
                                bitmap_slice,
                                builder_idx,
                                bitmap_idx + bitmap_offset,
                                continuous_selected,
                            );
                            builder_idx += continuous_selected;
                            bitmap_idx += continuous_selected;
                            unset_bits += cur_unset_bits;
                            buf = cur_buf;
                            continuous_selected = 0;
                        }
                    }

                    while mask != 0 {
                        let n = mask.trailing_zeros() as usize;
                        if bitmap.get_bit_unchecked(bitmap_idx + n) {
                            buf |= BIT_MASK[builder_idx % 8];
                        } else {
                            unset_bits += 1;
                        }
                        builder_idx += 1;
                        if builder_idx % 8 == 0 {
                            store_advance_aligned(buf, &mut builder_ptr);
                            buf = 0;
                        }
                        mask = mask & (mask - 1);
                    }
                    bitmap_idx += CHUNK_SIZE;
                }
            }

            if continuous_selected > 0 {
                if builder_idx % 8 != 0 {
                    while continuous_selected > 0 {
                        if bitmap.get_bit_unchecked(bitmap_idx) {
                            buf |= BIT_MASK[builder_idx % 8];
                        } else {
                            unset_bits += 1;
                        }
                        bitmap_idx += 1;
                        builder_idx += 1;
                        continuous_selected -= 1;
                        if builder_idx % 8 == 0 {
                            store_advance_aligned(buf, &mut builder_ptr);
                            buf = 0;
                            break;
                        }
                    }
                }

                if continuous_selected > 0 {
                    let (cur_buf, cur_unset_bits) = copy_continuous_bits(
                        &mut builder_ptr,
                        bitmap_slice,
                        builder_idx,
                        bitmap_idx + bitmap_offset,
                        continuous_selected,
                    );
                    builder_idx += continuous_selected;
                    bitmap_idx += continuous_selected;
                    unset_bits += cur_unset_bits;
                    buf = cur_buf;
                }
            }

            for (i, is_selected) in mask_chunks.remainder_iter().enumerate() {
                if is_selected {
                    if bitmap.get_bit_unchecked(bitmap_idx + i) {
                        buf |= BIT_MASK[builder_idx % 8];
                    } else {
                        unset_bits += 1;
                    }
                    builder_idx += 1;
                    if builder_idx % 8 == 0 {
                        store_advance_aligned(buf, &mut builder_ptr);
                        buf = 0;
                    }
                }
            }

            if builder_idx % 8 != 0 {
                store_advance_aligned(buf, &mut builder_ptr);
            }
        }

        let bitmap = unsafe {
            set_vec_len_by_ptr(&mut builder, builder_ptr);
            Bitmap::from_inner(Arc::new(builder.into()), 0, self.num_rows, unset_bits)
                .ok()
                .unwrap()
        };
        self.result = Some(Value::Column(BooleanType::upcast_column(bitmap)));
        Ok(())
    }

    fn visit_binary(&mut self, col: BinaryColumn) -> Result<()> {
        self.result = Some(Value::Column(BinaryType::upcast_column(
            self.filter_binary_types(&col),
        )));
        Ok(())
    }

    fn visit_string(&mut self, column: StringColumn) -> Result<()> {
        let column: BinaryColumn = column.into();
        self.result = Some(Value::Column(StringType::upcast_column(unsafe {
            StringColumn::from_binary_unchecked(self.filter_binary_types(&column))
        })));
        Ok(())
    }

    fn visit_variant(&mut self, column: BinaryColumn) -> Result<()> {
        self.result = Some(Value::Column(VariantType::upcast_column(
            self.filter_binary_types(&column),
        )));
        Ok(())
    }
}

impl<'a> FilterVisitor<'a> {
    fn filter_primitive_types<T: Copy>(&mut self, buffer: Buffer<T>) -> Buffer<T> {
        let mut builder: Vec<T> = Vec::with_capacity(self.num_rows);
        let mut ptr = builder.as_mut_ptr();
        let mut values_ptr = buffer.as_slice().as_ptr();
        let (mut slice, offset, mut length) = self.filter.as_slice();

        unsafe {
            if offset > 0 {
                let mut mask = slice[0];
                while mask != 0 {
                    let n = mask.trailing_zeros() as usize;
                    // If `offset` > 0, the valid bits of this byte start at `offset`, we also
                    // need to ensure that we cannot iterate more than `length` bits.
                    if n >= offset && n < offset + length {
                        copy_advance_aligned(values_ptr.add(n - offset), &mut ptr, 1);
                    }
                    mask = mask & (mask - 1);
                }
                let bits_to_align = 8 - offset;
                length = if length >= bits_to_align {
                    length - bits_to_align
                } else {
                    0
                };
                slice = &slice[1..];
                values_ptr = values_ptr.add(bits_to_align);
            }

            const CHUNK_SIZE: usize = 64;
            let mut mask_chunks = BitChunksExact::<u64>::new(slice, length);
            let mut continuous_selected = 0;
            for mut mask in mask_chunks.by_ref() {
                if mask == u64::MAX {
                    continuous_selected += CHUNK_SIZE;
                } else {
                    if continuous_selected > 0 {
                        copy_advance_aligned(values_ptr, &mut ptr, continuous_selected);
                        values_ptr = values_ptr.add(continuous_selected);
                        continuous_selected = 0;
                    }
                    while mask != 0 {
                        let n = mask.trailing_zeros() as usize;
                        copy_advance_aligned(values_ptr.add(n), &mut ptr, 1);
                        mask = mask & (mask - 1);
                    }
                    values_ptr = values_ptr.add(CHUNK_SIZE);
                }
            }
            if continuous_selected > 0 {
                copy_advance_aligned(values_ptr, &mut ptr, continuous_selected);
                values_ptr = values_ptr.add(continuous_selected);
            }

            for (i, is_selected) in mask_chunks.remainder_iter().enumerate() {
                if is_selected {
                    copy_advance_aligned(values_ptr.add(i), &mut ptr, 1);
                }
            }

            set_vec_len_by_ptr(&mut builder, ptr);
        }

        builder.into()
    }

    fn filter_binary_types(&mut self, values: &BinaryColumn) -> BinaryColumn {
        let mut builder = BinaryColumnBuilder::with_capacity(self.num_rows, 0);
        let iter = TrueIdxIter::new(self.original_rows, Some(self.filter));
        for i in iter {
            unsafe {
                builder.put_slice(values.index_unchecked(i));
                builder.commit_row();
            }
        }
        builder.build()
    }
}
