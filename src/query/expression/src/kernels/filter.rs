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

use databend_common_arrow::arrow::bitmap::utils::BitChunkIterExact;
use databend_common_arrow::arrow::bitmap::utils::BitChunksExact;
use databend_common_arrow::arrow::bitmap::utils::SlicesIterator;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_arrow::arrow::bitmap::TrueIdxIter;
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
                    filter_visitor.filter_rows,
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

/// The iteration strategy used to evaluate [`FilterVisitor`]
#[derive(Debug)]
pub enum IterationStrategy {
    None,
    All,
    /// Range iterator
    SlicesIterator,
    /// True index iterator
    IndexIterator,
}

/// based on <https://dl.acm.org/doi/abs/10.1145/3465998.3466009>
const SELECTIVITY_THRESHOLD: f64 = 0.8;

impl IterationStrategy {
    fn default_strategy(length: usize, true_count: usize) -> Self {
        if length == 0 || true_count == 0 {
            return IterationStrategy::None;
        }
        if length == true_count {
            return IterationStrategy::All;
        }
        let selectivity_frac = true_count as f64 / length as f64;
        if selectivity_frac > SELECTIVITY_THRESHOLD {
            return IterationStrategy::SlicesIterator;
        }
        IterationStrategy::IndexIterator
    }
}

pub struct FilterVisitor<'a> {
    filter: &'a Bitmap,
    result: Option<Value<AnyType>>,
    filter_rows: usize,
    original_rows: usize,
    strategy: IterationStrategy,
}

impl<'a> FilterVisitor<'a> {
    pub fn new(filter: &'a Bitmap) -> Self {
        let filter_rows = filter.len() - filter.unset_bits();
        let strategy = IterationStrategy::default_strategy(filter.len(), filter_rows);
        Self {
            filter,
            result: None,
            filter_rows,
            original_rows: filter.len(),
            strategy,
        }
    }

    pub fn with_strategy(mut self, strategy: IterationStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn take_result(&mut self) -> Option<Value<AnyType>> {
        self.result.take()
    }
}

impl<'a> ValueVisitor for FilterVisitor<'a> {
    fn visit_value(&mut self, value: Value<AnyType>) -> Result<()> {
        match value {
            Value::Scalar(c) => self.visit_scalar(c),
            Value::Column(c) => {
                assert!(c.len() == self.original_rows);
                match self.strategy {
                    IterationStrategy::None => self.result = Some(Value::Column(c.slice(0..0))),
                    IterationStrategy::All => self.result = Some(Value::Column(c)),
                    IterationStrategy::SlicesIterator | IterationStrategy::IndexIterator => {
                        self.visit_column(c)?
                    }
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
        match self.strategy {
            IterationStrategy::IndexIterator => {
                let iter = TrueIdxIter::new(self.original_rows, Some(self.filter));
                iter.for_each(|index| {
                    T::push_item(&mut builder, unsafe {
                        T::index_column_unchecked(&column, index)
                    })
                });
            }
            _ => {
                let iter = SlicesIterator::new(self.filter);
                iter.for_each(|(start, len)| {
                    T::append_column(&mut builder, &T::slice_column(&column, start..start + len))
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
            bitmap.slice(0, self.filter_rows);
            self.result = Some(Value::Column(BooleanType::upcast_column(bitmap)));
            return Ok(());
        }

        let bitmap = match self.strategy {
            IterationStrategy::IndexIterator => {
                let iter = TrueIdxIter::new(self.original_rows, Some(self.filter));
                MutableBitmap::from_trusted_len_iter(iter.map(|index| bitmap.get_bit(index))).into()
            }
            _ => {
                let src = bitmap.values();
                let offset = bitmap.offset();

                let mut builder = MutableBitmap::with_capacity(self.filter_rows);
                let iter = SlicesIterator::new(self.filter);
                iter.for_each(|(start, len)| {
                    builder.append_packed_range(start + offset..start + len + offset, src)
                });
                builder.into()
            }
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
        match self.strategy {
            IterationStrategy::IndexIterator => {
                let iter = TrueIdxIter::new(self.original_rows, Some(self.filter));
                Vec::from_iter(iter.map(|index| buffer[index])).into()
            }
            _ => {
                let mut builder = Vec::with_capacity(self.filter_rows);
                let iter = SlicesIterator::new(self.filter);
                iter.for_each(|(start, len)| {
                    builder.extend_from_slice(&buffer[start..start + len]);
                });
                builder.into()
            }
        }
    }

    // TODO: optimize this after BinaryView is introduced by @andy
    fn filter_binary_types(&mut self, values: &BinaryColumn) -> BinaryColumn {
        // Each element of `items` is (string pointer(u64), string length).
        let mut items: Vec<(u64, usize)> = Vec::with_capacity(self.filter_rows);
        // [`BinaryColumn`] consists of [`data`] and [`offset`], we build [`data`] and [`offset`] respectively,
        // and then call `BinaryColumn::new(data.into(), offsets.into())` to create [`BinaryColumn`].
        let values_offset = values.offsets().as_slice();
        let values_data_ptr = values.data().as_slice().as_ptr();
        let mut offsets: Vec<u64> = Vec::with_capacity(self.filter_rows + 1);
        let mut offsets_ptr = offsets.as_mut_ptr();
        let mut items_ptr = items.as_mut_ptr();
        let mut data_size = 0;

        // Build [`offset`] and calculate `data_size` required by [`data`].
        unsafe {
            store_advance_aligned::<u64>(0, &mut offsets_ptr);
            let mut idx = 0;
            let (mut slice, offset, mut length) = self.filter.as_slice();
            if offset > 0 {
                let mut mask = slice[0];
                while mask != 0 {
                    let n = mask.trailing_zeros() as usize;
                    // If `offset` > 0, the valid bits of this byte start at `offset`, we also
                    // need to ensure that we cannot iterate more than `length` bits.
                    if n >= offset && n < offset + length {
                        let start = *values_offset.get_unchecked(n - offset) as usize;
                        let len = *values_offset.get_unchecked(n - offset + 1) as usize - start;
                        data_size += len as u64;
                        store_advance_aligned(data_size, &mut offsets_ptr);
                        store_advance_aligned(
                            (values_data_ptr.add(start) as u64, len),
                            &mut items_ptr,
                        );
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
                idx += bits_to_align;
            }

            const CHUNK_SIZE: usize = 64;
            let mut mask_chunks = BitChunksExact::<u64>::new(slice, length);
            let mut continuous_selected = 0;
            for mut mask in mask_chunks.by_ref() {
                if mask == u64::MAX {
                    continuous_selected += CHUNK_SIZE;
                } else {
                    if continuous_selected > 0 {
                        let start = *values_offset.get_unchecked(idx) as usize;
                        let len = *values_offset.get_unchecked(idx + continuous_selected) as usize
                            - start;
                        store_advance_aligned(
                            (values_data_ptr.add(start) as u64, len),
                            &mut items_ptr,
                        );
                        for i in 0..continuous_selected {
                            data_size += *values_offset.get_unchecked(idx + i + 1)
                                - *values_offset.get_unchecked(idx + i);
                            store_advance_aligned(data_size, &mut offsets_ptr);
                        }
                        idx += continuous_selected;
                        continuous_selected = 0;
                    }
                    while mask != 0 {
                        let n = mask.trailing_zeros() as usize;
                        let start = *values_offset.get_unchecked(idx + n) as usize;
                        let len = *values_offset.get_unchecked(idx + n + 1) as usize - start;
                        data_size += len as u64;
                        store_advance_aligned(
                            (values_data_ptr.add(start) as u64, len),
                            &mut items_ptr,
                        );
                        store_advance_aligned(data_size, &mut offsets_ptr);
                        mask = mask & (mask - 1);
                    }
                    idx += CHUNK_SIZE;
                }
            }
            if continuous_selected > 0 {
                let start = *values_offset.get_unchecked(idx) as usize;
                let len = *values_offset.get_unchecked(idx + continuous_selected) as usize - start;
                store_advance_aligned((values_data_ptr.add(start) as u64, len), &mut items_ptr);
                for i in 0..continuous_selected {
                    data_size += *values_offset.get_unchecked(idx + i + 1)
                        - *values_offset.get_unchecked(idx + i);
                    store_advance_aligned(data_size, &mut offsets_ptr);
                }
                idx += continuous_selected;
            }

            for (i, is_selected) in mask_chunks.remainder_iter().enumerate() {
                if is_selected {
                    let start = *values_offset.get_unchecked(idx + i) as usize;
                    let len = *values_offset.get_unchecked(idx + i + 1) as usize - start;
                    data_size += len as u64;
                    store_advance_aligned((values_data_ptr.add(start) as u64, len), &mut items_ptr);
                    store_advance_aligned(data_size, &mut offsets_ptr);
                }
            }
            set_vec_len_by_ptr(&mut items, items_ptr);
            set_vec_len_by_ptr(&mut offsets, offsets_ptr);
        }

        // Build [`data`].
        let mut data: Vec<u8> = Vec::with_capacity(data_size as usize);
        let mut data_ptr = data.as_mut_ptr();

        unsafe {
            for (str_ptr, len) in items.iter() {
                copy_advance_aligned(*str_ptr as *const u8, &mut data_ptr, *len);
            }
            set_vec_len_by_ptr(&mut data, data_ptr);
        }

        BinaryColumn::new(data.into(), offsets.into())
    }
}
