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

use std::mem::MaybeUninit;
use std::ops::Range;

use binary::BinaryColumnBuilder;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_exception::Result;
use string::StringColumnBuilder;

use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::Value;
use crate::kernels::mask_segments::Segment;
use crate::kernels::mask_segments::for_each_segment;
use crate::types::binary::BinaryColumn;
use crate::types::nullable::NullableColumn;
use crate::types::string::StringColumn;
use crate::types::*;
use crate::visitor::ValueVisitor;

impl DataBlock {
    pub fn filter_with_bitmap(self, bitmap: &Bitmap) -> Result<DataBlock> {
        if self.num_rows() == 0 {
            return Ok(self);
        }

        let count_zeros = bitmap.null_count();
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
                        filter_visitor.visit_value(entry.value())?;
                        let result = filter_visitor.result.take().unwrap();
                        Ok(BlockEntry::new(result, || {
                            (entry.data_type(), filter_visitor.filter_rows)
                        }))
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
///
/// Under both `SlicesIterator` and `IndexIterator`, a column is filtered one
/// 64-row word of the filter at a time (see `filter_rows`), which adapts to
/// the density of each word. The two strategies differ only for string
/// columns.
#[derive(Debug)]
pub enum IterationStrategy {
    None,
    All,
    /// String columns keep their data buffers and filter the views.
    SlicesIterator,
    /// String columns copy the selected strings into new buffers.
    IndexIterator,
}

/// based on <https://dl.acm.org/doi/abs/10.1145/3465998.3466009>
pub const SELECTIVITY_THRESHOLD: f64 = 0.8;

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
        let filter_rows = filter.len() - filter.null_count();
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

impl ValueVisitor for FilterVisitor<'_> {
    fn visit_value(&mut self, value: Value<AnyType>) -> Result<()> {
        match value {
            Value::Scalar(c) => self.visit_scalar(c),
            Value::Column(c) => {
                assert_eq!(c.len(), self.original_rows);
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

    fn visit_typed_column<T: ValueType>(
        &mut self,
        column: T::Column,
        data_type: &DataType,
    ) -> Result<()> {
        let c = T::upcast_column_with_type(column.clone(), data_type);
        let mut builder = ColumnBuilder::with_capacity(&c.data_type(), self.filter_rows);
        filter_rows(self.filter, &mut TypedSink::<T> {
            column: &column,
            builder: T::downcast_builder(&mut builder),
        });
        self.result = Some(Value::Column(builder.build()));
        Ok(())
    }

    fn visit_number<T: Number>(&mut self, buffer: Buffer<T>) -> Result<()> {
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
        if bitmap.null_count() == 0 {
            bitmap.slice(0, self.filter_rows);
            self.result = Some(Value::Column(BooleanType::upcast_column(bitmap)));
            return Ok(());
        }

        self.result = Some(Value::Column(BooleanType::upcast_column(filter_bitmap(
            self.filter,
            &bitmap,
        ))));
        Ok(())
    }

    fn visit_binary(&mut self, col: BinaryColumn) -> Result<()> {
        self.result = Some(Value::Column(BinaryType::upcast_column(
            self.filter_binary_types(&col),
        )));
        Ok(())
    }

    fn visit_string(&mut self, column: StringColumn) -> Result<()> {
        self.result = Some(Value::Column(StringType::upcast_column(
            self.filter_string_types(&column),
        )));
        Ok(())
    }

    fn visit_variant(&mut self, column: BinaryColumn) -> Result<()> {
        self.result = Some(Value::Column(VariantType::upcast_column(
            self.filter_binary_types(&column),
        )));
        Ok(())
    }

    /// Filters the encoded values (keys / deltas) and keeps the encoding.
    fn visit_encoded(&mut self, mut column: Box<EncodedColumn>) -> Result<()> {
        // Reuses the box of the input column.
        *column = column.map_codes(|codes| self.filter_narrow(codes));
        self.result = Some(Value::Column(Column::Encoded(column)));
        Ok(())
    }
}

impl FilterVisitor<'_> {
    fn filter_narrow(&mut self, buffer: NarrowBuffer) -> NarrowBuffer {
        crate::map_narrow_buffer!(buffer, |b| self.filter_primitive_types(b))
    }

    fn filter_primitive_types<T: Copy>(&mut self, buffer: Buffer<T>) -> Buffer<T> {
        let mut sink = FixedSink {
            values: buffer.as_slice(),
            out: Vec::with_capacity(self.filter_rows),
        };
        filter_rows(self.filter, &mut sink);
        sink.out.into()
    }

    fn filter_string_types(&mut self, values: &StringColumn) -> StringColumn {
        match self.strategy {
            IterationStrategy::IndexIterator => {
                let mut sink = StringSink {
                    values,
                    builder: StringColumnBuilder::with_capacity(self.filter_rows),
                };
                filter_rows(self.filter, &mut sink);
                sink.builder.build()
            }
            _ => {
                // reuse the buffers
                let new_views = self.filter_primitive_types(values.views().clone());
                unsafe {
                    StringColumn::new_unchecked_unknown_md(
                        new_views,
                        values.data_buffers().clone(),
                        None,
                    )
                }
            }
        }
    }

    fn filter_binary_types(&mut self, values: &BinaryColumn) -> BinaryColumn {
        let mut sink = BinarySink {
            values,
            builder: BinaryColumnBuilder::with_capacity(self.filter_rows, 0),
        };
        filter_rows(self.filter, &mut sink);
        sink.builder.build()
    }
}

/// A mixed word with at most this many set bits copies its selected values
/// one set bit at a time; a denser word uses a branch-free loop over all of
/// its rows.
const SPARSE_WORD_BITS: u32 = 16;

/// A mixed word with at least this many set bits is copied as the runs
/// between its unset bits, merged with the runs of the neighboring words.
const DENSE_WORD_BITS: u32 = 60;

/// Receives the selected rows of a filter from [`filter_rows`], in row order.
trait RowSink {
    /// Copies the rows of `rows`, which are all selected.
    fn copy_run(&mut self, rows: Range<usize>);

    /// Copies the rows `start + i` for the set bits `i` of `word`, a word with
    /// both set and unset bits; `ones` is its number of set bits, and `len` its
    /// number of rows.
    fn copy_word(&mut self, word: u64, ones: u32, start: usize, len: usize);
}

/// Passes the selected rows of `mask` to `sink`. Runs of selected rows (whole
/// set words, and the parts of dense words between their unset bits, merged
/// across words) are copied in bulk; words with no set bit are skipped.
fn filter_rows(mask: &Bitmap, sink: &mut impl RowSink) {
    let mut runs = RunMerger { sink, run: 0..0 };
    for_each_segment(mask, |segment| match segment {
        Segment::Run {
            set: true,
            start,
            len,
        } => runs.push(start, start + len),
        Segment::Run { set: false, .. } => {}
        Segment::Mixed { word, start, len } => {
            let ones = word.count_ones();
            if ones >= DENSE_WORD_BITS {
                let mut row = 0;
                for_each_set_bit(!word & low_bits(len), |unset| {
                    if unset > row {
                        runs.push(start + row, start + unset);
                    }
                    row = unset + 1;
                });
                if row < len {
                    runs.push(start + row, start + len);
                }
            } else {
                runs.flush();
                runs.sink.copy_word(word, ones, start, len);
            }
        }
    });
    runs.flush();
}

/// Merges consecutive runs of rows before passing them to the sink.
struct RunMerger<'a, S> {
    sink: &'a mut S,
    /// The pending run.
    run: Range<usize>,
}

impl<S: RowSink> RunMerger<'_, S> {
    #[inline]
    fn push(&mut self, start: usize, end: usize) {
        if start != self.run.end {
            self.flush();
            self.run.start = start;
        }
        self.run.end = end;
    }

    #[inline]
    fn flush(&mut self) {
        if !self.run.is_empty() {
            self.sink.copy_run(self.run.clone());
            self.run.start = self.run.end;
        }
    }
}

/// Calls `f` with the position of each set bit of `word`, lowest first.
#[inline(always)]
fn for_each_set_bit(mut word: u64, mut f: impl FnMut(usize)) {
    while word != 0 {
        f(word.trailing_zeros() as usize);
        word &= word - 1;
    }
}

/// The bits below `len`, for `len <= 64`.
#[inline]
fn low_bits(len: usize) -> u64 {
    if len == 64 { u64::MAX } else { (1 << len) - 1 }
}

/// Fixed-width values.
struct FixedSink<'a, T> {
    values: &'a [T],
    out: Vec<T>,
}

impl<T: Copy> RowSink for FixedSink<'_, T> {
    #[inline]
    fn copy_run(&mut self, rows: Range<usize>) {
        self.out.extend_from_slice(&self.values[rows]);
    }

    #[inline]
    fn copy_word(&mut self, word: u64, ones: u32, start: usize, len: usize) {
        let src = &self.values[start..start + len];
        let out = &mut self.out;
        // The selected values of a word are written to the next 64 free slots
        // of `out`; near the end of `out` they are pushed one by one.
        let Some(window) = out.spare_capacity_mut().first_chunk_mut::<64>() else {
            for_each_set_bit(word, |i| out.push(src[i]));
            return;
        };
        // A mixed word has fewer than 64 set bits, so `selected & 63` is
        // `selected`; the mask lets the compiler drop the bounds check.
        let mut selected = 0;
        if ones <= SPARSE_WORD_BITS {
            for_each_set_bit(word, |i| {
                window[selected & 63] = MaybeUninit::new(src[i]);
                selected += 1;
            });
        } else {
            // Writes every value to the next slot, and moves to the next slot
            // only when the value is selected.
            for (i, value) in src.iter().enumerate() {
                window[selected & 63] = MaybeUninit::new(*value);
                selected += ((word >> i) & 1) as usize;
            }
        }
        let len = out.len() + selected;
        // SAFETY: the first `selected` slots after the end of `out` are
        // initialized, and they are within its capacity.
        unsafe { out.set_len(len) };
    }
}

/// Copies the selected strings into new buffers.
struct StringSink<'a> {
    values: &'a StringColumn,
    builder: StringColumnBuilder,
}

impl StringSink<'_> {
    #[inline]
    fn copy_row(&mut self, row: usize) {
        // SAFETY: `row` is a row of the filter, which has the length of the
        // column.
        unsafe {
            self.builder
                .put_and_commit(self.values.value_unchecked(row))
        };
    }
}

impl RowSink for StringSink<'_> {
    fn copy_run(&mut self, rows: Range<usize>) {
        rows.for_each(|row| self.copy_row(row));
    }

    fn copy_word(&mut self, word: u64, _ones: u32, start: usize, _len: usize) {
        for_each_set_bit(word, |i| self.copy_row(start + i));
    }
}

/// `Binary` and `Variant` values.
struct BinarySink<'a> {
    values: &'a BinaryColumn,
    builder: BinaryColumnBuilder,
}

impl RowSink for BinarySink<'_> {
    fn copy_run(&mut self, rows: Range<usize>) {
        self.builder.append_column(&self.values.slice(rows));
    }

    fn copy_word(&mut self, word: u64, _ones: u32, start: usize, _len: usize) {
        for_each_set_bit(word, |i| {
            // SAFETY: `start + i` is a row of the filter, which has the length
            // of the column.
            self.builder
                .put_slice(unsafe { self.values.index_unchecked(start + i) });
            self.builder.commit_row();
        });
    }
}

/// Any other type, through its `ValueType`.
struct TypedSink<'a, 'b, T: ValueType> {
    column: &'a T::Column,
    builder: T::ColumnBuilderMut<'b>,
}

impl<T: ValueType> RowSink for TypedSink<'_, '_, T> {
    fn copy_run(&mut self, rows: Range<usize>) {
        T::append_column_mut(&mut self.builder, &T::slice_column(self.column, rows));
    }

    fn copy_word(&mut self, word: u64, _ones: u32, start: usize, _len: usize) {
        for_each_set_bit(word, |i| {
            // SAFETY: `start + i` is a row of the filter, which has the length
            // of the column.
            let item = unsafe { T::index_column_unchecked(self.column, start + i) };
            T::push_item_mut(&mut self.builder, item);
        });
    }
}

/// Filters a bitmap one 64-bit word at a time: a mask word with all bits set
/// keeps the value word, a mixed word packs its selected value bits.
fn filter_bitmap(mask: &Bitmap, values: &Bitmap) -> Bitmap {
    assert_eq!(values.len(), mask.len());
    let mut out = BitPacker::with_capacity(mask.true_count());
    let mut mask_words = mask.chunks::<u64>();
    let mut value_words = values.chunks::<u64>();
    for (mask_word, value_word) in mask_words.by_ref().zip(value_words.by_ref()) {
        out.push_selected(value_word, mask_word);
    }
    let remainder_len = mask_words.remainder_len();
    if remainder_len > 0 {
        let mask_word = mask_words.remainder() & low_bits(remainder_len);
        out.push_selected(value_words.remainder(), mask_word);
    }
    out.finish()
}

/// Appends bits to a bitmap 64 at a time.
struct BitPacker {
    bytes: Vec<u8>,
    /// The pending bits, `pending_len` of them.
    pending: u64,
    pending_len: usize,
    len: usize,
}

impl BitPacker {
    fn with_capacity(bits: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(bits.div_ceil(64) * 8),
            pending: 0,
            pending_len: 0,
            len: 0,
        }
    }

    /// Appends the bits of `values` at the set bits of `mask`.
    #[inline]
    fn push_selected(&mut self, values: u64, mask: u64) {
        match mask {
            0 => {}
            u64::MAX => self.push(values, 64),
            _ => self.push(compress_bits(values, mask), mask.count_ones() as usize),
        }
    }

    /// Appends the low `len` bits of `bits`; the higher bits must be unset.
    #[inline]
    fn push(&mut self, bits: u64, len: usize) {
        debug_assert!(len == 64 || bits >> len == 0);
        self.pending |= bits << self.pending_len;
        self.len += len;
        let pending_len = self.pending_len + len;
        if pending_len < 64 {
            self.pending_len = pending_len;
            return;
        }
        self.bytes.extend_from_slice(&self.pending.to_le_bytes());
        // The bits of `bits` that did not fit in the flushed word.
        let flushed = 64 - self.pending_len;
        self.pending = if flushed == 64 { 0 } else { bits >> flushed };
        self.pending_len = pending_len - 64;
    }

    fn finish(mut self) -> Bitmap {
        let pending_bytes = self.pending_len.div_ceil(8);
        self.bytes
            .extend_from_slice(&self.pending.to_le_bytes()[..pending_bytes]);
        Bitmap::from_u8_vec(self.bytes, self.len)
    }
}

/// Packs the bits of `values` at the set bits of `mask` into the low bits of
/// the result, in order (a software `pext`). Loops over the set bits or the
/// unset bits of `mask`, whichever are fewer.
#[inline]
fn compress_bits(values: u64, mask: u64) -> u64 {
    if mask.count_ones() < 32 {
        let mut out = 0;
        let mut next = 0;
        for_each_set_bit(mask, |i| {
            out |= ((values >> i) & 1) << next;
            next += 1;
        });
        out
    } else {
        // Removes the unset positions from the highest one down, so that the
        // positions still to remove do not move.
        let mut out = values;
        let mut unset = !mask;
        while unset != 0 {
            let i = 63 - unset.leading_zeros();
            let below = (1u64 << i) - 1;
            out = (out & below) | ((out >> 1) & !below);
            unset ^= 1 << i;
        }
        out
    }
}
