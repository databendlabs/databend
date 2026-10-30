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

//! Row selection for a binary `if`: row `i` of the output comes from the then
//! value when bit `i` of the mask is set, and from the else value otherwise.
//!
//! The mask is read one 64-row word at a time. Consecutive words that are all
//! unset or all set are merged into a run and copied from one side in bulk;
//! the rows of a mixed word are selected one by one. Fixed-width values use a
//! branch-free select on mixed words, booleans and validity are combined with
//! word-wide bit operations, and every other type selects through its concrete
//! `ValueType`, with `AnyType` as the fallback.

use std::ops::Not;

use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::binary;
use databend_common_column::bitmap::ternary;
use databend_common_column::bitmap::utils::BitChunkIterExact;
use databend_common_column::bitmap::utils::BitChunksExact;
use databend_common_column::buffer::Buffer;

use crate::Column;
use crate::ColumnBuilder;
use crate::Scalar;
use crate::Value;
use crate::types::AccessType;
use crate::types::AnyType;
use crate::types::BinaryType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::DecimalDataKind;
use crate::types::DecimalDataType;
use crate::types::DecimalType;
use crate::types::NullableColumn;
use crate::types::NumberDataType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::TimestampType;
use crate::types::ValueType;
use crate::types::VariantType;
use crate::types::decimal::Decimal;
use crate::types::i256;
use crate::with_number_mapped_type;

/// Selects the rows of `then_value` where `mask` is set and of `else_value`
/// where it is unset.
///
/// Both values must have type `data_type`, and every column among them must
/// have `mask.len()` rows. Encoded columns are decoded.
pub fn select_if_then_else(
    mask: &Bitmap,
    then_value: &Value<AnyType>,
    else_value: &Value<AnyType>,
    data_type: &DataType,
) -> Column {
    for value in [then_value, else_value] {
        if let Value::Column(column) = value {
            assert_eq!(column.len(), mask.len());
        }
    }
    // Encoded columns are selected through their decoded values.
    let then_value = then_value.decoded();
    let else_value = else_value.decoded();
    let (then_value, else_value) = (then_value.as_ref(), else_value.as_ref());
    // An aggregate state is selected as its physical type.
    let physical_type = data_type.physical_type();
    let data_type = physical_type.as_ref();
    if has_nested_decimal(data_type) {
        return select_rows(mask, then_value, else_value, data_type);
    }
    select_typed(mask, then_value, else_value, data_type)
        .unwrap_or_else(|| select_values::<AnyType>(mask, then_value, else_value, data_type))
}

/// Whether `data_type` has a decimal inside an array, map or tuple (also
/// through the physical type of an aggregate state). A non-strict decimal (e.g.
/// `Decimal(10, 2)` carried by `Decimal128`) nested there can only be converted
/// to the strict width row by row, so such types skip the bulk paths.
pub(crate) fn has_nested_decimal(data_type: &DataType) -> bool {
    fn contains_decimal(data_type: &DataType) -> bool {
        match data_type {
            DataType::Decimal(_) => true,
            DataType::Nullable(inner) | DataType::Array(inner) | DataType::Map(inner) => {
                contains_decimal(inner)
            }
            DataType::Tuple(fields) => fields.iter().any(contains_decimal),
            DataType::AggregateState(state) => contains_decimal(state.physical_type()),
            _ => false,
        }
    }
    match data_type {
        DataType::Nullable(inner) => has_nested_decimal(inner),
        DataType::Array(inner) | DataType::Map(inner) => contains_decimal(inner),
        DataType::Tuple(fields) => fields.iter().any(contains_decimal),
        DataType::AggregateState(state) => has_nested_decimal(state.physical_type()),
        _ => false,
    }
}

/// Pushes every row through the generic builder.
fn select_rows(
    mask: &Bitmap,
    then_value: &Value<AnyType>,
    else_value: &Value<AnyType>,
    data_type: &DataType,
) -> Column {
    let mut builder = ColumnBuilder::with_capacity(data_type, mask.len());
    for (row, take_then) in mask.iter().enumerate() {
        let value = if take_then { then_value } else { else_value };
        // SAFETY: `row` is a row of the mask, which has the length of every column.
        builder.push(unsafe { value_item(value, row) });
    }
    builder.build()
}

/// Returns `None` when the values do not have the physical layout that the
/// typed selection of `data_type` expects; the caller then uses `AnyType`.
fn select_typed(
    mask: &Bitmap,
    then_value: &Value<AnyType>,
    else_value: &Value<AnyType>,
    data_type: &DataType,
) -> Option<Column> {
    match data_type {
        DataType::Nullable(inner) => {
            let (then_inner, then_validity) = split_nullable(then_value)?;
            let (else_inner, else_validity) = split_nullable(else_value)?;
            let validity = select_bitmap(mask, &then_validity, &else_validity);
            // The inner value of a NULL row is never read, so when one side is a
            // NULL scalar the other side provides the inner values of all rows.
            let column = match (then_inner, else_inner) {
                (Some(then_inner), Some(else_inner)) => {
                    select_if_then_else(mask, &then_inner, &else_inner, inner)
                }
                (Some(inner_value), None) | (None, Some(inner_value))
                    if DecimalDataType::from_value(&inner_value)
                        .is_none_or(|(ty, _)| ty.is_strict()) =>
                {
                    inner_value.convert_to_full_column(inner, mask.len())
                }
                // A non-strict decimal is converted by the selection.
                (Some(inner_value), None) | (None, Some(inner_value)) => {
                    select_if_then_else(mask, &inner_value, &inner_value, inner)
                }
                (None, None) => ColumnBuilder::repeat_default(inner, mask.len()).build(),
            };
            Some(NullableColumn::new_column(column, validity))
        }
        DataType::Boolean => {
            let then_value = then_value.try_downcast::<BooleanType>().ok()?;
            let else_value = else_value.try_downcast::<BooleanType>().ok()?;
            Some(Column::Boolean(select_bitmap(
                mask,
                &then_value,
                &else_value,
            )))
        }
        DataType::Number(number_type) => {
            with_number_mapped_type!(|NUM| match number_type {
                NumberDataType::NUM =>
                    select_fixed::<NumberType<NUM>>(mask, then_value, else_value, data_type),
            })
        }
        DataType::Decimal(size) => match size.data_kind() {
            DecimalDataKind::Decimal64 => {
                select_decimal::<i64>(mask, then_value, else_value, data_type)
            }
            DecimalDataKind::Decimal128 => {
                select_decimal::<i128>(mask, then_value, else_value, data_type)
            }
            DecimalDataKind::Decimal256 => {
                select_decimal::<i256>(mask, then_value, else_value, data_type)
            }
        },
        DataType::Date => select_fixed::<DateType>(mask, then_value, else_value, data_type),
        DataType::Timestamp => {
            select_fixed::<TimestampType>(mask, then_value, else_value, data_type)
        }
        DataType::String => select_downcast::<StringType>(mask, then_value, else_value, data_type),
        DataType::Binary => select_downcast::<BinaryType>(mask, then_value, else_value, data_type),
        DataType::Variant => {
            select_downcast::<VariantType>(mask, then_value, else_value, data_type)
        }
        _ => None,
    }
}

/// Splits a value of type `Nullable(_)` into its inner value and validity. A
/// NULL scalar has no inner value.
fn split_nullable(value: &Value<AnyType>) -> Option<(Option<Value<AnyType>>, Value<BooleanType>)> {
    match value {
        Value::Scalar(Scalar::Null) => Some((None, Value::Scalar(false))),
        Value::Scalar(scalar) => Some((Some(Value::Scalar(scalar.clone())), Value::Scalar(true))),
        Value::Column(Column::Nullable(column)) => Some((
            Some(Value::Column(column.column.clone())),
            Value::Column(column.validity.clone()),
        )),
        Value::Column(_) => None,
    }
}

/// `(mask & then) | (!mask & else)`, one 64-bit word at a time.
fn select_bitmap(
    mask: &Bitmap,
    then_value: &Value<BooleanType>,
    else_value: &Value<BooleanType>,
) -> Bitmap {
    match (then_value, else_value) {
        (Value::Scalar(then_bit), Value::Scalar(else_bit)) => match (*then_bit, *else_bit) {
            (true, true) => Bitmap::new_constant(true, mask.len()),
            (false, false) => Bitmap::new_constant(false, mask.len()),
            (true, false) => mask.clone(),
            (false, true) => mask.not(),
        },
        (Value::Column(then_bits), Value::Scalar(true)) => {
            binary(mask, then_bits, |m, t| (m & t) | !m)
        }
        (Value::Column(then_bits), Value::Scalar(false)) => mask & then_bits,
        (Value::Scalar(true), Value::Column(else_bits)) => mask | else_bits,
        (Value::Scalar(false), Value::Column(else_bits)) => binary(mask, else_bits, |m, f| !m & f),
        (Value::Column(then_bits), Value::Column(else_bits)) => {
            ternary(mask, then_bits, else_bits, |m, t, f| (m & t) | (!m & f))
        }
    }
}

/// Fixed-width values whose column is a plain `Buffer<N>`.
fn select_fixed<T>(
    mask: &Bitmap,
    then_value: &Value<AnyType>,
    else_value: &Value<AnyType>,
    data_type: &DataType,
) -> Option<Column>
where
    T: ValueType<Column = Buffer<<T as AccessType>::Scalar>>,
    T::Scalar: Copy,
{
    let then_value = FixedSide::from_value(then_value.try_downcast::<T>().ok()?);
    let else_value = FixedSide::from_value(else_value.try_downcast::<T>().ok()?);
    let values = select_fixed_values(mask, &then_value, &else_value);
    Some(T::upcast_column_with_type(values.into(), data_type))
}

/// Decimal values are read at the physical width of `data_type`. A branch
/// carried with another width (a non-strict representation, e.g.
/// `Decimal(10, 2)` stored as `Decimal128`) is converted first.
fn select_decimal<D: Decimal>(
    mask: &Bitmap,
    then_value: &Value<AnyType>,
    else_value: &Value<AnyType>,
    data_type: &DataType,
) -> Option<Column> {
    let then_value = decimal_side::<D>(then_value)?;
    let else_value = decimal_side::<D>(else_value)?;
    let values = select_fixed_values(mask, &then_value, &else_value);
    Some(DecimalType::<D>::upcast_column_with_type(
        values.into(),
        data_type,
    ))
}

fn decimal_side<D: Decimal>(value: &Value<AnyType>) -> Option<FixedSide<D>> {
    match value {
        Value::Scalar(Scalar::Decimal(scalar)) => Some(FixedSide::Scalar(scalar.as_decimal::<D>())),
        Value::Column(Column::Decimal(column)) => {
            let column = Column::Decimal(column.clone().strict_decimal());
            <D as Decimal>::try_downcast_column(&column)
                .map(|(buffer, _)| FixedSide::Column(buffer))
        }
        _ => None,
    }
}

enum FixedSide<N> {
    Scalar(N),
    Column(Buffer<N>),
}

impl<N: Copy> FixedSide<N> {
    fn from_value<T>(value: Value<T>) -> Self
    where T: AccessType<Scalar = N, Column = Buffer<N>> {
        match value {
            Value::Scalar(scalar) => FixedSide::Scalar(scalar),
            Value::Column(column) => FixedSide::Column(column),
        }
    }
}

fn select_fixed_values<N: Copy>(
    mask: &Bitmap,
    then: &FixedSide<N>,
    else_: &FixedSide<N>,
) -> Vec<N> {
    match (then, else_) {
        (FixedSide::Scalar(t), FixedSide::Scalar(f)) => {
            select_fixed_impl(mask, Repeat(*t), Repeat(*f))
        }
        (FixedSide::Scalar(t), FixedSide::Column(f)) => {
            select_fixed_impl(mask, Repeat(*t), f.as_slice())
        }
        (FixedSide::Column(t), FixedSide::Scalar(f)) => {
            select_fixed_impl(mask, t.as_slice(), Repeat(*f))
        }
        (FixedSide::Column(t), FixedSide::Column(f)) => {
            select_fixed_impl(mask, t.as_slice(), f.as_slice())
        }
    }
}

/// One side of a fixed-width selection: a column, or a scalar repeated for
/// every row.
trait FixedSource<N: Copy>: Copy {
    /// # Safety
    ///
    /// `row` must be less than the mask length.
    unsafe fn get(self, row: usize) -> N;
    fn extend(self, out: &mut Vec<N>, start: usize, len: usize);
}

#[derive(Clone, Copy)]
struct Repeat<N>(N);

impl<N: Copy> FixedSource<N> for Repeat<N> {
    #[inline(always)]
    unsafe fn get(self, _row: usize) -> N {
        self.0
    }

    #[inline]
    fn extend(self, out: &mut Vec<N>, _start: usize, len: usize) {
        out.extend(std::iter::repeat_n(self.0, len));
    }
}

impl<N: Copy> FixedSource<N> for &[N] {
    #[inline(always)]
    unsafe fn get(self, row: usize) -> N {
        // SAFETY: the caller keeps `row` below the mask length, which is the
        // length of this column (checked in `select_if_then_else`).
        unsafe { *self.get_unchecked(row) }
    }

    #[inline]
    fn extend(self, out: &mut Vec<N>, start: usize, len: usize) {
        out.extend_from_slice(&self[start..start + len]);
    }
}

fn select_fixed_impl<N: Copy>(
    mask: &Bitmap,
    then: impl FixedSource<N>,
    else_: impl FixedSource<N>,
) -> Vec<N> {
    let mut out = Vec::with_capacity(mask.len());
    for_each_segment(mask, |segment| match segment {
        Segment::Run {
            take_then,
            start,
            len,
        } => {
            if take_then {
                then.extend(&mut out, start, len);
            } else {
                else_.extend(&mut out, start, len);
            }
        }
        Segment::Mixed { word, start, len } => {
            // Both sides are loaded and one is picked without a branch, which
            // lets the loop compile to vector blends.
            out.extend((0..len).map(|i| {
                // SAFETY: `start + i` is a row of the mask.
                let (t, f) = unsafe { (then.get(start + i), else_.get(start + i)) };
                if (word >> i) & 1 != 0 { t } else { f }
            }));
        }
    });
    out
}

/// Types with a concrete `ValueType`; falls back to `AnyType` when a value
/// cannot be downcast.
fn select_downcast<T: ValueType>(
    mask: &Bitmap,
    then_value: &Value<AnyType>,
    else_value: &Value<AnyType>,
    data_type: &DataType,
) -> Option<Column> {
    let then_value = then_value.try_downcast::<T>().ok()?;
    let else_value = else_value.try_downcast::<T>().ok()?;
    Some(select_values::<T>(
        mask,
        &then_value,
        &else_value,
        data_type,
    ))
}

/// Runs are appended in bulk (`push_repeat` for a scalar, `append_column` of
/// a slice for a column) and the rows of mixed words are pushed one by one.
fn select_values<T: ValueType>(
    mask: &Bitmap,
    then_value: &Value<T>,
    else_value: &Value<T>,
    data_type: &DataType,
) -> Column {
    let mut builder = ColumnBuilder::with_capacity(data_type, mask.len());
    {
        let mut out = T::downcast_builder(&mut builder);
        for_each_segment(mask, |segment| match segment {
            Segment::Run {
                take_then,
                start,
                len,
            } => extend_run::<T>(
                &mut out,
                if take_then { then_value } else { else_value },
                start,
                len,
            ),
            Segment::Mixed { word, start, len } => {
                for i in 0..len {
                    let value = if (word >> i) & 1 != 0 {
                        then_value
                    } else {
                        else_value
                    };
                    // SAFETY: `start + i` is a row of the mask.
                    let item = unsafe { value_item(value, start + i) };
                    T::push_item_mut(&mut out, item);
                }
            }
        });
    }
    builder.build()
}

fn extend_run<T: ValueType>(
    out: &mut T::ColumnBuilderMut<'_>,
    value: &Value<T>,
    start: usize,
    len: usize,
) {
    match value {
        Value::Scalar(scalar) => T::push_item_repeat_mut(out, T::to_scalar_ref(scalar), len),
        Value::Column(column) => {
            T::append_column_mut(out, &T::slice_column(column, start..start + len))
        }
    }
}

/// # Safety
///
/// `row` must be less than the length of `value` when it is a column.
#[inline(always)]
unsafe fn value_item<T: AccessType>(value: &Value<T>, row: usize) -> T::ScalarRef<'_> {
    match value {
        Value::Scalar(scalar) => T::to_scalar_ref(scalar),
        // SAFETY: guaranteed by the caller.
        Value::Column(column) => unsafe { T::index_column_unchecked(column, row) },
    }
}

/// A part of the mask: a run of whole 64-row words that are all set
/// (`take_then`) or all unset, or a single word with both set and unset bits.
enum Segment {
    Run {
        take_then: bool,
        start: usize,
        len: usize,
    },
    Mixed {
        word: u64,
        start: usize,
        len: usize,
    },
}

/// Calls `f` for the segments of `mask` in row order. Bit `i` of a mixed word
/// is the row `start + i`.
fn for_each_segment(mask: &Bitmap, f: impl FnMut(Segment)) {
    let mut segments = SegmentBuilder {
        f,
        start: 0,
        run: None,
    };
    let (bytes, offset, len) = mask.as_slice();
    if offset == 0 {
        segments.push_chunks(BitChunksExact::<u64>::new(bytes, len));
    } else {
        segments.push_chunks(mask.chunks::<u64>());
    }
    segments.flush_run();
}

struct SegmentBuilder<F: FnMut(Segment)> {
    f: F,
    /// The first row of the next word.
    start: usize,
    /// The pending run: `(take_then, start, len)`.
    run: Option<(bool, usize, usize)>,
}

impl<F: FnMut(Segment)> SegmentBuilder<F> {
    fn push_chunks(&mut self, mut chunks: impl BitChunkIterExact<u64>) {
        for word in chunks.by_ref() {
            self.push_word(word, 64);
        }
        let remainder_len = chunks.remainder_len();
        if remainder_len > 0 {
            self.push_word(chunks.remainder(), remainder_len);
        }
    }

    #[inline]
    fn push_word(&mut self, word: u64, len: usize) {
        let full = if len == 64 { u64::MAX } else { (1 << len) - 1 };
        let word = word & full;
        let start = self.start;
        self.start += len;

        let take_then = if word == 0 {
            false
        } else if word == full {
            true
        } else {
            self.flush_run();
            (self.f)(Segment::Mixed { word, start, len });
            return;
        };
        match &mut self.run {
            Some((run_then, _, run_len)) if *run_then == take_then => *run_len += len,
            _ => {
                self.flush_run();
                self.run = Some((take_then, start, len));
            }
        }
    }

    #[inline]
    fn flush_run(&mut self) {
        if let Some((take_then, start, len)) = self.run.take() {
            (self.f)(Segment::Run {
                take_then,
                start,
                len,
            });
        }
    }
}
