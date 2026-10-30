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

use std::borrow::Cow;
use std::collections::HashMap;
use std::iter::TrustedLen;
use std::ops::Range;

use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;

use super::AccessType;
use super::AnyType;
use super::DataType;
use super::DecimalColumn;
use super::DecimalDataType;
use super::DecimalScalar;
use super::NumberColumn;
use super::NumberDataType;
use super::ValueType;
use super::i256;
use super::number::Number;
use crate::BlockEntry;
use crate::Column;
use crate::Domain;
use crate::LikePattern;
use crate::Scalar;
use crate::ScalarRef;
use crate::SelectOp;
use crate::TakeIndex;
use crate::Value;
use crate::with_integer_mapped_type;

/// Unsigned integers stored in the narrowest byte-aligned width that fits their maximum (see
/// [`NarrowBuffer::pack`]). Holds the deltas of a [`ForColumn`] and the keys of a [`DictColumn`].
///
/// "Narrow" means the narrowest width the values need, not necessarily a small one (`U64`
/// holds deltas of 128-bit decimals). Unrelated to Arrow types, and not bit-packed: widths are
/// whole bytes so kernels can index the values directly.
#[derive(Clone, Debug, PartialEq)]
pub enum NarrowBuffer {
    U8(Buffer<u8>),
    U16(Buffer<u16>),
    U32(Buffer<u32>),
    U64(Buffer<u64>),
}

#[macro_export]
macro_rules! with_narrow_buffer {
    ($buffer:expr, | $b:ident | $body:expr) => {
        match $buffer {
            $crate::types::encoded::NarrowBuffer::U8($b) => $body,
            $crate::types::encoded::NarrowBuffer::U16($b) => $body,
            $crate::types::encoded::NarrowBuffer::U32($b) => $body,
            $crate::types::encoded::NarrowBuffer::U64($b) => $body,
        }
    };
}

#[macro_export]
macro_rules! map_narrow_buffer {
    ($buffer:expr, | $b:ident | $body:expr) => {
        match $buffer {
            $crate::types::encoded::NarrowBuffer::U8($b) => {
                $crate::types::encoded::NarrowBuffer::U8($body)
            }
            $crate::types::encoded::NarrowBuffer::U16($b) => {
                $crate::types::encoded::NarrowBuffer::U16($body)
            }
            $crate::types::encoded::NarrowBuffer::U32($b) => {
                $crate::types::encoded::NarrowBuffer::U32($body)
            }
            $crate::types::encoded::NarrowBuffer::U64($b) => {
                $crate::types::encoded::NarrowBuffer::U64($body)
            }
        }
    };
}
/// Widens a delta or key of any [`NarrowBuffer`] width. Generic so that the arms of
/// `with_narrow_buffer!` share the code, the `u64` one included.
#[inline(always)]
fn widen<D: Into<u64>>(value: D) -> u64 {
    value.into()
}

impl NarrowBuffer {
    /// Byte width needed to hold values up to `max`.
    pub fn width_for(max: u64) -> usize {
        if max <= u8::MAX as u64 {
            1
        } else if max <= u16::MAX as u64 {
            2
        } else if max <= u32::MAX as u64 {
            4
        } else {
            8
        }
    }

    /// Packs `values` into the narrowest width that can hold `max`.
    pub fn pack(values: impl ExactSizeIterator<Item = u64>, max: u64) -> Self {
        match Self::width_for(max) {
            1 => NarrowBuffer::U8(values.map(|v| v as u8).collect::<Vec<_>>().into()),
            2 => NarrowBuffer::U16(values.map(|v| v as u16).collect::<Vec<_>>().into()),
            4 => NarrowBuffer::U32(values.map(|v| v as u32).collect::<Vec<_>>().into()),
            _ => NarrowBuffer::U64(values.collect::<Vec<_>>().into()),
        }
    }

    pub fn len(&self) -> usize {
        with_narrow_buffer!(self, |b| b.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn byte_width(&self) -> usize {
        match self {
            NarrowBuffer::U8(_) => 1,
            NarrowBuffer::U16(_) => 2,
            NarrowBuffer::U32(_) => 4,
            NarrowBuffer::U64(_) => 8,
        }
    }

    pub fn memory_size(&self) -> usize {
        self.len() * self.byte_width()
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<u64> {
        with_narrow_buffer!(self, |b| b.get(index).map(|v| widen(*v)))
    }

    /// # Safety
    ///
    /// `index` must be in bounds.
    #[inline]
    pub unsafe fn get_unchecked(&self, index: usize) -> u64 {
        unsafe { with_narrow_buffer!(self, |b| widen(*b.get_unchecked(index))) }
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        map_narrow_buffer!(self, |b| b.clone().sliced(range.start, range.len()))
    }

    pub fn iter(&self) -> NarrowIter<'_> {
        match self {
            NarrowBuffer::U8(b) => NarrowIter::U8(b.iter()),
            NarrowBuffer::U16(b) => NarrowIter::U16(b.iter()),
            NarrowBuffer::U32(b) => NarrowIter::U32(b.iter()),
            NarrowBuffer::U64(b) => NarrowIter::U64(b.iter()),
        }
    }

    pub fn min_max(&self) -> Option<(u64, u64)> {
        with_narrow_buffer!(self, |b| {
            let mut iter = b.iter();
            let first = widen(*iter.next()?);
            Some(iter.fold((first, first), |(min, max), v| {
                let v = widen(*v);
                (min.min(v), max.max(v))
            }))
        })
    }
}

pub enum NarrowIter<'a> {
    U8(std::slice::Iter<'a, u8>),
    U16(std::slice::Iter<'a, u16>),
    U32(std::slice::Iter<'a, u32>),
    U64(std::slice::Iter<'a, u64>),
}

impl Iterator for NarrowIter<'_> {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<u64> {
        match self {
            NarrowIter::U8(it) => it.next().map(|v| *v as u64),
            NarrowIter::U16(it) => it.next().map(|v| *v as u64),
            NarrowIter::U32(it) => it.next().map(|v| *v as u64),
            NarrowIter::U64(it) => it.next().copied(),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            NarrowIter::U8(it) => it.size_hint(),
            NarrowIter::U16(it) => it.size_hint(),
            NarrowIter::U32(it) => it.size_hint(),
            NarrowIter::U64(it) => it.size_hint(),
        }
    }
}

impl ExactSizeIterator for NarrowIter<'_> {}

unsafe impl TrustedLen for NarrowIter<'_> {}

/// Values of a plain column that a [`DictColumn`] can be built from, other than strings and
/// floats (keyed by their bits in [`DictColumn::try_encode`]).
macro_rules! with_hashable_buffer {
    ($column:expr, | $b:ident | $body:expr) => {
        match $column {
            Column::Number(n) => Some(crate::with_number_type!(|NUM| match n {
                NumberColumn::NUM($b) => $body,
            })),
            Column::Date($b) => Some($body),
            Column::Timestamp($b) => Some($body),
            Column::Decimal(DecimalColumn::Decimal64($b, _)) => Some($body),
            Column::Decimal(DecimalColumn::Decimal128($b, _)) => Some($body),
            Column::Decimal(DecimalColumn::Decimal256($b, _)) => Some($body),
            _ => None,
        }
    };
}

/// Dictionary encoded column: `value(i) = values[keys[i]]`.
#[derive(Clone, Debug, PartialEq)]
pub struct DictColumn {
    /// Index of each row into `values`.
    pub keys: NarrowBuffer,
    /// The dictionary, a regular (decoded) column.
    pub values: Column,
}

impl DictColumn {
    pub fn new(keys: NarrowBuffer, values: Column) -> Self {
        debug_assert!(
            keys.min_max()
                .is_none_or(|(_, max)| (max as usize) < values.len())
        );
        DictColumn { keys, values }
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn decode(&self) -> Column {
        with_narrow_buffer!(&self.keys, |b| self.take_values(b.as_slice()))
    }

    /// Returns `values[keys[i]]` for the given keys.
    pub fn take_values<I: TakeIndex + ?Sized>(&self, keys: &I) -> Column {
        BlockEntry::Column(self.values.clone())
            .take(keys)
            .unwrap()
            .to_column()
    }

    pub fn compare_scalar(&self, op: &SelectOp, scalar: &Scalar) -> Option<Bitmap> {
        let scalar = scalar.as_ref();
        if scalar.infer_data_type() != self.values.data_type() {
            return None;
        }
        // One result per dictionary value; rows look them up by key.
        let (mut has_true, mut has_false) = (false, false);
        let matches = self
            .values
            .iter()
            .map(|value| {
                let m = op_matches(op, value.cmp(&scalar));
                has_true |= m;
                has_false |= !m;
                m
            })
            .collect::<Vec<bool>>();
        let len = self.len();
        if !(has_true && has_false) {
            return Some(Bitmap::new_constant(has_true, len));
        }
        Some(with_narrow_buffer!(&self.keys, |b| {
            let keys = b.as_slice();
            // SAFETY: `i < keys.len()`, and keys are in bounds of the values.
            Bitmap::collect_bool(len, |i| unsafe {
                *matches.get_unchecked(*keys.get_unchecked(i) as usize)
            })
        }))
    }

    /// Matches a string pattern once per dictionary value, then maps the results to rows.
    /// `None` for dictionaries whose values are not strings.
    pub fn like(&self, pattern: &LikePattern, not: bool) -> Option<Bitmap> {
        let Column::String(values) = &self.values else {
            return None;
        };
        // Cache by dictionary index: repeated keys must not evaluate LIKE again.
        let dict_matches: Vec<bool> = values
            .iter()
            .map(|value| pattern.compare(value.as_bytes()) ^ not)
            .collect();
        Some(with_narrow_buffer!(&self.keys, |keys| {
            keys.iter().map(|key| dict_matches[*key as usize]).collect()
        }))
    }

    /// Whether dictionary encoding supports columns of `data_type`: non-nullable types whose
    /// values can be hashed.
    pub fn supports(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Number(_)
                | DataType::Date
                | DataType::Timestamp
                | DataType::Decimal(_)
                | DataType::String
        )
    }

    /// Builds the dictionary of a regular column, with the values in first-seen order.
    ///
    /// Gives up (`None`) when the column has more than `max_ndv` distinct values or its type
    /// is not supported.
    pub fn try_encode(column: &Column, max_ndv: usize) -> Option<DictColumn> {
        if column.len() == 0 {
            return None;
        }
        // `first_rows[key]`: the first row holding the value of `key`.
        let (keys, first_rows) = match column {
            Column::String(c) => dict_keys(c.iter(), max_ndv)?,
            // Floats are keyed by their bits: `F32`/`F64` equate `+0.0` and `-0.0` (and all
            // NaNs), but the dictionary must reproduce the exact values, e.g. for
            // `atan2(-0.0, -1.0) = -pi` vs `atan2(+0.0, -1.0) = pi`.
            Column::Number(NumberColumn::Float32(b)) => {
                dict_keys(b.iter().map(|v| v.0.to_bits()), max_ndv)?
            }
            Column::Number(NumberColumn::Float64(b)) => {
                dict_keys(b.iter().map(|v| v.0.to_bits()), max_ndv)?
            }
            column => with_hashable_buffer!(column, |b| dict_keys(b.iter().copied(), max_ndv))??,
        };
        let values = BlockEntry::Column(column.clone())
            .take(first_rows.as_slice())
            .ok()?
            .to_column();
        let max_key = values.len() as u64 - 1;
        let keys = NarrowBuffer::pack(keys.into_iter().map(|key| key as u64), max_key);
        Some(DictColumn::new(keys, values))
    }
}

fn op_matches(op: &SelectOp, ordering: std::cmp::Ordering) -> bool {
    match op {
        SelectOp::Equal => ordering.is_eq(),
        SelectOp::NotEqual => ordering.is_ne(),
        SelectOp::Gt => ordering.is_gt(),
        SelectOp::Gte => ordering.is_ge(),
        SelectOp::Lt => ordering.is_lt(),
        SelectOp::Lte => ordering.is_le(),
    }
}

/// `(key of each row, first row of each key)`, or `None` beyond `max_ndv` distinct values.
fn dict_keys<T: std::hash::Hash + Eq>(
    values: impl ExactSizeIterator<Item = T>,
    max_ndv: usize,
) -> Option<(Vec<u32>, Vec<u32>)> {
    let max_ndv = max_ndv.min(u32::MAX as usize);
    let hasher = ahash::RandomState::with_seeds(0, 1, 2, 3);
    let mut dictionary = HashMap::with_capacity_and_hasher(max_ndv.min(1024), hasher);
    let mut keys = Vec::with_capacity(values.len());
    let mut first_rows = Vec::new();
    for (row, value) in values.enumerate() {
        let next = first_rows.len() as u32;
        let key = *dictionary.entry(value).or_insert(next);
        if key == next {
            if first_rows.len() == max_ndv {
                return None;
            }
            first_rows.push(row as u32);
        }
        keys.push(key);
    }
    Some((keys, first_rows))
}

/// Physical types that frame of reference columns decode to.
///
/// Raw values are the `i128` representation used by [`ForColumn`].
pub trait ForNative: Copy + Send + Sync + 'static {
    /// The raw values representable by the type.
    const RAW_MIN: i128;
    const RAW_MAX: i128;

    /// `raw` must be in `RAW_MIN..=RAW_MAX`.
    fn from_raw(raw: i128) -> Self;
    /// `self` must be representable as a raw value.
    fn to_raw(self) -> i128;
    /// `self + delta`; the caller guarantees the result is representable.
    fn add_delta(self, delta: u64) -> Self;
}

macro_rules! impl_for_native {
    ($($t:ty),*) => {$(
        impl ForNative for $t {
            const RAW_MIN: i128 = <$t>::MIN as i128;
            const RAW_MAX: i128 = <$t>::MAX as i128;

            #[inline(always)]
            fn from_raw(raw: i128) -> Self {
                raw as $t
            }

            #[inline(always)]
            fn to_raw(self) -> i128 {
                self as i128
            }

            #[inline(always)]
            fn add_delta(self, delta: u64) -> Self {
                // Wrapping arithmetic is exact because the result is representable.
                self.wrapping_add(delta as $t)
            }
        }
    )*};
}

impl_for_native!(i8, i16, i32, i64, i128, u8, u16, u32, u64);

impl ForNative for i256 {
    const RAW_MIN: i128 = i128::MIN;
    const RAW_MAX: i128 = i128::MAX;

    #[inline(always)]
    fn from_raw(raw: i128) -> Self {
        i256::from(raw)
    }

    #[inline(always)]
    fn to_raw(self) -> i128 {
        self.as_i128()
    }

    #[inline(always)]
    fn add_delta(self, delta: u64) -> Self {
        self + i256::from(delta)
    }
}

/// Values of a plain column whose type a [`ForColumn`] supports.
macro_rules! with_plain_buffer {
    ($column:expr, | $b:ident | $body:expr) => {
        match $column {
            Column::Number(NumberColumn::Int8($b)) => Some($body),
            Column::Number(NumberColumn::Int16($b)) => Some($body),
            Column::Number(NumberColumn::Int32($b)) => Some($body),
            Column::Number(NumberColumn::Int64($b)) => Some($body),
            Column::Number(NumberColumn::UInt8($b)) => Some($body),
            Column::Number(NumberColumn::UInt16($b)) => Some($body),
            Column::Number(NumberColumn::UInt32($b)) => Some($body),
            Column::Number(NumberColumn::UInt64($b)) => Some($body),
            Column::Date($b) => Some($body),
            Column::Timestamp($b) => Some($body),
            Column::Decimal(DecimalColumn::Decimal64($b, _)) => Some($body),
            Column::Decimal(DecimalColumn::Decimal128($b, _)) => Some($body),
            _ => None,
        }
    };
}

/// Frame of reference encoded column: `value(i) = base + deltas[i]`.
///
/// Holds a non-nullable integer, `Date`, `Timestamp` or `Decimal` column. Values are handled
/// as raw `i128` (the unscaled value for decimals); the logical type decides the physical type
/// of the decoded column, e.g. `Decimal(38, 6)` decodes to `i128` even if every value fits `i64`.
#[derive(Clone, Debug, PartialEq)]
pub struct ForColumn {
    data_type: DataType,
    base: i128,
    /// Upper bound of the deltas: every value is in `base..=base + max_delta`.
    max_delta: u64,
    deltas: NarrowBuffer,
}

impl ForColumn {
    pub fn supports(data_type: &DataType) -> bool {
        match data_type {
            DataType::Number(ty) => ty.is_integer(),
            DataType::Date | DataType::Timestamp | DataType::Decimal(_) => true,
            _ => false,
        }
    }

    pub fn new(data_type: DataType, base: i128, max_delta: u64, deltas: NarrowBuffer) -> Self {
        debug_assert!(Self::supports(&data_type));
        ForColumn {
            data_type,
            base,
            max_delta,
            deltas,
        }
    }

    /// Encodes `column` when it is supported and its deltas are narrower than its values.
    pub fn try_encode(column: &Column) -> Option<ForColumn> {
        let data_type = column.data_type();
        if let DataType::Decimal(size) = &data_type {
            // Decoding picks the physical type from the size, so it must match the column's.
            match (column, DecimalDataType::from(*size)) {
                (Column::Decimal(DecimalColumn::Decimal64(..)), DecimalDataType::Decimal64(_))
                | (
                    Column::Decimal(DecimalColumn::Decimal128(..)),
                    DecimalDataType::Decimal128(_),
                ) => {}
                _ => return None,
            }
        }
        let width = physical_width(&data_type)?;
        let (base, max_delta, deltas) =
            with_plain_buffer!(column, |values| encode_values(values.as_slice(), width))??;
        Some(ForColumn::new(data_type, base, max_delta, deltas))
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn base(&self) -> i128 {
        self.base
    }

    pub fn max_delta(&self) -> u64 {
        self.max_delta
    }

    pub fn deltas(&self) -> &NarrowBuffer {
        &self.deltas
    }

    /// The same encoding over other deltas, e.g. a subset of the rows.
    pub fn with_deltas(&self, deltas: NarrowBuffer) -> Self {
        ForColumn::new(self.data_type.clone(), self.base, self.max_delta, deltas)
    }

    pub fn len(&self) -> usize {
        self.deltas.len()
    }

    pub fn is_empty(&self) -> bool {
        self.deltas.is_empty()
    }

    /// Lower bound of the raw values.
    pub fn raw_min(&self) -> i128 {
        self.base
    }

    /// Upper bound of the raw values.
    pub fn raw_max(&self) -> i128 {
        self.base + self.max_delta as i128
    }

    /// Whether every value is representable as `T`.
    pub fn fits<T: ForNative>(&self) -> bool {
        self.raw_min() >= T::RAW_MIN && self.raw_max() <= T::RAW_MAX
    }

    #[inline]
    pub fn raw_value(&self, index: usize) -> Option<i128> {
        self.deltas.get(index).map(|d| self.base + d as i128)
    }

    /// # Safety
    ///
    /// `index` must be in bounds.
    #[inline]
    pub unsafe fn raw_value_unchecked(&self, index: usize) -> i128 {
        unsafe { self.base + self.deltas.get_unchecked(index) as i128 }
    }

    /// Iterates the values as `T` without decoding, if they all fit `T`.
    pub fn values_as<T: ForNative>(&self) -> Option<ForValues<'_, T>> {
        self.fits::<T>().then(|| ForValues {
            base: T::from_raw(self.base),
            deltas: self.deltas.iter(),
        })
    }

    /// The values as `T`, which must fit them.
    pub fn decode_as<T: ForNative>(&self) -> Buffer<T> {
        debug_assert!(self.fits::<T>());
        let base = T::from_raw(self.base);
        with_narrow_buffer!(&self.deltas, |b| {
            b.iter()
                .map(|d| base.add_delta(widen(*d)))
                .collect::<Vec<_>>()
                .into()
        })
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        self.with_deltas(self.deltas.slice(range))
    }

    /// Materializes the column of the logical type.
    pub fn decode(&self) -> Column {
        match &self.data_type {
            DataType::Number(ty) => with_integer_mapped_type!(|NUM| match ty {
                NumberDataType::NUM => Column::Number(NUM::upcast_column(self.decode_as::<NUM>())),
                _ => unreachable!("frame of reference over {ty:?}"),
            }),
            DataType::Date => Column::Date(self.decode_as::<i32>()),
            DataType::Timestamp => Column::Timestamp(self.decode_as::<i64>()),
            DataType::Decimal(size) => Column::Decimal(match DecimalDataType::from(*size) {
                DecimalDataType::Decimal64(_) => {
                    DecimalColumn::Decimal64(self.decode_as::<i64>(), *size)
                }
                DecimalDataType::Decimal128(_) => {
                    DecimalColumn::Decimal128(self.decode_as::<i128>(), *size)
                }
                DecimalDataType::Decimal256(_) => {
                    DecimalColumn::Decimal256(self.decode_as::<i256>(), *size)
                }
            }),
            ty => unreachable!("frame of reference over {ty}"),
        }
    }

    /// Byte width of a decoded value.
    pub fn decoded_width(&self) -> usize {
        physical_width(&self.data_type).unwrap()
    }

    /// The scalar of the logical type for a raw value.
    pub fn scalar_ref(&self, raw: i128) -> ScalarRef<'static> {
        match &self.data_type {
            DataType::Number(ty) => with_integer_mapped_type!(|NUM| match ty {
                NumberDataType::NUM => ScalarRef::Number(NUM::upcast_scalar(NUM::from_raw(raw))),
                _ => unreachable!("frame of reference over {ty:?}"),
            }),
            DataType::Date => ScalarRef::Date(raw as i32),
            DataType::Timestamp => ScalarRef::Timestamp(raw as i64),
            DataType::Decimal(size) => ScalarRef::Decimal(match DecimalDataType::from(*size) {
                DecimalDataType::Decimal64(_) => DecimalScalar::Decimal64(raw as i64, *size),
                DecimalDataType::Decimal128(_) => DecimalScalar::Decimal128(raw, *size),
                DecimalDataType::Decimal256(_) => {
                    DecimalScalar::Decimal256(i256::from_raw(raw), *size)
                }
            }),
            ty => unreachable!("frame of reference over {ty}"),
        }
    }

    /// Domain of the column, from the bounds of the encoding.
    pub fn domain(&self) -> Domain {
        let bounds = NarrowBuffer::U64(vec![0, self.max_delta].into());
        self.with_deltas(bounds).decode().domain()
    }

    /// The raw value of `scalar` if it has the logical type of the column (for decimals:
    /// the same scale), so it compares with the raw values of the column.
    pub fn raw_scalar(&self, scalar: &Scalar) -> Option<i128> {
        match (&self.data_type, scalar) {
            (DataType::Number(ty), Scalar::Number(n)) if n.data_type() == *ty => {
                n.integer_to_i128()
            }
            (DataType::Date, Scalar::Date(v)) => Some(*v as i128),
            (DataType::Timestamp, Scalar::Timestamp(v)) => Some(*v as i128),
            // A constant of a smaller scale, e.g. `l_quantity >= 1` binds `1` as `Decimal(3, 0)`,
            // is rescaled to the scale of the column, which keeps the comparison exact.
            (DataType::Decimal(size), Scalar::Decimal(d)) if d.scale() <= size.scale() => {
                let raw = match d {
                    DecimalScalar::Decimal64(v, _) => *v as i128,
                    DecimalScalar::Decimal128(v, _) => *v,
                    DecimalScalar::Decimal256(v, _) => {
                        let raw = v.as_i128();
                        (i256::from(raw) == *v).then_some(raw)?
                    }
                };
                // `None` on overflow: the comparison falls back to the decoded values.
                raw.checked_mul(10i128.checked_pow((size.scale() - d.scale()) as u32)?)
            }
            _ => None,
        }
    }

    /// Evaluates `value_a(i) <op> value_b(i)` for every row without decoding. With
    /// `value = base + delta`, `base_a + a <op> base_b + b` is `a <op> b + (base_b - base_a)`:
    /// the deltas compare after shifting one side by the difference of the bases, a constant
    /// of the block. (Comparing the deltas directly would be wrong, as they are relative to
    /// different bases.)
    ///
    /// `None` if the columns do not compare by their raw values (different types or decimal
    /// scales, different lengths) or the shifted deltas do not fit `i64`.
    pub fn compare_for(&self, op: &SelectOp, other: &ForColumn) -> Option<Bitmap> {
        let comparable = match (&self.data_type, &other.data_type) {
            (DataType::Decimal(a), DataType::Decimal(b)) => a.scale() == b.scale(),
            (a, b) => a == b,
        };
        if !comparable || self.len() != other.len() {
            return None;
        }
        let offset = i64::try_from(other.base.checked_sub(self.base)?).ok()?;
        i64::try_from(self.max_delta).ok()?;
        offset.checked_add(i64::try_from(other.max_delta).ok()?)?;
        Some(with_narrow_buffer!(&self.deltas, |a| {
            with_narrow_buffer!(&other.deltas, |b| {
                compare_delta_pairs(a.as_slice(), b.as_slice(), op, offset)
            })
        }))
    }

    /// Evaluates `value(i) <op> raw` for every row on the deltas.
    pub fn compare_raw(&self, op: &SelectOp, raw: i128) -> Bitmap {
        let len = self.len();
        let max = self.max_delta as i128;
        // `value <op> raw` is `delta <op> t`.
        let t = raw
            .checked_sub(self.base)
            .unwrap_or(if raw > 0 { i128::MAX } else { i128::MIN });
        let constant = match op {
            SelectOp::Equal => (t < 0 || t > max).then_some(false),
            SelectOp::NotEqual => (t < 0 || t > max).then_some(true),
            SelectOp::Gt if t < 0 => Some(true),
            SelectOp::Gt if t >= max => Some(false),
            SelectOp::Gte if t <= 0 => Some(true),
            SelectOp::Gte if t > max => Some(false),
            SelectOp::Lt if t <= 0 => Some(false),
            SelectOp::Lt if t > max => Some(true),
            SelectOp::Lte if t < 0 => Some(false),
            SelectOp::Lte if t >= max => Some(true),
            _ => None,
        };
        if let Some(value) = constant {
            return Bitmap::new_constant(value, len);
        }
        with_narrow_buffer!(&self.deltas, |b| compare_deltas(b.as_slice(), op, t as u64))
    }
}

/// Byte width of the physical values of a type a [`ForColumn`] supports.
fn physical_width(data_type: &DataType) -> Option<usize> {
    match data_type {
        DataType::Number(ty) if ty.is_integer() => Some(ty.bit_width() as usize / 8),
        DataType::Date => Some(4),
        DataType::Timestamp => Some(8),
        DataType::Decimal(size) => Some(match DecimalDataType::from(*size) {
            DecimalDataType::Decimal64(_) => 8,
            DecimalDataType::Decimal128(_) => 16,
            DecimalDataType::Decimal256(_) => 32,
        }),
        _ => None,
    }
}

/// `(base, max_delta, deltas)` of `values`, if the deltas are narrower than `width` bytes.
fn encode_values<T: ForNative + PartialOrd>(
    values: &[T],
    width: usize,
) -> Option<(i128, u64, NarrowBuffer)> {
    let (&first, rest) = values.split_first()?;
    let (min, max) = rest.iter().fold((first, first), |(min, max), &v| {
        (if v < min { v } else { min }, if v > max { v } else { max })
    });
    let range = u64::try_from(max.to_raw().checked_sub(min.to_raw())?).ok()?;
    if NarrowBuffer::width_for(range) >= width {
        return None;
    }
    let base = min.to_raw();
    let deltas = NarrowBuffer::pack(values.iter().map(|v| (v.to_raw() - base) as u64), range);
    Some((base, range, deltas))
}

/// Unsigned delta widths of a [`NarrowBuffer`], as `i64` (deltas of `compare_for` fit it).
trait DeltaInt: Copy {
    fn to_i64(self) -> i64;
}

macro_rules! impl_delta_int {
    ($($t:ty),*) => {$(
        impl DeltaInt for $t {
            #[inline(always)]
            fn to_i64(self) -> i64 {
                self as i64
            }
        }
    )*};
}

impl_delta_int!(u8, u16, u32, u64);

/// `a[i] <op> b[i] + offset` for every row.
fn compare_delta_pairs<A: DeltaInt, B: DeltaInt>(
    a: &[A],
    b: &[B],
    op: &SelectOp,
    offset: i64,
) -> Bitmap {
    // SAFETY: `i < a.len() == b.len()`.
    let pair = |i: usize| unsafe {
        (
            a.get_unchecked(i).to_i64(),
            b.get_unchecked(i).to_i64() + offset,
        )
    };
    let len = a.len();
    match op {
        SelectOp::Equal => Bitmap::collect_bool(len, |i| matches!(pair(i), (a, b) if a == b)),
        SelectOp::NotEqual => Bitmap::collect_bool(len, |i| matches!(pair(i), (a, b) if a != b)),
        SelectOp::Gt => Bitmap::collect_bool(len, |i| matches!(pair(i), (a, b) if a > b)),
        SelectOp::Gte => Bitmap::collect_bool(len, |i| matches!(pair(i), (a, b) if a >= b)),
        SelectOp::Lt => Bitmap::collect_bool(len, |i| matches!(pair(i), (a, b) if a < b)),
        SelectOp::Lte => Bitmap::collect_bool(len, |i| matches!(pair(i), (a, b) if a <= b)),
    }
}

fn compare_deltas<D>(deltas: &[D], op: &SelectOp, t: u64) -> Bitmap
where D: Copy + PartialOrd + TryFrom<u64> {
    let Ok(t) = D::try_from(t) else {
        unreachable!("compared delta must fit the delta width")
    };
    // SAFETY: `i < deltas.len()`.
    let delta = |i: usize| unsafe { *deltas.get_unchecked(i) };
    let len = deltas.len();
    match op {
        SelectOp::Equal => Bitmap::collect_bool(len, |i| delta(i) == t),
        SelectOp::NotEqual => Bitmap::collect_bool(len, |i| delta(i) != t),
        SelectOp::Gt => Bitmap::collect_bool(len, |i| delta(i) > t),
        SelectOp::Gte => Bitmap::collect_bool(len, |i| delta(i) >= t),
        SelectOp::Lt => Bitmap::collect_bool(len, |i| delta(i) < t),
        SelectOp::Lte => Bitmap::collect_bool(len, |i| delta(i) <= t),
    }
}

/// Values of a [`ForColumn`] as `T`, see [`ForColumn::values_as`].
pub struct ForValues<'a, T> {
    base: T,
    deltas: NarrowIter<'a>,
}

impl<T: ForNative> Iterator for ForValues<'_, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        self.deltas.next().map(|d| self.base.add_delta(d))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.deltas.size_hint()
    }
}

impl<T: ForNative> ExactSizeIterator for ForValues<'_, T> {}

unsafe impl<T: ForNative> TrustedLen for ForValues<'_, T> {}

/// A column kept in a compact encoded form (`Column::Encoded`); its logical type is the type of
/// the decoded column.
#[derive(Clone, Debug, PartialEq)]
pub enum EncodedColumn {
    Dict(DictColumn),
    For(ForColumn),
}

impl EncodedColumn {
    pub fn len(&self) -> usize {
        match self {
            EncodedColumn::Dict(c) => c.len(),
            EncodedColumn::For(c) => c.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The logical type, i.e. the type of the decoded column.
    pub fn data_type(&self) -> DataType {
        match self {
            EncodedColumn::Dict(c) => c.values.data_type(),
            EncodedColumn::For(c) => c.data_type().clone(),
        }
    }

    /// Materializes the regular column.
    pub fn decode(&self) -> Column {
        match self {
            EncodedColumn::Dict(c) => c.decode(),
            EncodedColumn::For(c) => c.decode(),
        }
    }

    pub fn index(&self, index: usize) -> Option<ScalarRef<'_>> {
        match self {
            EncodedColumn::Dict(c) => {
                let key = c.keys.get(index)?;
                c.values.index(key as usize)
            }
            EncodedColumn::For(c) => Some(c.scalar_ref(c.raw_value(index)?)),
        }
    }

    /// # Safety
    ///
    /// `index` must be in bounds.
    pub unsafe fn index_unchecked(&self, index: usize) -> ScalarRef<'_> {
        unsafe {
            match self {
                EncodedColumn::Dict(c) => {
                    let key = c.keys.get_unchecked(index);
                    c.values.index_unchecked(key as usize)
                }
                EncodedColumn::For(c) => c.scalar_ref(c.raw_value_unchecked(index)),
            }
        }
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        match self {
            EncodedColumn::Dict(c) => EncodedColumn::Dict(DictColumn {
                keys: c.keys.slice(range),
                values: c.values.clone(),
            }),
            EncodedColumn::For(c) => EncodedColumn::For(c.slice(range)),
        }
    }

    pub fn memory_size(&self, gc: bool) -> usize {
        match self {
            EncodedColumn::Dict(c) => c.keys.memory_size() + c.values.memory_size(gc),
            EncodedColumn::For(c) => c.deltas().memory_size(),
        }
    }

    pub fn domain(&self) -> Domain {
        match self {
            // The dictionary may contain unreferenced entries, so its domain is a
            // (valid) superset of the column's domain.
            EncodedColumn::Dict(c) => c.values.domain(),
            EncodedColumn::For(c) => c.domain(),
        }
    }

    /// Calls `f` with a regular column and the row in it that holds the value of `row`.
    ///
    /// A FOR row is decoded into a column of its own on every call, so consumers of many rows
    /// (e.g. output formats) decode the column once with [`Column::decode_encoded`] instead.
    pub fn with_row<R>(&self, row: usize, f: impl FnOnce(&Column, usize) -> R) -> R {
        match self {
            EncodedColumn::Dict(c) => f(&c.values, c.keys.get(row).unwrap() as usize),
            EncodedColumn::For(c) => f(&c.slice(row..row + 1).decode(), 0),
        }
    }

    /// Replaces the per-row codes (FOR deltas / dictionary keys) with `f(codes)` and keeps the
    /// rest of the encoding, e.g. to filter or take rows.
    ///
    /// `f` may only select or reorder codes, so they stay within the bounds of the encoding.
    pub fn map_codes(self, f: impl FnOnce(NarrowBuffer) -> NarrowBuffer) -> Self {
        match self {
            EncodedColumn::For(mut c) => {
                c.deltas = f(c.deltas);
                EncodedColumn::For(c)
            }
            EncodedColumn::Dict(mut c) => {
                c.keys = f(c.keys);
                EncodedColumn::Dict(c)
            }
        }
    }

    /// Evaluates `value(i) <op> scalar` for every row without decoding. `None` if `scalar`
    /// cannot be compared with the encoded values directly.
    pub fn compare_scalar(&self, op: &SelectOp, scalar: &Scalar) -> Option<Bitmap> {
        match self {
            EncodedColumn::For(c) => Some(c.compare_raw(op, c.raw_scalar(scalar)?)),
            EncodedColumn::Dict(c) => c.compare_scalar(op, scalar),
        }
    }

    pub fn as_for(&self) -> Option<&ForColumn> {
        match self {
            EncodedColumn::For(c) => Some(c),
            _ => None,
        }
    }

    /// The code of each row (dictionary keys / frame of reference deltas) and the number of
    /// possible codes. Rows with the same code have the same value.
    pub fn codes(&self) -> (&NarrowBuffer, u64) {
        match self {
            EncodedColumn::For(c) => (c.deltas(), c.max_delta().saturating_add(1)),
            EncodedColumn::Dict(c) => (&c.keys, c.values.len() as u64),
        }
    }

    pub fn as_dict(&self) -> Option<&DictColumn> {
        match self {
            EncodedColumn::Dict(c) => Some(c),
            _ => None,
        }
    }
}

/// An encoded column read as `T` without decoding it, see [`crate::ColumnView::Encoded`] and
/// [`AccessType::try_downcast_encoded`].
#[derive(Debug, Clone)]
pub enum EncodedView<T: AccessType> {
    /// Row `i` is `T::for_scalar_ref(raw value i)`.
    For(ForColumn),
    /// Row `i` is `values[keys[i]]`.
    Dict {
        keys: NarrowBuffer,
        values: T::Column,
    },
}

impl<T: AccessType> EncodedView<T> {
    /// Reads a dictionary encoded column whose dictionary downcasts to `T`.
    pub fn try_dict(column: &EncodedColumn) -> Option<Self> {
        let dict = column.as_dict()?;
        Some(EncodedView::Dict {
            keys: dict.keys.clone(),
            values: T::try_downcast_column(&dict.values).ok()?,
        })
    }

    pub fn len(&self) -> usize {
        match self {
            EncodedView::For(c) => c.len(),
            EncodedView::Dict { keys, .. } => keys.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn index(&self, index: usize) -> Option<T::ScalarRef<'_>> {
        match self {
            EncodedView::For(c) => c.raw_value(index).map(T::for_scalar_ref),
            EncodedView::Dict { keys, values } => {
                T::index_column(values, keys.get(index)? as usize)
            }
        }
    }

    /// # Safety
    ///
    /// `index` must be in bounds.
    pub unsafe fn index_unchecked(&self, index: usize) -> T::ScalarRef<'_> {
        unsafe {
            match self {
                EncodedView::For(c) => T::for_scalar_ref(c.raw_value_unchecked(index)),
                EncodedView::Dict { keys, values } => {
                    T::index_column_unchecked(values, keys.get_unchecked(index) as usize)
                }
            }
        }
    }

    pub fn iter(&self) -> EncodedViewIter<'_, T> {
        match self {
            EncodedView::For(c) => EncodedViewIter::For {
                base: c.base(),
                deltas: c.deltas().iter(),
            },
            EncodedView::Dict { keys, values } => EncodedViewIter::Dict {
                keys: keys.iter(),
                values,
            },
        }
    }

    /// The encoded column of `data_type`, see [`crate::ColumnView::upcast`].
    pub fn upcast(self, data_type: &DataType) -> Column
    where T: ValueType {
        let encoded = match self {
            EncodedView::For(c) => EncodedColumn::For(c),
            EncodedView::Dict { keys, values } => EncodedColumn::Dict(DictColumn::new(
                keys,
                T::upcast_column_with_type(values, data_type),
            )),
        };
        Column::Encoded(Box::new(encoded))
    }
}

pub enum EncodedViewIter<'a, T: AccessType> {
    For {
        base: i128,
        deltas: NarrowIter<'a>,
    },
    Dict {
        keys: NarrowIter<'a>,
        values: &'a T::Column,
    },
}

impl<'a, T: AccessType> Iterator for EncodedViewIter<'a, T> {
    type Item = T::ScalarRef<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            EncodedViewIter::For { base, deltas } => {
                let delta = deltas.next()?;
                Some(T::for_scalar_ref(*base + delta as i128))
            }
            EncodedViewIter::Dict { keys, values } => {
                let key = keys.next()?;
                // SAFETY: the keys of a dictionary are in bounds of its values.
                Some(unsafe { T::index_column_unchecked(values, key as usize) })
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            EncodedViewIter::For { deltas, .. } => deltas.size_hint(),
            EncodedViewIter::Dict { keys, .. } => keys.size_hint(),
        }
    }
}

impl<T: AccessType> ExactSizeIterator for EncodedViewIter<'_, T> {}

impl Column {
    /// Decodes an encoded column, also inside `Nullable`; other columns are returned unchanged.
    ///
    /// Row-at-a-time consumers (e.g. output formats) decode once per block instead of per row.
    pub fn decode_encoded(self) -> Self {
        match self {
            Column::Encoded(c) => c.decode(),
            Column::Nullable(column) if column.column().is_encoded() => {
                let (column, validity) = column.destructure();
                column.decode_encoded().wrap_nullable(Some(validity))
            }
            other => other,
        }
    }
}

impl Value<AnyType> {
    /// Decodes an encoded column value; other values are returned unchanged.
    pub fn decode_encoded(self) -> Self {
        match self {
            Value::Column(column) => Value::Column(column.decode_encoded()),
            other => other,
        }
    }

    pub fn decoded(&self) -> Cow<'_, Self> {
        match self {
            Value::Column(Column::Encoded(encoded)) => Cow::Owned(Value::Column(encoded.decode())),
            Value::Column(Column::Nullable(column)) => {
                if let Column::Encoded(encoded) = column.column() {
                    Cow::Owned(Value::Column(
                        encoded
                            .decode()
                            .wrap_nullable(Some(column.validity().clone())),
                    ))
                } else {
                    Cow::Borrowed(self)
                }
            }
            _ => Cow::Borrowed(self),
        }
    }

    pub fn is_encoded(&self) -> bool {
        match self {
            Value::Column(Column::Encoded(_)) => true,
            Value::Column(Column::Nullable(c)) => c.column().is_encoded(),
            _ => false,
        }
    }
}

impl BlockEntry {
    pub fn decoded_value(&self) -> Value<AnyType> {
        match self {
            BlockEntry::Column(Column::Encoded(encoded)) => Value::Column(encoded.decode()),
            BlockEntry::Column(Column::Nullable(column)) => {
                if let Column::Encoded(encoded) = column.column() {
                    Value::Column(
                        encoded
                            .decode()
                            .wrap_nullable(Some(column.validity().clone())),
                    )
                } else {
                    self.value()
                }
            }
            _ => self.value(),
        }
    }
}

pub fn encoded_error(col: &EncodedColumn) -> ErrorCode {
    ErrorCode::Internal(format!(
        "unexpected encoded column of type {}",
        col.data_type()
    ))
}
