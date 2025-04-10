// Copyright 2020-2022 Jorge C. Leitão
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

use std::iter::FromIterator;
use std::iter::TrustedLen;
use std::ops::Deref;
use std::sync::Arc;

use arrow_data::ArrayData;
use arrow_data::ArrayDataBuilder;
use either::Either;

use super::chunk_iter_to_vec;
use super::utils::count_zeros;
use super::utils::fmt;
use super::utils::get_bit;
use super::utils::get_bit_unchecked;
use super::utils::BitChunk;
use super::utils::BitChunks;
use super::utils::BitmapIter;
use super::utils::ZipValidity;
use super::IntoIter;
use super::MutableBitmap;
use crate::buffer::Bytes;
use crate::error::Error;

/// An immutable container semantically equivalent to `Arc<Vec<bool>>` but represented as `Arc<Vec<u8>>` where
/// each boolean is represented as a single bit.
///
/// # Examples
/// ```
/// use crate::bitmap::Bitmap;
/// use crate::bitmap::MutableBitmap;
///
/// let bitmap = Bitmap::from([true, false, true]);
/// assert_eq!(bitmap.iter().collect::<Vec<_>>(), vec![true, false, true]);
///
/// // creation directly from bytes
/// let bitmap = Bitmap::try_new(vec![0b00001101], 5).unwrap();
/// // note: the first bit is the left-most of the first byte
/// assert_eq!(bitmap.iter().collect::<Vec<_>>(), vec![
///     true, false, true, true, false
/// ]);
/// // we can also get the slice:
/// assert_eq!(bitmap.as_slice(), ([0b00001101u8].as_ref(), 0, 5));
/// // debug helps :)
/// assert_eq!(format!("{:?}", bitmap), "[0b___01101]".to_string());
///
/// // it supports copy-on-write semantics (to a `MutableBitmap`)
/// let bitmap: MutableBitmap = bitmap.into_mut().right().unwrap();
/// assert_eq!(
///     bitmap,
///     MutableBitmap::from([true, false, true, true, false])
/// );
///
/// // slicing is 'O(1)' (data is shared)
/// let bitmap = Bitmap::try_new(vec![0b00001101], 5).unwrap();
/// let mut sliced = bitmap.clone();
/// sliced.slice(1, 4);
/// assert_eq!(sliced.as_slice(), ([0b00001101u8].as_ref(), 1, 4)); // 1 here is the offset:
/// assert_eq!(format!("{:?}", sliced), "[0b___0110_]".to_string());
/// // when sliced (or cloned), it is no longer possible to `into_mut`.
/// let same: Bitmap = sliced.into_mut().left().unwrap();
/// ```
#[derive(Clone)]
pub struct Bitmap {
    bytes: Arc<Bytes<u8>>,
    // both are measured in bits. They are used to bound the bitmap to a region of Bytes.
    offset: usize,
    length: usize,
    // this is a cache: it is computed on initialization
    unset_bits: usize,
}

impl std::fmt::Debug for Bitmap {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let (bytes, offset, len) = self.as_slice();
        fmt(bytes, offset, len, f)
    }
}

impl Default for Bitmap {
    fn default() -> Self {
        MutableBitmap::new().into()
    }
}

pub(super) fn check(bytes: &[u8], offset: usize, length: usize) -> Result<(), Error> {
    if offset + length > bytes.len().saturating_mul(8) {
        return Err(Error::InvalidArgumentError(format!(
            "The offset + length of the bitmap ({}) must be `<=` to the number of bytes times 8 ({})",
            offset + length,
            bytes.len().saturating_mul(8)
        )));
    }
    Ok(())
}

impl Bitmap {
    /// Initializes an empty [`Bitmap`].
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Initializes a new [`Bitmap`] from vector of bytes and a length.
    /// # Errors
    /// This function errors iff `length > bytes.len() * 8`
    #[inline]
    pub fn try_new(bytes: Vec<u8>, length: usize) -> Result<Self, Error> {
        check(&bytes, 0, length)?;
        let unset_bits = count_zeros(&bytes, 0, length);
        Ok(Self {
            length,
            offset: 0,
            bytes: Arc::new(bytes.into()),
            unset_bits,
        })
    }

    /// Returns the length of the [`Bitmap`].
    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns whether [`Bitmap`] is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a new iterator of `bool` over this bitmap
    pub fn iter(&self) -> BitmapIter {
        BitmapIter::new(&self.bytes, self.offset, self.length)
    }

    pub fn option_iter<'a>(
        &'a self,
        validity: Option<&'a Bitmap>,
    ) -> ZipValidity<bool, BitmapIter<'a>, BitmapIter<'a>> {
        let bitmap_iter = validity.as_ref().map(|v| v.iter());
        ZipValidity::new(self.iter(), bitmap_iter)
    }

    /// Returns an iterator over bits in bit chunks [`BitChunk`].
    ///
    /// This iterator is useful to operate over multiple bits via e.g. bitwise.
    pub fn chunks<T: BitChunk>(&self) -> BitChunks<T> {
        BitChunks::new(&self.bytes, self.offset, self.length)
    }

    /// Returns the byte slice of this [`Bitmap`].
    ///
    /// The returned tuple contains:
    /// * `.1`: The byte slice, truncated to the start of the first bit. So the start of the slice
    ///   is within the first 8 bits.
    /// * `.2`: The start offset in bits on a range `0 <= offsets < 8`.
    /// * `.3`: The length in number of bits.
    #[inline]
    pub fn as_slice(&self) -> (&[u8], usize, usize) {
        let start = self.offset / 8;
        let len = (self.offset % 8 + self.length).saturating_add(7) / 8;
        (
            &self.bytes[start..start + len],
            self.offset % 8,
            self.length,
        )
    }

    /// Returns the number of unset bits on this [`Bitmap`].
    ///
    /// Guaranteed to be `<= self.len()`.
    /// # Implementation
    /// This function is `O(1)` - the number of unset bits is computed when the bitmap is
    /// created
    pub const fn null_count(&self) -> usize {
        self.unset_bits
    }

    pub fn true_count(&self) -> usize {
        self.len() - self.unset_bits
    }

    /// Slices `self`, offsetting by `offset` and truncating up to `length` bits.
    /// # Panics
    /// Panic iff `offset + length > self.length`, i.e. if the offset and `length`
    /// exceeds the allocated capacity of `self`.
    #[inline]
    pub fn slice(&mut self, offset: usize, length: usize) {
        assert!(offset + length <= self.length);
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Slices `self`, offsetting by `offset` and truncating up to `length` bits.
    /// # Safety
    /// The caller must ensure that `self.offset + offset + length <= self.len()`
    #[inline]
    pub unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        // we don't do a bitcount in the following cases:
        // 1. if there isn't any data sliced.
        // 2. if this [`Bitmap`] is all true or all false.
        if !(offset == 0 && length == self.length || self.unset_bits == 0) {
            // if `self.unset_bits == self.length` is false, we count the smallest chunk
            // and do a bitcount.
            if self.unset_bits == self.length {
                self.unset_bits = length;
            } else if length < self.length / 2 {
                // count the null values in the slice
                self.unset_bits = count_zeros(&self.bytes, self.offset + offset, length);
            } else {
                // subtract the null count of the chunks we slice off
                let start_end = self.offset + offset + length;
                let head_count = count_zeros(&self.bytes, self.offset, offset);
                let tail_count = count_zeros(&self.bytes, start_end, self.length - length - offset);
                self.unset_bits -= head_count + tail_count;
            }
        }
        self.offset += offset;
        self.length = length;
    }

    /// Slices `self`, offsetting by `offset` and truncating up to `length` bits.
    /// # Panics
    /// Panic iff `offset + length > self.length`, i.e. if the offset and `length`
    /// exceeds the allocated capacity of `self`.
    #[inline]
    #[must_use]
    pub fn sliced(self, offset: usize, length: usize) -> Self {
        assert!(offset + length <= self.length);
        unsafe { self.sliced_unchecked(offset, length) }
    }

    /// Slices `self`, offsetting by `offset` and truncating up to `length` bits.
    /// # Safety
    /// The caller must ensure that `self.offset + offset + length <= self.len()`
    #[inline]
    #[must_use]
    pub unsafe fn sliced_unchecked(mut self, offset: usize, length: usize) -> Self {
        self.slice_unchecked(offset, length);
        self
    }

    /// Returns whether the bit at position `i` is set.
    /// # Panics
    /// Panics iff `i >= self.len()`.
    #[inline]
    pub fn get_bit(&self, i: usize) -> bool {
        get_bit(&self.bytes, self.offset + i)
    }

    /// Unsafely returns whether the bit at position `i` is set.
    /// # Safety
    /// Unsound iff `i >= self.len()`.
    #[inline]
    pub unsafe fn get_bit_unchecked(&self, i: usize) -> bool {
        get_bit_unchecked(&self.bytes, self.offset + i)
    }

    /// Returns a pointer to the start of this [`Bitmap`] (ignores `offsets`)
    /// This pointer is allocated iff `self.len() > 0`.
    pub(crate) fn as_ptr(&self) -> *const u8 {
        self.bytes.deref().as_ptr()
    }

    /// Returns a pointer to the start of this [`Bitmap`] (ignores `offsets`)
    /// This pointer is allocated iff `self.len() > 0`.
    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn values(&self) -> &[u8] {
        self.bytes.deref()
    }

    /// Converts this [`Bitmap`] to [`MutableBitmap`], returning itself if the conversion
    /// is not possible
    ///
    /// This operation returns a [`MutableBitmap`] iff:
    /// * this [`Bitmap`] is not an offsetted slice of another [`Bitmap`]
    /// * this [`Bitmap`] has not been cloned (i.e. [`Arc`]`::get_mut` yields [`Some`])
    /// * this [`Bitmap`] was not imported from the c data interface (FFI)
    pub fn into_mut(mut self) -> Either<Self, MutableBitmap> {
        match (
            self.offset,
            Arc::get_mut(&mut self.bytes).and_then(|b| b.get_vec()),
        ) {
            (0, Some(v)) => {
                let data = std::mem::take(v);
                Either::Right(MutableBitmap::from_vec(data, self.length))
            }
            _ => Either::Left(self),
        }
    }

    /// Converts this [`Bitmap`] into a [`MutableBitmap`], cloning its internal
    /// buffer if required (clone-on-write).
    pub fn make_mut(self) -> MutableBitmap {
        match self.into_mut() {
            Either::Left(data) => {
                if data.offset > 0 {
                    // re-align the bits (remove the offset)
                    let chunks = data.chunks::<u64>();
                    let remainder = chunks.remainder();
                    let vec = chunk_iter_to_vec(chunks.chain(std::iter::once(remainder)));
                    MutableBitmap::from_vec(vec, data.length)
                } else {
                    MutableBitmap::from_vec(data.bytes.as_ref().to_vec(), data.length)
                }
            }
            Either::Right(data) => data,
        }
    }

    /// Initializes an new [`Bitmap`] filled with set/unset values.
    #[inline]
    pub fn new_constant(value: bool, length: usize) -> Self {
        match value {
            true => Self::new_trued(length),
            false => Self::new_zeroed(length),
        }
    }

    /// Initializes an new [`Bitmap`] filled with unset values.
    #[inline]
    pub fn new_zeroed(length: usize) -> Self {
        // don't use `MutableBitmap::from_len_zeroed().into()`
        // it triggers a bitcount
        let bytes = vec![0; length.saturating_add(7) / 8];
        unsafe { Bitmap::from_inner_unchecked(Arc::new(bytes.into()), 0, length, length) }
    }

    /// Initializes an new [`Bitmap`] filled with set values.
    #[inline]
    pub fn new_trued(length: usize) -> Self {
        // just set each byte to u8::MAX
        // we will not access data with index >= length
        let bytes = vec![0b11111111u8; length.saturating_add(7) / 8];
        unsafe { Bitmap::from_inner_unchecked(Arc::new(bytes.into()), 0, length, 0) }
    }

    /// Counts the nulls (unset bits) starting from `offset` bits and for `length` bits.
    #[inline]
    pub fn null_count_range(&self, offset: usize, length: usize) -> usize {
        count_zeros(&self.bytes, self.offset + offset, length)
    }

    /// Creates a new [`Bitmap`] from a slice and length.
    /// # Panics
    /// Panic iff `length > slice.as_ref().len() * 8`
    #[inline]
    pub fn from_u8_slice<T: AsRef<[u8]>>(slice: T, length: usize) -> Self {
        Bitmap::try_new(slice.as_ref().to_vec(), length).unwrap()
    }

    /// Alias for `Bitmap::try_new().unwrap()`
    /// This function is `O(1)`
    /// # Panics
    /// This function panics iff `length > vec.len() * 8`
    #[inline]
    pub fn from_u8_vec(vec: Vec<u8>, length: usize) -> Self {
        Bitmap::try_new(vec, length).unwrap()
    }

    /// Returns whether the bit at position `i` is set.
    #[inline]
    pub fn get(&self, i: usize) -> Option<bool> {
        if i < self.len() {
            Some(unsafe { self.get_bit_unchecked(i) })
        } else {
            None
        }
    }

    /// Returns its internal representation
    #[must_use]
    pub fn into_inner(self) -> (Arc<Bytes<u8>>, usize, usize, usize) {
        let Self {
            bytes,
            offset,
            length,
            unset_bits,
        } = self;
        (bytes, offset, length, unset_bits)
    }

    /// Creates a `[Bitmap]` from its internal representation.
    /// This is the inverted from `[Bitmap::into_inner]`
    ///
    /// # Safety
    /// The invariants of this struct must be upheld
    pub unsafe fn from_inner(
        bytes: Arc<Bytes<u8>>,
        offset: usize,
        length: usize,
        unset_bits: usize,
    ) -> Result<Self, Error> {
        check(&bytes, offset, length)?;
        Ok(Self {
            bytes,
            offset,
            length,
            unset_bits,
        })
    }

    /// Creates a `[Bitmap]` from its internal representation.
    /// This is the inverted from `[Bitmap::into_inner]`
    ///
    /// # Safety
    /// Callers must ensure all invariants of this struct are upheld.
    pub unsafe fn from_inner_unchecked(
        bytes: Arc<Bytes<u8>>,
        offset: usize,
        length: usize,
        unset_bits: usize,
    ) -> Self {
        Self {
            bytes,
            offset,
            length,
            unset_bits,
        }
    }
}

impl<P: AsRef<[bool]>> From<P> for Bitmap {
    fn from(slice: P) -> Self {
        Self::from_trusted_len_iter(slice.as_ref().iter().copied())
    }
}

impl FromIterator<bool> for Bitmap {
    fn from_iter<I>(iter: I) -> Self
    where I: IntoIterator<Item = bool> {
        MutableBitmap::from_iter(iter).into()
    }
}

impl Bitmap {
    /// Creates a new [`Bitmap`] from an iterator of booleans.
    /// # Safety
    /// The iterator must report an accurate length.
    #[inline]
    pub unsafe fn from_trusted_len_iter_unchecked<I: Iterator<Item = bool>>(iterator: I) -> Self {
        MutableBitmap::from_trusted_len_iter_unchecked(iterator).into()
    }

    /// Creates a new [`Bitmap`] from an iterator of booleans.
    #[inline]
    pub fn from_trusted_len_iter<I: TrustedLen<Item = bool>>(iterator: I) -> Self {
        MutableBitmap::from_trusted_len_iter(iterator).into()
    }

    /// Invokes `f` with values `0..len` collecting the boolean results into a new `MutableBuffer`
    ///
    /// This is similar to `from_trusted_len_iter`, however, can be significantly faster
    /// as it eliminates the conditional `Iterator::next`
    #[inline]
    pub fn collect_bool<F: FnMut(usize) -> bool>(len: usize, f: F) -> Self {
        MutableBitmap::collect_bool(len, f).into()
    }

    /// Create a new [`Bitmap`] from an arrow [`NullBuffer`]
    ///
    /// [`NullBuffer`]: arrow_buffer::buffer::NullBuffer
    pub fn from_null_buffer(value: arrow_buffer::buffer::NullBuffer) -> Self {
        let offset = value.offset();
        let length = value.len();
        let unset_bits = value.null_count();
        Self {
            offset,
            length,
            unset_bits,
            bytes: Arc::new(crate::buffer::to_bytes(value.buffer().clone())),
        }
    }

    pub fn into_array_data(&self) -> ArrayData {
        ArrayData::from(self)
    }

    pub fn map_all_sets_to_none(v: Option<Bitmap>) -> Option<Bitmap> {
        match v {
            Some(v) if v.unset_bits == 0 => None,
            None => None,
            other => other,
        }
    }
}

impl<'a> IntoIterator for &'a Bitmap {
    type Item = bool;
    type IntoIter = BitmapIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        BitmapIter::<'a>::new(&self.bytes, self.offset, self.length)
    }
}

impl IntoIterator for Bitmap {
    type Item = bool;
    type IntoIter = IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter::new(self)
    }
}

impl From<Bitmap> for arrow_buffer::buffer::NullBuffer {
    fn from(value: Bitmap) -> Self {
        let null_count = value.unset_bits;
        let buffer = crate::buffer::to_buffer(value.bytes);
        let buffer = arrow_buffer::buffer::BooleanBuffer::new(buffer, value.offset, value.length);
        // Safety: null count is accurate
        unsafe { arrow_buffer::buffer::NullBuffer::new_unchecked(buffer, null_count) }
    }
}

impl From<&Bitmap> for ArrayData {
    fn from(value: &Bitmap) -> Self {
        let buffer = arrow_buffer::buffer::NullBuffer::from(value.clone());
        let builder = ArrayDataBuilder::new(arrow_schema::DataType::Boolean)
            .len(buffer.len())
            .offset(buffer.offset())
            .buffers(vec![buffer.into_inner().into_inner()]);

        // Safety: Array is valid
        unsafe { builder.build_unchecked() }
    }
}

impl From<Bitmap> for ArrayData {
    fn from(value: Bitmap) -> Self {
        ArrayData::from(&value)
    }
}

impl Bitmap {
    pub fn from_array_data(data: ArrayData) -> Self {
        assert_eq!(data.data_type(), &arrow_schema::DataType::Boolean);

        let buffers = data.buffers();
        let buffer =
            arrow_buffer::BooleanBuffer::new(buffers[0].clone(), data.offset(), data.len());
        // Use NullBuffer to compute set count
        Bitmap::from_null_buffer(arrow_buffer::NullBuffer::new(buffer))
    }
}
