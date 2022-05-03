// Copyright 2022 Datafuse Labs.
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
// This file is copied from
// https://github.com/jorgecarleitao/arrow2/blob/main/src/bitmap/mutable.rs
// and modified by Databend. We only reserve some functions which is useful
// to us.

use std::hint::unreachable_unchecked;
use std::iter::FromIterator;

use arrow::bitmap::Bitmap;

use super::util::count_zeros;
use super::util::get_bit;
use super::util::set;
use super::util::set_bit;
use super::util::set_bit_unchecked;

/// A container to store booleans. [`MutableBitmap`] is semantically equivalent
/// to [`Vec<bool>`], but each value is stored as a single bit, thereby achieving a compression of 8x.
/// This container is the counterpart of [`Vec`] for boolean values.
/// [`MutableBitmap`] can be converted to a [`Bitmap`] at `O(1)`.
/// The main difference against [`Vec<bool>`] is that a bitmap cannot be represented as `&[bool]`.
/// # Implementation
/// This container is backed by [`Vec<u8>`].
pub struct MutableBitmap {
    buffer: Vec<u8>,
    // invariant: length.saturating_add(7) / 8 == buffer.len();
    length: usize,
}

impl Default for MutableBitmap {
    /// Initializes an default [`MutableBitmap`].
    fn default() -> Self {
        Self {
            buffer: Vec::new(),
            length: 0,
        }
    }
}

impl MutableBitmap {
    /// Initializes an empty [`MutableBitmap`].
    #[inline]
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            length: 0,
        }
    }

    /// Empties the [`MutableBitmap`].
    #[inline]
    pub fn clear(&mut self) {
        self.length = 0;
        self.buffer.clear();
    }

    /// Initializes a zeroed [`MutableBitmap`].
    #[inline]
    pub fn from_len_zeroed(length: usize) -> Self {
        Self {
            buffer: vec![0; length.saturating_add(7) / 8],
            length,
        }
    }

    /// Initializes a [`MutableBitmap`] with all values set to valid/ true.
    #[inline]
    pub fn from_len_set(length: usize) -> Self {
        Self {
            buffer: vec![u8::MAX; length.saturating_add(7) / 8],
            length,
        }
    }

    /// Initializes a pre-allocated [`MutableBitmap`] with capacity for `capacity` bits.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity.saturating_add(7) / 8),
            length: 0,
        }
    }

    /// Reserves `additional` bits in the [`MutableBitmap`], potentially re-allocating its buffer.
    #[inline(always)]
    pub fn reserve(&mut self, additional: usize) {
        self.buffer
            .reserve((self.length + additional).saturating_add(7) / 8 - self.buffer.len())
    }

    /// Pushes a new bit to the [`MutableBitmap`], re-sizing it if necessary.
    #[inline]
    pub fn push(&mut self, value: bool) {
        if self.length % 8 == 0 {
            self.buffer.push(0);
        }
        let byte = self.buffer.as_mut_slice().last_mut().unwrap();
        *byte = set(*byte, self.length % 8, value);
        self.length += 1;
    }

    /// Pop the last bit from the [`MutableBitmap`].
    /// Note if the [`MutableBitmap`] is empty, this method will return None.
    #[inline]
    pub fn pop(&mut self) -> Option<bool> {
        if self.is_empty() {
            return None;
        }

        self.length -= 1;
        let value = self.get(self.length);
        if self.length % 8 == 0 {
            self.buffer.pop();
        }
        Some(value)
    }

    /// Returns the capacity of [`MutableBitmap`] in number of bits.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buffer.capacity() * 8
    }

    /// Pushes a new bit to the [`MutableBitmap`]
    /// # Safety
    /// The caller must ensure that the [`MutableBitmap`] has sufficient capacity.
    #[inline]
    pub unsafe fn push_unchecked(&mut self, value: bool) {
        if self.length % 8 == 0 {
            self.buffer.push(0);
        }
        let byte = self.buffer.as_mut_slice().last_mut().unwrap();
        *byte = set(*byte, self.length % 8, value);
        self.length += 1;
    }

    /// Returns the number of unset bits on this [`MutableBitmap`].
    #[inline]
    pub fn null_count(&self) -> usize {
        count_zeros(&self.buffer, 0, self.length)
    }

    /// Returns the length of the [`MutableBitmap`].
    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns whether [`MutableBitmap`] is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn extend_set(&mut self, mut additional: usize) {
        let offset = self.length % 8;
        let added = if offset != 0 {
            // offset != 0 => at least one byte in the buffer
            let last_index = self.buffer.len() - 1;
            let last = &mut self.buffer[last_index];

            let remaining = 0b11111111u8;
            let remaining = remaining >> 8usize.saturating_sub(additional);
            let remaining = remaining << offset;
            *last |= remaining;
            std::cmp::min(additional, 8 - offset)
        } else {
            0
        };
        self.length += added;
        additional = additional.saturating_sub(added);
        if additional > 0 {
            debug_assert_eq!(self.length % 8, 0);
            let existing = self.length.saturating_add(7) / 8;
            let required = (self.length + additional).saturating_add(7) / 8;
            // add remaining as full bytes
            self.buffer
                .extend(std::iter::repeat(0b11111111u8).take(required - existing));
            self.length += additional;
        }
    }

    fn extend_unset(&mut self, mut additional: usize) {
        let offset = self.length % 8;
        let added = if offset != 0 {
            // offset != 0 => at least one byte in the buffer
            let last_index = self.buffer.len() - 1;
            let last = &mut self.buffer[last_index];
            *last &= 0b11111111u8 >> (8 - offset); // unset them
            std::cmp::min(additional, 8 - offset)
        } else {
            0
        };
        self.length += added;
        additional = additional.saturating_sub(added);
        if additional > 0 {
            debug_assert_eq!(self.length % 8, 0);
            self.buffer
                .resize((self.length + additional).saturating_add(7) / 8, 0);
            self.length += additional;
        }
    }

    /// Extends [`MutableBitmap`] by `additional` values of constant `value`.
    #[inline]
    pub fn extend_constant(&mut self, additional: usize, value: bool) {
        if additional == 0 {
            return;
        }

        if value {
            self.extend_set(additional)
        } else {
            self.extend_unset(additional)
        }
    }

    /// Returns whether the position `index` is set.
    /// # Panics
    /// Panics iff `index >= self.len()`.
    #[inline]
    pub fn get(&self, index: usize) -> bool {
        get_bit(&self.buffer, index)
    }

    /// Sets the position `index` to `value`
    /// # Panics
    /// Panics iff `index >= self.len()`.
    #[inline]
    pub fn set(&mut self, index: usize, value: bool) {
        set_bit(self.buffer.as_mut_slice(), index, value)
    }

    /// Sets the position `index` to `value`
    /// # Safety
    /// Caller must ensure that `index < self.len()`
    #[inline]
    pub unsafe fn set_unchecked(&mut self, index: usize, value: bool) {
        set_bit_unchecked(self.buffer.as_mut_slice(), index, value)
    }

    /// Shrinks the capacity of the [`MutableBitmap`] to fit its current length.
    pub fn shrink_to_fit(&mut self) {
        self.buffer.shrink_to_fit();
    }
}

impl MutableBitmap {
    /// Initializes a [`MutableBitmap`] from a [`Vec<u8>`] and a length.
    /// This function is `O(1)`.
    /// # Panic
    /// Panics iff the length is larger than the length of the buffer times 8.
    #[inline]
    pub fn from_vec(buffer: Vec<u8>, length: usize) -> Self {
        assert!(length <= buffer.len() * 8);
        Self { buffer, length }
    }
}

impl From<MutableBitmap> for Bitmap {
    #[inline]
    fn from(buffer: MutableBitmap) -> Self {
        Bitmap::from_u8_vec(buffer.buffer, buffer.length)
    }
}

impl From<MutableBitmap> for Option<Bitmap> {
    #[inline]
    fn from(buffer: MutableBitmap) -> Self {
        if buffer.null_count() > 0 {
            Some(Bitmap::from_u8_vec(buffer.buffer, buffer.length))
        } else {
            None
        }
    }
}

// impl<P: AsRef<[bool]>> From<P> for MutableBitmap {
//     #[inline]
//     fn from(slice: P) -> Self {
//         MutableBitmap::from_trusted_len_iter(slice.as_ref().iter().copied())
//     }
// }

impl FromIterator<bool> for MutableBitmap {
    fn from_iter<I>(iter: I) -> Self
    where I: IntoIterator<Item = bool> {
        let mut iterator = iter.into_iter();
        let mut buffer = {
            let byte_capacity: usize = iterator.size_hint().0.saturating_add(7) / 8;
            Vec::with_capacity(byte_capacity)
        };

        let mut length = 0;

        loop {
            let mut exhausted = false;
            let mut byte_accum: u8 = 0;
            let mut mask: u8 = 1;

            //collect (up to) 8 bits into a byte
            while mask != 0 {
                if let Some(value) = iterator.next() {
                    length += 1;
                    byte_accum |= match value {
                        true => mask,
                        false => 0,
                    };
                    mask <<= 1;
                } else {
                    exhausted = true;
                    break;
                }
            }

            // break if the iterator was exhausted before it provided a bool for this byte
            if exhausted && mask == 1 {
                break;
            }

            //ensure we have capacity to write the byte
            if buffer.len() == buffer.capacity() {
                //no capacity for new byte, allocate 1 byte more (plus however many more the iterator advertises)
                let additional_byte_capacity = 1usize.saturating_add(
                    iterator.size_hint().0.saturating_add(7) / 8, //convert bit count to byte count, rounding up
                );
                buffer.reserve(additional_byte_capacity)
            }

            // Soundness: capacity was allocated above
            buffer.push(byte_accum);
            if exhausted {
                break;
            }
        }
        Self { buffer, length }
    }
}

// [7, 6, 5, 4, 3, 2, 1, 0], [15, 14, 13, 12, 11, 10, 9, 8]
// [00000001_00000000_00000000_00000000_...] // u64
/// # Safety
/// The iterator must be trustedLen and its len must be least `len`.
#[inline]
unsafe fn get_chunk_unchecked(iterator: &mut impl Iterator<Item = bool>) -> u64 {
    let mut byte = 0u64;
    let mut mask;
    for i in 0..8 {
        mask = 1u64 << (8 * i);
        for _ in 0..8 {
            let value = match iterator.next() {
                Some(value) => value,
                None => unsafe { unreachable_unchecked() },
            };

            byte |= match value {
                true => mask,
                false => 0,
            };
            mask <<= 1;
        }
    }
    byte
}

/// # Safety
/// The iterator must be trustedLen and its len must be least `len`.
#[inline]
unsafe fn get_byte_unchecked(len: usize, iterator: &mut impl Iterator<Item = bool>) -> u8 {
    let mut byte_accum: u8 = 0;
    let mut mask: u8 = 1;
    for _ in 0..len {
        let value = match iterator.next() {
            Some(value) => value,
            None => unsafe { unreachable_unchecked() },
        };

        byte_accum |= match value {
            true => mask,
            false => 0,
        };
        mask <<= 1;
    }
    byte_accum
}

/// Extends the [`Vec<u8>`] from `iterator`
/// # Safety
/// The iterator MUST be [`TrustedLen`].
#[inline]
unsafe fn extend_aligned_trusted_iter_unchecked(
    buffer: &mut Vec<u8>,
    mut iterator: impl Iterator<Item = bool>,
) -> usize {
    let additional_bits = iterator.size_hint().1.unwrap();
    let chunks = additional_bits / 64;
    let remainder = additional_bits % 64;

    let additional = (additional_bits + 7) / 8;
    assert_eq!(
        additional,
        // a hint of how the following calculation will be done
        chunks * 8 + remainder / 8 + (remainder % 8 > 0) as usize
    );
    buffer.reserve(additional);

    // chunks of 64 bits
    for _ in 0..chunks {
        let chunk = get_chunk_unchecked(&mut iterator);
        buffer.extend_from_slice(&chunk.to_le_bytes());
    }

    // remaining complete bytes
    for _ in 0..(remainder / 8) {
        let byte = unsafe { get_byte_unchecked(8, &mut iterator) };
        buffer.push(byte)
    }

    // remaining bits
    let remainder = remainder % 8;
    if remainder > 0 {
        let byte = unsafe { get_byte_unchecked(remainder, &mut iterator) };
        buffer.push(byte)
    }
    additional_bits
}

impl MutableBitmap {
    /// Extends `self` from an iterator of trusted len.
    /// # Safety
    /// The caller must guarantee that the iterator has a trusted len.
    #[inline]
    pub unsafe fn extend_from_trusted_len_iter_unchecked<I: Iterator<Item = bool>>(
        &mut self,
        mut iterator: I,
    ) {
        // the length of the iterator throughout this function.
        let mut length = iterator.size_hint().1.unwrap();

        let bit_offset = self.length % 8;

        if length < 8 - bit_offset {
            if bit_offset == 0 {
                self.buffer.push(0);
            }
            // the iterator will not fill the last byte
            let byte = self.buffer.as_mut_slice().last_mut().unwrap();
            let mut i = bit_offset;
            for value in iterator {
                *byte = set(*byte, i, value);
                i += 1;
            }
            self.length += length;
            return;
        }

        // at this point we know that length will hit a byte boundary and thus
        // increase the buffer.

        if bit_offset != 0 {
            // we are in the middle of a byte; lets finish it
            let byte = self.buffer.as_mut_slice().last_mut().unwrap();
            (bit_offset..8).for_each(|i| {
                *byte = set(*byte, i, iterator.next().unwrap());
            });
            self.length += 8 - bit_offset;
            length -= 8 - bit_offset;
        }

        // everything is aligned; proceed with the bulk operation
        debug_assert_eq!(self.length % 8, 0);

        unsafe { extend_aligned_trusted_iter_unchecked(&mut self.buffer, iterator) };
        self.length += length;
    }

    /// Creates a new [`MutableBitmap`] from an iterator of booleans.
    /// # Safety
    /// The iterator must report an accurate length.
    #[inline]
    pub unsafe fn from_trusted_len_iter_unchecked<I>(iterator: I) -> Self
    where I: Iterator<Item = bool> {
        let mut buffer = Vec::<u8>::new();

        let length = extend_aligned_trusted_iter_unchecked(&mut buffer, iterator);

        Self { buffer, length }
    }
}
