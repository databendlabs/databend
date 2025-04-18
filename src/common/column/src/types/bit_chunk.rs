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

use std::fmt::Binary;
use std::ops::BitAndAssign;
use std::ops::Not;
use std::ops::Shl;
use std::ops::ShlAssign;
use std::ops::ShrAssign;

use num_traits::PrimInt;

use super::NativeType;

/// A chunk of bits. This is used to create masks of a given length
/// whose width is `1` bit. In `portable_simd` notation, this corresponds to `m1xY`.
///
/// This (sealed) trait is implemented for [`u8`], [`u16`], [`u32`] and [`u64`].
pub trait BitChunk:
    super::private::Sealed
    + PrimInt
    + NativeType
    + Binary
    + ShlAssign
    + Not<Output = Self>
    + ShrAssign<usize>
    + ShlAssign<usize>
    + Shl<usize, Output = Self>
    + BitAndAssign
{
    /// convert itself into bytes.
    fn to_ne_bytes(self) -> Self::Bytes;
    /// convert itself from bytes.
    fn from_ne_bytes(v: Self::Bytes) -> Self;
}

macro_rules! bit_chunk {
    ($ty:ty) => {
        impl BitChunk for $ty {
            #[inline(always)]
            fn to_ne_bytes(self) -> Self::Bytes {
                self.to_ne_bytes()
            }

            #[inline(always)]
            fn from_ne_bytes(v: Self::Bytes) -> Self {
                Self::from_ne_bytes(v)
            }
        }
    };
}

bit_chunk!(u8);
bit_chunk!(u16);
bit_chunk!(u32);
bit_chunk!(u64);

/// An [`Iterator<Item=bool>`] over a [`BitChunk`]. This iterator is often
/// compiled to SIMD.
/// The [LSB](https://en.wikipedia.org/wiki/Bit_numbering#Least_significant_bit) corresponds
/// to the first slot, as defined by the arrow specification.
/// # Example
/// ```
/// use arrow2::types::BitChunkIter;
/// let a = 0b00010000u8;
/// let iter = BitChunkIter::new(a, 7);
/// let r = iter.collect::<Vec<_>>();
/// assert_eq!(r, vec![false, false, false, false, true, false, false]);
/// ```
pub struct BitChunkIter<T: BitChunk> {
    value: T,
    mask: T,
    remaining: usize,
}

impl<T: BitChunk> BitChunkIter<T> {
    /// Creates a new [`BitChunkIter`] with `len` bits.
    #[inline]
    pub fn new(value: T, len: usize) -> Self {
        assert!(len <= std::mem::size_of::<T>() * 8);
        Self {
            value,
            remaining: len,
            mask: T::one(),
        }
    }
}

impl<T: BitChunk> Iterator for BitChunkIter<T> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        };
        let result = Some(self.value & self.mask != T::zero());
        self.remaining -= 1;
        self.mask <<= 1;
        result
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

// # Safety
// a mathematical invariant of this iterator
unsafe impl<T: BitChunk> std::iter::TrustedLen for BitChunkIter<T> {}

/// An [`Iterator<Item=usize>`] over a [`BitChunk`] returning the index of each bit set in the chunk
/// See <https://lemire.me/blog/2018/03/08/iterating-over-set-bits-quickly-simd-edition/> for details
/// # Example
/// ```
/// use arrow2::types::BitChunkOnes;
/// let a = 0b00010000u8;
/// let iter = BitChunkOnes::new(a);
/// let r = iter.collect::<Vec<_>>();
/// assert_eq!(r, vec![4]);
/// ```
pub struct BitChunkOnes<T: BitChunk> {
    value: T,
    remaining: usize,
}

impl<T: BitChunk> BitChunkOnes<T> {
    /// Creates a new [`BitChunkOnes`] with `len` bits.
    #[inline]
    pub fn new(value: T) -> Self {
        Self {
            value,
            remaining: value.count_ones() as usize,
        }
    }
}

impl<T: BitChunk> Iterator for BitChunkOnes<T> {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let v = self.value.trailing_zeros() as usize;
        self.value &= self.value - T::one();

        self.remaining -= 1;
        Some(v)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

// # Safety
// a mathematical invariant of this iterator
unsafe impl<T: BitChunk> std::iter::TrustedLen for BitChunkOnes<T> {}
