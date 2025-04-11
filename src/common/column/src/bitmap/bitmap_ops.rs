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

use std::iter::TrustedLen;
use std::ops::BitAnd;
use std::ops::BitOr;
use std::ops::BitXor;
use std::ops::Not;

use super::utils::BitChunk;
use super::utils::BitChunkIterExact;
use super::utils::BitChunksExact;
use super::Bitmap;
use crate::bitmap::MutableBitmap;

/// Creates a [Vec<u8>] from an [`Iterator`] of [`BitChunk`].
/// # Safety
/// The iterator must be [`TrustedLen`].
pub unsafe fn from_chunk_iter_unchecked<T: BitChunk, I: Iterator<Item = T>>(
    iterator: I,
) -> Vec<u8> {
    let (_, upper) = iterator.size_hint();
    let upper = upper.expect("try_from_trusted_len_iter requires an upper limit");
    let len = upper * std::mem::size_of::<T>();

    let mut buffer = Vec::with_capacity(len);

    let mut dst = buffer.as_mut_ptr();
    for item in iterator {
        let bytes = item.to_ne_bytes();
        for i in 0..std::mem::size_of::<T>() {
            std::ptr::write(dst, bytes[i]);
            dst = dst.add(1);
        }
    }
    assert_eq!(
        dst.offset_from(buffer.as_ptr()) as usize,
        len,
        "Trusted iterator length was not accurately reported"
    );
    buffer.set_len(len);
    buffer
}

/// Creates a [`Vec<u8>`] from a [`TrustedLen`] of [`BitChunk`].
pub fn chunk_iter_to_vec<T: BitChunk, I: TrustedLen<Item = T>>(iter: I) -> Vec<u8> {
    unsafe { from_chunk_iter_unchecked(iter) }
}

/// Apply a bitwise operation `op` to four inputs and return the result as a [`Bitmap`].
pub fn quaternary<F>(a1: &Bitmap, a2: &Bitmap, a3: &Bitmap, a4: &Bitmap, op: F) -> Bitmap
where F: Fn(u64, u64, u64, u64) -> u64 {
    assert_eq!(a1.len(), a2.len());
    assert_eq!(a1.len(), a3.len());
    assert_eq!(a1.len(), a4.len());
    let a1_chunks = a1.chunks();
    let a2_chunks = a2.chunks();
    let a3_chunks = a3.chunks();
    let a4_chunks = a4.chunks();

    let rem_a1 = a1_chunks.remainder();
    let rem_a2 = a2_chunks.remainder();
    let rem_a3 = a3_chunks.remainder();
    let rem_a4 = a4_chunks.remainder();

    let chunks = a1_chunks
        .zip(a2_chunks)
        .zip(a3_chunks)
        .zip(a4_chunks)
        .map(|(((a1, a2), a3), a4)| op(a1, a2, a3, a4));
    let buffer =
        chunk_iter_to_vec(chunks.chain(std::iter::once(op(rem_a1, rem_a2, rem_a3, rem_a4))));

    let length = a1.len();

    Bitmap::from_u8_vec(buffer, length)
}

/// Apply a bitwise operation `op` to three inputs and return the result as a [`Bitmap`].
pub fn ternary<F>(a1: &Bitmap, a2: &Bitmap, a3: &Bitmap, op: F) -> Bitmap
where F: Fn(u64, u64, u64) -> u64 {
    assert_eq!(a1.len(), a2.len());
    assert_eq!(a1.len(), a3.len());
    let a1_chunks = a1.chunks();
    let a2_chunks = a2.chunks();
    let a3_chunks = a3.chunks();

    let rem_a1 = a1_chunks.remainder();
    let rem_a2 = a2_chunks.remainder();
    let rem_a3 = a3_chunks.remainder();

    let chunks = a1_chunks
        .zip(a2_chunks)
        .zip(a3_chunks)
        .map(|((a1, a2), a3)| op(a1, a2, a3));

    let buffer = chunk_iter_to_vec(chunks.chain(std::iter::once(op(rem_a1, rem_a2, rem_a3))));

    let length = a1.len();

    Bitmap::from_u8_vec(buffer, length)
}

/// Apply a bitwise operation `op` to two inputs and return the result as a [`Bitmap`].
pub fn binary<F>(lhs: &Bitmap, rhs: &Bitmap, op: F) -> Bitmap
where F: Fn(u64, u64) -> u64 {
    assert_eq!(lhs.len(), rhs.len());
    let lhs_chunks = lhs.chunks();
    let rhs_chunks = rhs.chunks();
    let rem_lhs = lhs_chunks.remainder();
    let rem_rhs = rhs_chunks.remainder();

    let chunks = lhs_chunks
        .zip(rhs_chunks)
        .map(|(left, right)| op(left, right));

    let buffer = chunk_iter_to_vec(chunks.chain(std::iter::once(op(rem_lhs, rem_rhs))));

    let length = lhs.len();

    Bitmap::from_u8_vec(buffer, length)
}

fn unary_impl<F, I>(iter: I, op: F, length: usize) -> Bitmap
where
    I: BitChunkIterExact<u64>,
    F: Fn(u64) -> u64,
{
    let rem = op(iter.remainder());

    let iterator = iter.map(op).chain(std::iter::once(rem));

    let buffer = chunk_iter_to_vec(iterator);

    Bitmap::from_u8_vec(buffer, length)
}

/// Apply a bitwise operation `op` to one input and return the result as a [`Bitmap`].
pub fn unary<F>(lhs: &Bitmap, op: F) -> Bitmap
where F: Fn(u64) -> u64 {
    let (slice, offset, length) = lhs.as_slice();
    if offset == 0 {
        let iter = BitChunksExact::<u64>::new(slice, length);
        unary_impl(iter, op, lhs.len())
    } else {
        let iter = lhs.chunks::<u64>();
        unary_impl(iter, op, lhs.len())
    }
}

// create a new [`Bitmap`] semantically equal to ``bitmap`` but with an offset equal to ``offset``
pub(crate) fn align(bitmap: &Bitmap, new_offset: usize) -> Bitmap {
    let length = bitmap.len();

    let bitmap: Bitmap = std::iter::repeat_n(false, new_offset)
        .chain(bitmap.iter())
        .collect();

    bitmap.sliced(new_offset, length)
}

#[inline]
/// Compute bitwise AND operation
pub fn and(lhs: &Bitmap, rhs: &Bitmap) -> Bitmap {
    if lhs.null_count() == lhs.len() || rhs.null_count() == rhs.len() {
        assert_eq!(lhs.len(), rhs.len());
        Bitmap::new_zeroed(lhs.len())
    } else {
        binary(lhs, rhs, |x, y| x & y)
    }
}

#[inline]
/// Compute bitwise OR operation
pub fn or(lhs: &Bitmap, rhs: &Bitmap) -> Bitmap {
    if lhs.null_count() == 0 || rhs.null_count() == 0 {
        assert_eq!(lhs.len(), rhs.len());
        let mut mutable = MutableBitmap::with_capacity(lhs.len());
        mutable.extend_constant(lhs.len(), true);
        mutable.into()
    } else {
        binary(lhs, rhs, |x, y| x | y)
    }
}

#[inline]
/// Compute bitwise XOR operation
pub fn xor(lhs: &Bitmap, rhs: &Bitmap) -> Bitmap {
    let lhs_nulls = lhs.null_count();
    let rhs_nulls = rhs.null_count();

    // all false or all true
    if lhs_nulls == rhs_nulls && rhs_nulls == rhs.len() || lhs_nulls == 0 && rhs_nulls == 0 {
        assert_eq!(lhs.len(), rhs.len());
        Bitmap::new_zeroed(rhs.len())
    }
    // all false and all true or vice versa
    else if (lhs_nulls == 0 && rhs_nulls == rhs.len())
        || (lhs_nulls == lhs.len() && rhs_nulls == 0)
    {
        assert_eq!(lhs.len(), rhs.len());
        let mut mutable = MutableBitmap::with_capacity(lhs.len());
        mutable.extend_constant(lhs.len(), true);
        mutable.into()
    } else {
        binary(lhs, rhs, |x, y| x ^ y)
    }
}

fn eq(lhs: &Bitmap, rhs: &Bitmap) -> bool {
    if lhs.len() != rhs.len() {
        return false;
    }

    let mut lhs_chunks = lhs.chunks::<u64>();
    let mut rhs_chunks = rhs.chunks::<u64>();

    let equal_chunks = lhs_chunks
        .by_ref()
        .zip(rhs_chunks.by_ref())
        .all(|(left, right)| left == right);

    if !equal_chunks {
        return false;
    }
    let lhs_remainder = lhs_chunks.remainder_iter();
    let rhs_remainder = rhs_chunks.remainder_iter();
    lhs_remainder.zip(rhs_remainder).all(|(x, y)| x == y)
}

impl PartialEq for Bitmap {
    fn eq(&self, other: &Self) -> bool {
        eq(self, other)
    }
}

impl<'b> BitOr<&'b Bitmap> for &Bitmap {
    type Output = Bitmap;

    fn bitor(self, rhs: &'b Bitmap) -> Bitmap {
        or(self, rhs)
    }
}

impl<'b> BitAnd<&'b Bitmap> for &Bitmap {
    type Output = Bitmap;

    fn bitand(self, rhs: &'b Bitmap) -> Bitmap {
        and(self, rhs)
    }
}

impl<'b> BitXor<&'b Bitmap> for &Bitmap {
    type Output = Bitmap;

    fn bitxor(self, rhs: &'b Bitmap) -> Bitmap {
        xor(self, rhs)
    }
}

impl Not for &Bitmap {
    type Output = Bitmap;

    fn not(self) -> Bitmap {
        unary(self, |a| !a)
    }
}
