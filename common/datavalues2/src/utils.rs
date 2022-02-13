// Copyright 2021 Datafuse Labs.
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
use std::ops::Deref;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;

pub struct Wrap<T>(pub T);

impl<T> Deref for Wrap<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

unsafe fn index_of_unchecked<T>(slice: &[T], item: &T) -> usize {
    (item as *const _ as usize - slice.as_ptr() as usize) / std::mem::size_of::<T>()
}

#[allow(dead_code)]
fn index_of<T>(slice: &[T], item: &T) -> Option<usize> {
    debug_assert!(std::mem::size_of::<T>() > 0);
    let ptr = item as *const T;
    unsafe {
        if slice.as_ptr() < ptr && slice.as_ptr().add(slice.len()) > ptr {
            Some(index_of_unchecked(slice, item))
        } else {
            None
        }
    }
}

pub fn get_iter_capacity<T, I: Iterator<Item = T>>(iter: &I) -> usize {
    match iter.size_hint() {
        (_lower, Some(upper)) => upper,
        (0, None) => 1024,
        (lower, None) => lower,
    }
}

pub fn const_validitiess(size: usize, valid: bool) -> Bitmap {
    let mut bitmap = MutableBitmap::with_capacity(size);
    bitmap.extend_constant(size, valid);
    bitmap.into()
}

pub fn combine_validities(lhs: Option<&Bitmap>, rhs: Option<&Bitmap>) -> Option<Bitmap> {
    match (lhs, rhs) {
        (Some(lhs), None) => Some(lhs.clone()),
        (None, Some(rhs)) => Some(rhs.clone()),
        (None, None) => None,
        (Some(lhs), Some(rhs)) => Some(lhs & rhs),
    }
}

pub fn combine_validities_2(lhs: Option<Bitmap>, rhs: Option<Bitmap>) -> Option<Bitmap> {
    match (lhs, rhs) {
        (Some(lhs), None) => Some(lhs),
        (None, Some(rhs)) => Some(rhs),
        (None, None) => None,
        (Some(lhs), Some(rhs)) => Some((&lhs) & (&rhs)),
    }
}

/// Forked from Arrow until their API stabilizes.
///
/// Note that the bound checks are optimized away.
///
#[cfg(feature = "simd")]
use packed_simd::u8x64;

const BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];

/// Returns the nearest number that is `>=` than `num` and is a multiple of 64
#[inline]
pub fn round_upto_multiple_of_64(num: usize) -> usize {
    round_upto_power_of_2(num, 64)
}

/// Returns the nearest multiple of `factor` that is `>=` than `num`. Here `factor` must
/// be a power of 2.
pub fn round_upto_power_of_2(num: usize, factor: usize) -> usize {
    debug_assert!(factor > 0 && (factor & (factor - 1)) == 0);
    (num + (factor - 1)) & !(factor - 1)
}

/// Returns whether bit at position `i` in `data` is set or not
#[inline]
pub fn get_bit(data: &[u8], i: usize) -> bool {
    (data[i >> 3] & BIT_MASK[i & 7]) != 0
}

/// Returns whether bit at position `i` in `data` is set or not.
///
/// # Safety
///
/// Note this doesn't do any bound checking, for performance reason. The caller is
/// responsible to guarantee that `i` is within bounds.
#[inline]
pub unsafe fn get_bit_raw(data: *const u8, i: usize) -> bool {
    (*data.add(i >> 3) & BIT_MASK[i & 7]) != 0
}

/// Sets bit at position `i` for `data`
#[inline]
pub fn set_bit(data: &mut [u8], i: usize) {
    data[i >> 3] |= BIT_MASK[i & 7];
}

/// Sets bit at position `i` for `data`
///
/// # Safety
///
/// Note this doesn't do any bound checking, for performance reason. The caller is
/// responsible to guarantee that `i` is within bounds.
#[inline]
pub unsafe fn set_bit_raw(data: *mut u8, i: usize) {
    *data.add(i >> 3) |= BIT_MASK[i & 7];
}

/// Sets bit at position `i` for `data` to 0
#[inline]
pub fn unset_bit(data: &mut [u8], i: usize) {
    data[i >> 3] ^= BIT_MASK[i & 7];
}

/// Sets bit at position `i` for `data` to 0
///
/// # Safety
///
/// Note this doesn't do any bound checking, for performance reason. The caller is
/// responsible to guarantee that `i` is within bounds.
#[inline]
pub unsafe fn unset_bit_raw(data: *mut u8, i: usize) {
    *data.add(i >> 3) ^= BIT_MASK[i & 7];
}

/// Returns the ceil of `value`/`divisor`
#[inline]
pub fn ceil(value: usize, divisor: usize) -> usize {
    let (quot, rem) = (value / divisor, value % divisor);
    if rem > 0 && divisor > 0 {
        quot + 1
    } else {
        quot
    }
}

/// Performs SIMD bitwise binary operations.
///
/// # Safety
///
/// Note that each slice should be 64 bytes and it is the callers responsibility to ensure
/// that this is the case.  If passed slices larger than 64 bytes the operation will only
/// be performed on the first 64 bytes.  Slices less than 64 bytes will panic.
#[cfg(simd)]
pub unsafe fn bitwise_bin_op_simd<F>(left: &[u8], right: &[u8], result: &mut [u8], op: F)
where F: Fn(u8x64, u8x64) -> u8x64 {
    let left_simd = u8x64::from_slice_unaligned_unchecked(left);
    let right_simd = u8x64::from_slice_unaligned_unchecked(right);
    let simd_result = op(left_simd, right_simd);
    simd_result.write_to_slice_unaligned_unchecked(result);
}
