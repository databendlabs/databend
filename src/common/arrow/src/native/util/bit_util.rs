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

use std::mem::size_of;

use crate::arrow::buffer::Buffer;
use crate::arrow::error::Error;
use crate::arrow::error::Result;

#[inline]
pub fn from_le_slice<T: FromBytes>(bs: &[u8]) -> T {
    // TODO: propagate the error (#3577)
    T::try_from_le_slice(bs).unwrap()
}

#[inline]
fn array_from_slice<const N: usize>(bs: &[u8]) -> Result<[u8; N]> {
    // Need to slice as may be called with zero-padded values
    match bs.get(..N) {
        Some(b) => Ok(b.try_into().unwrap()),
        None => Err(general_err!(
            "error converting value, expected {} bytes got {}",
            N,
            bs.len()
        )),
    }
}

pub trait FromBytes: Sized {
    type Buffer: AsMut<[u8]> + Default;
    fn try_from_le_slice(b: &[u8]) -> Result<Self>;
    fn from_le_bytes(bs: Self::Buffer) -> Self;
}

macro_rules! from_le_bytes {
    ($($ty: ty),*) => {
        $(
        impl FromBytes for $ty {
            type Buffer = [u8; size_of::<Self>()];
            fn try_from_le_slice(b: &[u8]) -> Result<Self> {
                Ok(Self::from_le_bytes(array_from_slice(b)?))
            }
            fn from_le_bytes(bs: Self::Buffer) -> Self {
                <$ty>::from_le_bytes(bs)
            }
        }
        )*
    };
}

from_le_bytes! { u8, u16, u32, u64, i8, i16, i32, i64, f32, f64 }

impl FromBytes for bool {
    type Buffer = [u8; 1];

    fn try_from_le_slice(b: &[u8]) -> Result<Self> {
        Ok(Self::from_le_bytes(array_from_slice(b)?))
    }
    fn from_le_bytes(bs: Self::Buffer) -> Self {
        bs[0] != 0
    }
}

pub trait AsBytes {
    /// Returns slice of bytes for this data type.
    fn as_bytes(&self) -> &[u8];
}

impl AsBytes for Buffer<u8> {
    fn as_bytes(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsBytes for [u8] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

impl AsBytes for bool {
    fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self as *const bool as *const u8, 1) }
    }
}

impl AsBytes for Vec<u8> {
    fn as_bytes(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<'a> AsBytes for &'a str {
    fn as_bytes(&self) -> &[u8] {
        (self as &str).as_bytes()
    }
}

impl AsBytes for str {
    fn as_bytes(&self) -> &[u8] {
        (self as &str).as_bytes()
    }
}

macro_rules! gen_as_bytes {
    ($source_ty:ident) => {
        impl AsBytes for $source_ty {
            #[allow(clippy::size_of_in_element_count)]
            fn as_bytes(&self) -> &[u8] {
                unsafe {
                    std::slice::from_raw_parts(
                        self as *const $source_ty as *const u8,
                        std::mem::size_of::<$source_ty>(),
                    )
                }
            }
        }
    };
}

gen_as_bytes!(i8);
gen_as_bytes!(i16);
gen_as_bytes!(i32);
gen_as_bytes!(i64);
gen_as_bytes!(u8);
gen_as_bytes!(u16);
gen_as_bytes!(u32);
gen_as_bytes!(u64);
gen_as_bytes!(f32);
gen_as_bytes!(f64);

/// Reads `size` of bytes from `src`, and reinterprets them as type `ty`, in
/// little-endian order.
/// This is copied and modified from byteorder crate.
pub(crate) fn read_num_bytes<T>(size: usize, src: &[u8]) -> T
where T: FromBytes {
    assert!(size <= src.len());
    let mut buffer = <T as FromBytes>::Buffer::default();
    buffer.as_mut()[..size].copy_from_slice(&src[..size]);
    <T>::from_le_bytes(buffer)
}

/// Returns the ceil of value/divisor.
///
/// This function should be removed after
/// [`int_roundings`](https://github.com/rust-lang/rust/issues/88581) is stable.
#[inline]
pub fn ceil<T: num::Integer>(value: T, divisor: T) -> T {
    num::Integer::div_ceil(&value, &divisor)
}

/// Returns the `num_bits` least-significant bits of `v`
#[inline]
pub fn trailing_bits(v: u64, num_bits: usize) -> u64 {
    if num_bits >= 64 {
        v
    } else {
        v & ((1 << num_bits) - 1)
    }
}

/// Returns the minimum number of bits needed to represent the value 'x'
#[inline]
pub fn num_required_bits(x: u64) -> u8 {
    64 - x.leading_zeros() as u8
}

static BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];

/// Returns whether bit at position `i` in `data` is set or not
#[inline]
pub fn get_bit(data: &[u8], i: usize) -> bool {
    (data[i >> 3] & BIT_MASK[i & 7]) != 0
}

#[cfg(test)]
mod tests {
    use rand::distributions::Distribution;
    use rand::distributions::Standard;
    use rand::thread_rng;

    use super::*;

    fn random_numbers<T>(n: usize) -> Vec<T>
    where Standard: Distribution<T> {
        let mut rng = thread_rng();
        Standard.sample_iter(&mut rng).take(n).collect()
    }

    #[test]
    fn test_ceil() {
        assert_eq!(ceil(0, 1), 0);
        assert_eq!(ceil(1, 1), 1);
        assert_eq!(ceil(1, 2), 1);
        assert_eq!(ceil(1, 8), 1);
        assert_eq!(ceil(7, 8), 1);
        assert_eq!(ceil(8, 8), 1);
        assert_eq!(ceil(9, 8), 2);
        assert_eq!(ceil(9, 9), 1);
        assert_eq!(ceil(10000000000_u64, 10), 1000000000);
        assert_eq!(ceil(10_u64, 10000000000), 1);
        assert_eq!(ceil(10000000000_u64, 1000000000), 10);
    }

    #[test]
    fn test_num_required_bits() {
        assert_eq!(num_required_bits(0), 0);
        assert_eq!(num_required_bits(1), 1);
        assert_eq!(num_required_bits(2), 2);
        assert_eq!(num_required_bits(4), 3);
        assert_eq!(num_required_bits(8), 4);
        assert_eq!(num_required_bits(10), 4);
        assert_eq!(num_required_bits(12), 4);
        assert_eq!(num_required_bits(16), 5);
        assert_eq!(num_required_bits(u64::MAX), 64);
    }

    #[test]
    fn test_get_bit() {
        // 00001101
        assert!(get_bit(&[0b00001101], 0));
        assert!(!get_bit(&[0b00001101], 1));
        assert!(get_bit(&[0b00001101], 2));
        assert!(get_bit(&[0b00001101], 3));

        // 01001001 01010010
        assert!(get_bit(&[0b01001001, 0b01010010], 0));
        assert!(!get_bit(&[0b01001001, 0b01010010], 1));
        assert!(!get_bit(&[0b01001001, 0b01010010], 2));
        assert!(get_bit(&[0b01001001, 0b01010010], 3));
        assert!(!get_bit(&[0b01001001, 0b01010010], 4));
        assert!(!get_bit(&[0b01001001, 0b01010010], 5));
        assert!(get_bit(&[0b01001001, 0b01010010], 6));
        assert!(!get_bit(&[0b01001001, 0b01010010], 7));
        assert!(!get_bit(&[0b01001001, 0b01010010], 8));
        assert!(get_bit(&[0b01001001, 0b01010010], 9));
        assert!(!get_bit(&[0b01001001, 0b01010010], 10));
        assert!(!get_bit(&[0b01001001, 0b01010010], 11));
        assert!(get_bit(&[0b01001001, 0b01010010], 12));
        assert!(!get_bit(&[0b01001001, 0b01010010], 13));
        assert!(get_bit(&[0b01001001, 0b01010010], 14));
        assert!(!get_bit(&[0b01001001, 0b01010010], 15));
    }
}
