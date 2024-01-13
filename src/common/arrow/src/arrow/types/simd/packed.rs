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

pub use std::simd::f32x16;
pub use std::simd::f32x8;
pub use std::simd::f64x8;
pub use std::simd::i16x32;
pub use std::simd::i16x8;
pub use std::simd::i32x16;
pub use std::simd::i32x8;
pub use std::simd::i64x8;
pub use std::simd::i8x64;
pub use std::simd::i8x8;
pub use std::simd::mask32x16 as m32x16;
pub use std::simd::mask64x8 as m64x8;
pub use std::simd::mask8x64 as m8x64;
pub use std::simd::u16x32;
pub use std::simd::u16x8;
pub use std::simd::u32x16;
pub use std::simd::u32x8;
pub use std::simd::u64x8;
pub use std::simd::u8x64;
pub use std::simd::u8x8;
pub use std::simd::SimdPartialEq;

/// Vector of 32 16-bit masks
#[allow(non_camel_case_types)]
pub type m16x32 = std::simd::Mask<i16, 32>;

use super::*;

macro_rules! simd {
    ($name:tt, $type:ty, $lanes:expr, $chunk:ty, $mask:tt) => {
        unsafe impl NativeSimd for $name {
            const LANES: usize = $lanes;
            type Native = $type;
            type Chunk = $chunk;
            type Mask = $mask;

            #[inline]
            fn select(self, mask: $mask, default: Self) -> Self {
                mask.select(self, default)
            }

            #[inline]
            fn from_chunk(v: &[$type]) -> Self {
                <$name>::from_slice(v)
            }

            #[inline]
            fn from_incomplete_chunk(v: &[$type], remaining: $type) -> Self {
                let mut a = [remaining; $lanes];
                a.iter_mut().zip(v.iter()).for_each(|(a, b)| *a = *b);
                <$name>::from_chunk(a.as_ref())
            }

            #[inline]
            fn align(values: &[Self::Native]) -> (&[Self::Native], &[Self], &[Self::Native]) {
                unsafe { values.align_to::<Self>() }
            }
        }
    };
}

simd!(u8x64, u8, 64, u64, m8x64);
simd!(u16x32, u16, 32, u32, m16x32);
simd!(u32x16, u32, 16, u16, m32x16);
simd!(u64x8, u64, 8, u8, m64x8);
simd!(i8x64, i8, 64, u64, m8x64);
simd!(i16x32, i16, 32, u32, m16x32);
simd!(i32x16, i32, 16, u16, m32x16);
simd!(i64x8, i64, 8, u8, m64x8);
simd!(f32x16, f32, 16, u16, m32x16);
simd!(f64x8, f64, 8, u8, m64x8);

macro_rules! chunk_macro {
    ($type:ty, $chunk:ty, $simd:ty, $mask:tt, $m:expr) => {
        impl FromMaskChunk<$chunk> for $mask {
            #[inline]
            fn from_chunk(chunk: $chunk) -> Self {
                ($m)(chunk)
            }
        }
    };
}

chunk_macro!(u8, u64, u8x64, m8x64, from_chunk_u64);
chunk_macro!(u16, u32, u16x32, m16x32, from_chunk_u32);
chunk_macro!(u32, u16, u32x16, m32x16, from_chunk_u16);
chunk_macro!(u64, u8, u64x8, m64x8, from_chunk_u8);

#[inline]
fn from_chunk_u8(chunk: u8) -> m64x8 {
    let idx = u64x8::from_array([1, 2, 4, 8, 16, 32, 64, 128]);
    let vecmask = u64x8::splat(chunk as u64);

    (idx & vecmask).simd_eq(idx)
}

#[inline]
fn from_chunk_u16(chunk: u16) -> m32x16 {
    let idx = u32x16::from_array([
        1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768,
    ]);
    let vecmask = u32x16::splat(chunk as u32);

    (idx & vecmask).simd_eq(idx)
}

#[inline]
fn from_chunk_u32(chunk: u32) -> m16x32 {
    let idx = u16x32::from_array([
        1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 1, 2, 4, 8,
        16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768,
    ]);
    let left = u16x32::from_chunk(&[
        1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]);
    let right = u16x32::from_chunk(&[
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512,
        1024, 2048, 4096, 8192, 16384, 32768,
    ]);

    let a = chunk.to_ne_bytes();
    let a1 = u16::from_ne_bytes([a[2], a[3]]);
    let a2 = u16::from_ne_bytes([a[0], a[1]]);

    let vecmask1 = u16x32::splat(a1);
    let vecmask2 = u16x32::splat(a2);

    (idx & left & vecmask1).simd_eq(idx) | (idx & right & vecmask2).simd_eq(idx)
}

#[inline]
fn from_chunk_u64(chunk: u64) -> m8x64 {
    let idx = u8x64::from_array([
        1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128, 1,
        2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128, 1, 2,
        4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128,
    ]);
    let idxs = [
        u8x64::from_chunk(&[
            1, 2, 4, 8, 16, 32, 64, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
        ]),
        u8x64::from_chunk(&[
            0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 4, 8, 16, 32, 64, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
        ]),
        u8x64::from_chunk(&[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 4, 8, 16, 32, 64, 128, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
        ]),
        u8x64::from_chunk(&[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 4, 8, 16,
            32, 64, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0,
        ]),
        u8x64::from_chunk(&[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 1, 2, 4, 8, 16, 32, 64, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
        ]),
        u8x64::from_chunk(&[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 4, 8, 16, 32, 64, 128, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
        ]),
        u8x64::from_chunk(&[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 4, 8, 16, 32, 64, 128,
            0, 0, 0, 0, 0, 0, 0, 0,
        ]),
        u8x64::from_chunk(&[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2,
            4, 8, 16, 32, 64, 128,
        ]),
    ];

    let a = chunk.to_ne_bytes();

    let mut result = m8x64::default();
    for i in 0..8 {
        result |= (idxs[i] & u8x64::splat(a[i])).simd_eq(idx)
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic1() {
        let a = 0b00000001000000010000000100000001u32;
        let a = from_chunk_u32(a);
        for i in 0..32 {
            assert_eq!(a.test(i), i % 8 == 0)
        }
    }

    #[test]
    fn test_basic2() {
        let a = 0b0000000100000001000000010000000100000001000000010000000100000001u64;
        let a = from_chunk_u64(a);
        for i in 0..64 {
            assert_eq!(a.test(i), i % 8 == 0)
        }
    }
}
