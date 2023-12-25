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

use databend_common_arrow::arrow::bitmap::Bitmap;
use ethnum::i256;

use super::row_converter::null_sentinel;
use crate::types::string::StringColumnBuilder;
use crate::types::F32;
use crate::types::F64;

pub trait FixedLengthEncoding: Copy {
    // 1 for null byte
    const ENCODED_LEN: usize = 1 + std::mem::size_of::<Self::Encoded>();

    type Encoded: Sized + Copy + AsRef<[u8]> + AsMut<[u8]>;

    fn encode(self) -> Self::Encoded;
}

impl FixedLengthEncoding for bool {
    type Encoded = [u8; 1];

    fn encode(self) -> [u8; 1] {
        [self as u8]
    }
}

macro_rules! encode_signed {
    ($n:expr, $t:ty) => {
        impl FixedLengthEncoding for $t {
            type Encoded = [u8; $n];

            fn encode(self) -> [u8; $n] {
                let mut b = self.to_be_bytes();
                // Toggle top "sign" bit to ensure consistent sort order
                b[0] ^= 0x80;
                b
            }
        }
    };
}

encode_signed!(1, i8);
encode_signed!(2, i16);
encode_signed!(4, i32);
encode_signed!(8, i64);
encode_signed!(16, i128);
encode_signed!(32, i256);

macro_rules! encode_unsigned {
    ($n:expr, $t:ty) => {
        impl FixedLengthEncoding for $t {
            type Encoded = [u8; $n];

            fn encode(self) -> [u8; $n] {
                self.to_be_bytes()
            }
        }
    };
}

encode_unsigned!(1, u8);
encode_unsigned!(2, u16);
encode_unsigned!(4, u32);
encode_unsigned!(8, u64);

impl FixedLengthEncoding for F32 {
    type Encoded = [u8; 4];

    fn encode(self) -> [u8; 4] {
        // https://github.com/rust-lang/rust/blob/9c20b2a8cc7588decb6de25ac6a7912dcef24d65/library/core/src/num/f32.rs#L1176-L1260
        let s = self.to_bits() as i32;
        let val = s ^ (((s >> 31) as u32) >> 1) as i32;
        val.encode()
    }
}

impl FixedLengthEncoding for F64 {
    type Encoded = [u8; 8];

    fn encode(self) -> [u8; 8] {
        // https://github.com/rust-lang/rust/blob/9c20b2a8cc7588decb6de25ac6a7912dcef24d65/library/core/src/num/f32.rs#L1176-L1260
        let s = self.to_bits() as i64;
        let val = s ^ (((s >> 63) as u64) >> 1) as i64;
        val.encode()
    }
}

pub fn encode<T, I>(
    out: &mut StringColumnBuilder,
    iter: I,
    (all_null, validity): (bool, Option<&Bitmap>),
    asc: bool,
    nulls_first: bool,
) where
    T: FixedLengthEncoding,
    I: IntoIterator<Item = T>,
{
    if let Some(validity) = validity {
        for ((offset, val), v) in out.offsets.iter_mut().skip(1).zip(iter).zip(validity) {
            let start = *offset as usize;
            let end = start + T::ENCODED_LEN;
            if v {
                let to_write = &mut out.data[start..end];
                to_write[0] = 1;
                let mut encoded = val.encode();
                if !asc {
                    // Flip bits to reverse order
                    encoded.as_mut().iter_mut().for_each(|v| *v = !*v)
                }
                to_write[1..].copy_from_slice(encoded.as_ref());
            } else {
                out.data[start] = null_sentinel(nulls_first);
            }
            *offset = end as u64;
        }
    } else if all_null {
        for offset in out.offsets.iter_mut().skip(1) {
            let start = *offset as usize;
            let end = start + T::ENCODED_LEN;
            out.data[start] = null_sentinel(nulls_first);
            *offset = end as u64;
        }
    } else {
        for (offset, val) in out.offsets.iter_mut().skip(1).zip(iter) {
            let start = *offset as usize;
            let end = start + T::ENCODED_LEN;
            let to_write = &mut out.data[start..end];
            to_write[0] = 1;
            let mut encoded = val.encode();
            if !asc {
                // Flip bits to reverse order
                encoded.as_mut().iter_mut().for_each(|v| *v = !*v)
            }
            to_write[1..].copy_from_slice(encoded.as_ref());
            *offset = end as u64;
        }
    }
}
