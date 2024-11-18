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

mod basic;

pub mod binary;
pub mod boolean;
pub mod double;
pub mod integer;

pub use basic::CommonCompression;
use databend_common_expression::types::Bitmap;

use crate::error::Result;

// number of samples to take
pub static SAMPLE_COUNT: usize = 10;

// run size of each sample
pub static SAMPLE_SIZE: usize = 64;

/// Compression codec
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Compression {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
    Snappy = 3,

    // start from 10 for none common compression
    Rle = 10,
    Dict = 11,
    OneValue = 12,
    Freq = 13,
    Bitpacking = 14,
    DeltaBitpacking = 15,
    Patas = 16,
}

impl Default for Compression {
    fn default() -> Self {
        Self::None
    }
}

impl Compression {
    pub fn is_none(&self) -> bool {
        matches!(self, Compression::None)
    }

    pub fn from_codec(t: u8) -> Result<Self> {
        match t {
            0 => Ok(Compression::None),
            1 => Ok(Compression::Lz4),
            2 => Ok(Compression::Zstd),
            3 => Ok(Compression::Snappy),
            10 => Ok(Compression::Rle),
            11 => Ok(Compression::Dict),
            12 => Ok(Compression::OneValue),
            13 => Ok(Compression::Freq),
            14 => Ok(Compression::Bitpacking),
            15 => Ok(Compression::DeltaBitpacking),
            16 => Ok(Compression::Patas),

            other => Err(crate::error::Error::OutOfSpec(format!(
                "Unknown compression codec {other}",
            ))),
        }
    }

    pub fn raw_mode(&self) -> bool {
        matches!(
            self,
            Compression::None | Compression::Lz4 | Compression::Zstd | Compression::Snappy
        )
    }

    pub fn all_not_raw_encodings() -> Vec<Self> {
        vec![
            Compression::Rle,
            Compression::Dict,
            Compression::OneValue,
            Compression::Freq,
            Compression::Bitpacking,
            Compression::DeltaBitpacking,
            Compression::Patas,
        ]
    }
}

#[inline]
pub(crate) fn is_valid(validity: Option<&Bitmap>, i: usize) -> bool {
    match validity {
        Some(v) => v.get_bit(i),
        None => true,
    }
}

#[inline]
pub(crate) fn get_bits_needed(input: u64) -> u32 {
    u64::BITS - input.leading_zeros()
}

#[cfg(test)]
mod tests {}
