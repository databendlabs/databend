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

use crate::arrow::bitmap::Bitmap;
use crate::arrow::error::Result;

// number of samples to take
pub static SAMPLE_COUNT: usize = 10;

// run size of each sample
pub static SAMPLE_SIZE: usize = 64;

/// Compression codec
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Compression {
    None,
    Lz4,
    Zstd,
    Snappy,

    // start from 10 for none common compression
    Rle,
    Dict,
    OneValue,
    Freq,
    Bitpacking,
    DeltaBitpacking,
    Patas,
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

            other => Err(crate::arrow::error::Error::OutOfSpec(format!(
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
}

impl From<Compression> for u8 {
    fn from(value: Compression) -> Self {
        match value {
            Compression::None => 0,
            Compression::Lz4 => 1,
            Compression::Zstd => 2,
            Compression::Snappy => 3,
            Compression::Rle => 10,
            Compression::Dict => 11,
            Compression::OneValue => 12,
            Compression::Freq => 13,
            Compression::Bitpacking => 14,
            Compression::DeltaBitpacking => 15,
            Compression::Patas => 16,
        }
    }
}

#[inline]
pub(crate) fn is_valid(validity: &Option<&Bitmap>, i: usize) -> bool {
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
