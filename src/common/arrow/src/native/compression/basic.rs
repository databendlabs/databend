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

use super::Compression;
use crate::arrow::error::Error;
use crate::arrow::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommonCompression {
    None,
    Lz4,
    Zstd,
    Snappy,
}

impl Default for CommonCompression {
    fn default() -> Self {
        Self::None
    }
}

impl TryFrom<&Compression> for CommonCompression {
    type Error = Error;

    fn try_from(value: &Compression) -> Result<Self> {
        match value {
            Compression::None => Ok(CommonCompression::None),
            Compression::Lz4 => Ok(CommonCompression::Lz4),
            Compression::Zstd => Ok(CommonCompression::Zstd),
            Compression::Snappy => Ok(CommonCompression::Snappy),
            other => Err(Error::OutOfSpec(format!(
                "Unknown compression codec {other:?}",
            ))),
        }
    }
}

impl CommonCompression {
    pub fn to_compression(self) -> Compression {
        match self {
            Self::None => Compression::None,
            Self::Lz4 => Compression::Lz4,
            Self::Zstd => Compression::Zstd,
            Self::Snappy => Compression::Snappy,
        }
    }

    pub fn decompress(&self, input: &[u8], out_slice: &mut [u8]) -> Result<()> {
        match self {
            Self::Lz4 => decompress_lz4(input, out_slice),
            Self::Zstd => decompress_zstd(input, out_slice),
            Self::Snappy => decompress_snappy(input, out_slice),
            Self::None => {
                out_slice.copy_from_slice(input);
                Ok(())
            }
        }
    }

    pub fn compress(&self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
        match self {
            Self::Lz4 => compress_lz4(input_buf, output_buf),
            Self::Zstd => compress_zstd(input_buf, output_buf),
            Self::Snappy => compress_snappy(input_buf, output_buf),
            Self::None => {
                output_buf.extend_from_slice(input_buf);
                Ok(input_buf.len())
            }
        }
    }
}

pub fn decompress_lz4(input_buf: &[u8], output_buf: &mut [u8]) -> Result<()> {
    lz4::block::decompress_to_buffer(input_buf, Some(output_buf.len() as i32), output_buf)
        .map(|_| {})
        .map_err(|e| e.into())
}

pub fn decompress_zstd(input_buf: &[u8], output_buf: &mut [u8]) -> Result<()> {
    zstd::bulk::decompress_to_buffer(input_buf, output_buf)
        .map(|_| {})
        .map_err(|e| e.into())
}

pub fn decompress_snappy(input_buf: &[u8], output_buf: &mut [u8]) -> Result<()> {
    snap::raw::Decoder::new()
        .decompress(input_buf, output_buf)
        .map(|_| {})
        .map_err(|e| {
            crate::arrow::error::Error::External("decompress snappy failed".to_owned(), Box::new(e))
        })
}

pub fn compress_lz4(input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
    let bound = lz4::block::compress_bound(input_buf.len())?;
    let len = output_buf.len();
    output_buf.reserve(bound);

    let s = unsafe { core::slice::from_raw_parts_mut(output_buf.as_mut_ptr().add(len), bound) };

    let size = lz4::block::compress_to_buffer(input_buf, None, false, s).map_err(|e| {
        crate::arrow::error::Error::External("Compress lz4 failed".to_owned(), Box::new(e))
    })?;

    unsafe { output_buf.set_len(size + len) };
    Ok(size)
}

pub fn compress_zstd(input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
    let bound = zstd::zstd_safe::compress_bound(input_buf.len());
    let len = output_buf.len();
    output_buf.reserve(bound);

    let s = unsafe { core::slice::from_raw_parts_mut(output_buf.as_mut_ptr().add(len), bound) };

    let size = zstd::bulk::compress_to_buffer(input_buf, s, 0).map_err(|e| {
        crate::arrow::error::Error::External("Compress zstd failed".to_owned(), Box::new(e))
    })?;

    unsafe { output_buf.set_len(size + len) };
    Ok(size)
}

pub fn compress_snappy(input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
    let bound = snap::raw::max_compress_len(input_buf.len());
    let len = output_buf.len();

    output_buf.reserve(bound);
    let s = unsafe { core::slice::from_raw_parts_mut(output_buf.as_mut_ptr().add(len), bound) };

    let size = snap::raw::Encoder::new()
        .compress(input_buf, s)
        .map_err(|e| {
            crate::arrow::error::Error::External("Compress snappy failed".to_owned(), Box::new(e))
        })?;

    unsafe { output_buf.set_len(size + len) };
    Ok(size)
}
