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

use std::io::Error;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Write;
use std::marker::PhantomData;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_io::prelude::BinaryRead;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use serde_json::from_slice;
#[cfg(feature = "dev")]
use snap::raw::Decoder as SnapDecoder;
#[cfg(feature = "dev")]
use snap::raw::Encoder as SnapEncoder;
use zstd::Decoder as ZstdDecoder;
use zstd::Encoder as ZstdEncoder;

// Formerly defined in internal_columns.rs, for dependency reasons, we move it here.
// The "_row_id" of internal column assumes that the max block count of a segment is 2^11
// (during compaction, a more modest constraint is used : 2 * 1000 -1)
pub const NUM_BLOCK_ID_BITS: usize = 11;
pub const MAX_SEGMENT_BLOCK_NUMBER: usize = 1 << NUM_BLOCK_ID_BITS;

#[repr(u8)]
#[derive(serde::Serialize, serde::Deserialize, Default, Debug, Clone, PartialEq)]
pub enum MetaCompression {
    None = 0,
    #[default]
    Zstd = 1,
    Snappy = 2,
}

impl TryFrom<u8> for MetaCompression {
    type Error = ErrorCode;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(MetaCompression::None),
            1 => Ok(MetaCompression::Zstd),
            2 => Ok(MetaCompression::Snappy),
            other => Err(ErrorCode::UnknownFormat(format!(
                "unsupported compression: {}",
                other
            ))),
        }
    }
}

pub fn compress(compression: &MetaCompression, data: Vec<u8>) -> Result<Vec<u8>> {
    match compression {
        MetaCompression::None => Ok(data),
        MetaCompression::Zstd => {
            let mut encoder = ZstdEncoder::new(Vec::new(), 0)?;
            encoder.write_all(&data)?;
            Ok(encoder.finish()?)
        }
        #[cfg(feature = "dev")]
        MetaCompression::Snappy => Ok(SnapEncoder::new()
            .compress_vec(&data)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?),
        #[cfg(not(feature = "dev"))]
        _ => Err(ErrorCode::UnknownFormat(format!(
            "unsupported compression: {:?}",
            compression
        ))),
    }
}

pub fn decompress(compression: &MetaCompression, data: Vec<u8>) -> Result<Vec<u8>> {
    match compression {
        MetaCompression::None => Ok(data),
        MetaCompression::Zstd => {
            let mut decoder = ZstdDecoder::new(&data[..])?;
            let mut decompressed_data = Vec::new();
            decoder
                .read_to_end(&mut decompressed_data)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            Ok(decompressed_data)
        }
        #[cfg(feature = "dev")]
        MetaCompression::Snappy => Ok(SnapDecoder::new()
            .decompress_vec(&data)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?),
        #[cfg(not(feature = "dev"))]
        _ => Err(ErrorCode::UnknownFormat(format!(
            "unsupported compression: {:?}",
            compression
        ))),
    }
}

#[repr(u8)]
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
pub enum MetaEncoding {
    Bincode = 1,
    MessagePack = 2,
    Json = 3,
}

impl TryFrom<u8> for MetaEncoding {
    type Error = ErrorCode;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(MetaEncoding::Bincode),
            2 => Ok(MetaEncoding::MessagePack),
            3 => Ok(MetaEncoding::Json),
            other => Err(ErrorCode::UnknownFormat(format!(
                "unsupported encoding: {}",
                other
            ))),
        }
    }
}

pub fn encode<T: Serialize>(encoding: &MetaEncoding, data: &T) -> Result<Vec<u8>> {
    match encoding {
        MetaEncoding::Bincode => {
            Ok(bincode::serialize(data).map_err(|e| Error::new(ErrorKind::InvalidData, e))?)
        }
        MetaEncoding::MessagePack => {
            // using to_vec_named to keep the format backward compatible
            let bytes = rmp_serde::to_vec_named(&data)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            Ok(bytes)
        }
        MetaEncoding::Json => Ok(serde_json::to_vec(&data)?),
    }
}

pub fn decode<'a, T: Deserialize<'a>>(encoding: &MetaEncoding, data: &'a [u8]) -> Result<T> {
    match encoding {
        MetaEncoding::Bincode => {
            Ok(bincode::deserialize(data).map_err(|e| Error::new(ErrorKind::InvalidData, e))?)
        }
        MetaEncoding::MessagePack => {
            Ok(rmp_serde::from_slice(data).map_err(|e| Error::new(ErrorKind::InvalidData, e))?)
        }
        MetaEncoding::Json => Ok(from_slice::<T>(data)?),
    }
}

pub fn read_and_deserialize<R, T>(
    reader: &mut R,
    size: u64,
    encoding: &MetaEncoding,
    compression: &MetaCompression,
) -> Result<T>
where
    R: Read,
    T: DeserializeOwned,
{
    let mut compressed_data = vec![0; size as usize];
    reader.read_exact(&mut compressed_data)?;

    let decompressed_data = decompress(compression, compressed_data)?;

    decode(encoding, &decompressed_data)
}

pub struct SegmentHeader {
    pub version: u64,
    pub encoding: MetaEncoding,
    pub compression: MetaCompression,
    pub blocks_size: u64,
    pub summary_size: u64,
}

pub fn decode_segment_header<R>(reader: &mut R) -> Result<SegmentHeader>
where R: Read {
    let version = reader.read_scalar::<u64>()?;
    let encoding = MetaEncoding::try_from(reader.read_scalar::<u8>()?)?;
    let compression = MetaCompression::try_from(reader.read_scalar::<u8>()?)?;
    let blocks_size: u64 = reader.read_scalar::<u64>()?;
    let summary_size: u64 = reader.read_scalar::<u64>()?;
    Ok(SegmentHeader {
        version,
        encoding,
        compression,
        blocks_size,
        summary_size,
    })
}

pub fn load_json<T>(r: impl Read, _v: &PhantomData<T>) -> Result<T>
where T: DeserializeOwned {
    Ok(serde_json::from_reader(r)?)
}
