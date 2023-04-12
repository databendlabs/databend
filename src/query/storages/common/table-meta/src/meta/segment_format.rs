//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
use std::io::Cursor;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Write;

use common_exception::ErrorCode;
use common_exception::Result;
use rmp_serde::Deserializer;
use rmp_serde::Serializer;
use serde::Deserialize;
use serde::Serialize;
use serde_json::from_slice;
use snap::raw::Decoder as SnapDecoder;
use snap::raw::Encoder as SnapEncoder;
use zstd::Decoder as ZstdDecoder;
use zstd::Encoder as ZstdEncoder;

#[repr(u64)]
#[derive(Default)]
pub enum Compression {
    None = 0,
    #[default]
    Zstd = 1,
    Snappy = 2,
}

impl TryFrom<u64> for Compression {
    type Error = ErrorCode;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Compression::None),
            1 => Ok(Compression::Zstd),
            2 => Ok(Compression::Snappy),
            other => Err(ErrorCode::UnknownFormat(format!(
                "unsupported compression: {}",
                other
            ))),
        }
    }
}

pub fn compress(compression: &Compression, data: Vec<u8>) -> Result<Vec<u8>> {
    match compression {
        Compression::None => Ok(data),
        Compression::Zstd => {
            let mut encoder = ZstdEncoder::new(Vec::new(), 0)?;
            encoder.write_all(&data)?;
            Ok(encoder.finish()?)
        }
        Compression::Snappy => Ok(SnapEncoder::new()
            .compress_vec(&data)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?),
    }
}

pub fn decompress(compression: &Compression, data: Vec<u8>) -> Result<Vec<u8>> {
    match compression {
        Compression::None => Ok(data),
        Compression::Zstd => {
            let mut decoder = ZstdDecoder::new(&data[..])?;
            let mut decompressed_data = Vec::new();
            decoder
                .read_to_end(&mut decompressed_data)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            Ok(decompressed_data)
        }
        Compression::Snappy => Ok(SnapDecoder::new()
            .decompress_vec(&data)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?),
    }
}

#[repr(u64)]
#[derive(Default)]
pub enum Encoding {
    #[default]
    Bincode = 1,
    MessagePack = 2,
    Json = 3,
}

impl TryFrom<u64> for Encoding {
    type Error = ErrorCode;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Encoding::Bincode),
            2 => Ok(Encoding::MessagePack),
            3 => Ok(Encoding::Json),
            other => Err(ErrorCode::UnknownFormat(format!(
                "unsupported encoding: {}",
                other
            ))),
        }
    }
}

impl Encoding {
    pub fn as_str(&self) -> &str {
        match self {
            Encoding::Bincode => "bincode",
            Encoding::MessagePack => "messagepack",
            Encoding::Json => "json",
        }
    }
}

pub fn encode<T: Serialize>(encoding: &Encoding, data: &T) -> Result<Vec<u8>> {
    match encoding {
        Encoding::Bincode => {
            Ok(bincode::serialize(data).map_err(|e| Error::new(ErrorKind::InvalidData, e))?)
        }
        Encoding::MessagePack => {
            let mut bs = Vec::new();
            data.serialize(&mut Serializer::new(&mut bs))
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            Ok(bs)
        }
        Encoding::Json => Ok(serde_json::to_vec(&data)?),
    }
}

pub fn decode<'a, T: Deserialize<'a>>(encoding: &Encoding, data: &'a Vec<u8>) -> Result<T> {
    match encoding {
        Encoding::Bincode => {
            Ok(bincode::deserialize(data).map_err(|e| Error::new(ErrorKind::InvalidData, e))?)
        }
        Encoding::MessagePack => {
            let mut deserializer = Deserializer::new(Cursor::new(data));
            Ok(Deserialize::deserialize(&mut deserializer)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?)
        }
        Encoding::Json => Ok(from_slice::<T>(data)?),
    }
}
