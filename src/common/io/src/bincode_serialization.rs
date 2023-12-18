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

use databend_common_exception::Result;

pub enum BincodeConfig {
    // Little endian
    Legacy,
    // Big endian
    Standard,
}

/// bincode serialize_into wrap with optimized config
#[inline]
pub fn bincode_serialize_into_buf<W: std::io::Write, T: serde::Serialize>(
    writer: &mut W,
    value: &T,
) -> Result<()> {
    bincode_serialize_into_buf_with_config(writer, value, BincodeConfig::Standard)
}

/// bincode deserialize_from wrap with optimized config
#[inline]
pub fn bincode_deserialize_from_slice<T: serde::de::DeserializeOwned>(slice: &[u8]) -> Result<T> {
    bincode_deserialize_from_slice_with_config(slice, BincodeConfig::Standard)
}

#[inline]
pub fn bincode_deserialize_from_stream<T: serde::de::DeserializeOwned>(
    stream: &mut &[u8],
) -> Result<T> {
    bincode_deserialize_from_stream_with_config(stream, BincodeConfig::Standard)
}

#[inline]
pub fn bincode_serialize_into_buf_with_config<W: std::io::Write, T: serde::Serialize>(
    writer: &mut W,
    value: &T,
    config: BincodeConfig,
) -> Result<()> {
    match config {
        BincodeConfig::Legacy => {
            bincode::serde::encode_into_std_write(value, writer, bincode::config::legacy())?
        }
        BincodeConfig::Standard => {
            bincode::serde::encode_into_std_write(value, writer, bincode::config::standard())?
        }
    };

    Ok(())
}

#[inline]
pub fn bincode_deserialize_from_slice_with_config<T: serde::de::DeserializeOwned>(
    slice: &[u8],
    config: BincodeConfig,
) -> Result<T> {
    let (value, _bytes_read) = match config {
        BincodeConfig::Legacy => {
            bincode::serde::decode_from_slice(slice, bincode::config::legacy())?
        }
        BincodeConfig::Standard => {
            bincode::serde::decode_from_slice(slice, bincode::config::standard())?
        }
    };

    Ok(value)
}

#[inline]
pub fn bincode_deserialize_from_stream_with_config<T: serde::de::DeserializeOwned>(
    stream: &mut &[u8],
    config: BincodeConfig,
) -> Result<T> {
    let (value, bytes_read) = match config {
        BincodeConfig::Legacy => {
            bincode::serde::decode_from_slice(stream, bincode::config::legacy())?
        }
        BincodeConfig::Standard => {
            bincode::serde::decode_from_slice(stream, bincode::config::standard())?
        }
    };

    // Update the slice to point to the remaining bytes.
    *stream = &stream[bytes_read..];

    Ok(value)
}
