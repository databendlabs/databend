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

use std::str::FromStr;

use chrono_tz::Tz;
use common_exception::ErrorCode;
use common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq)]
pub struct FormatSettings {
    pub record_delimiter: Vec<u8>,
    pub field_delimiter: Vec<u8>,
    pub empty_as_default: bool,
    pub skip_header: bool,
    pub compression: Compression,
    pub timezone: Tz,
    pub true_bytes: Vec<u8>,
    pub false_bytes: Vec<u8>,
}

impl Default for FormatSettings {
    fn default() -> Self {
        Self {
            record_delimiter: vec![b'\n'],
            field_delimiter: vec![b','],
            empty_as_default: false,
            skip_header: false,
            compression: Compression::None,
            timezone: "UTC".parse::<Tz>().unwrap(),
            true_bytes: vec![b't', b'r', b'u', b'e'],
            false_bytes: vec![b'f', b'a', b'l', b's', b'e'],
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
pub enum Compression {
    None,
    Auto,
    /// Deflate with gzip headers.
    Gzip,
    Bz2,
    Brotli,
    Zstd,
    /// Deflate with zlib headers
    Deflate,
    /// Raw default stream without any headers.
    RawDeflate,
    Lzo,
    Snappy,
}

impl Default for Compression {
    fn default() -> Self {
        Self::None
    }
}

impl FromStr for Compression {
    type Err = ErrorCode;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "auto" => Ok(Compression::Auto),
            "gzip" => Ok(Compression::Gzip),
            "bz2" => Ok(Compression::Bz2),
            "brotli" => Ok(Compression::Brotli),
            "zstd" => Ok(Compression::Zstd),
            "deflate" => Ok(Compression::Deflate),
            "rawdeflate" | "raw_deflate" => Ok(Compression::RawDeflate),
            "lzo" => Ok(Compression::Lzo),
            "snappy" => Ok(Compression::Snappy),
            "none" => Ok(Compression::None),
            _ => Err(ErrorCode::UnknownCompressionType(format!(
                "Unknown compression: {s}"
            ))),
        }
    }
}
