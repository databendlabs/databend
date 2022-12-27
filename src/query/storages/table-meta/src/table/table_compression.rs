//  Copyright 2022 Datafuse Labs.
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

use std::str::FromStr;

use common_arrow::native::Compression;
use common_arrow::parquet::compression::CompressionOptions;
use common_exception::ErrorCode;

#[derive(Clone, Copy)]
pub enum TableCompression {
    None,
    LZ4Raw,
    Snappy,
    Zstd,
}

impl Default for TableCompression {
    fn default() -> Self {
        TableCompression::LZ4Raw
    }
}

impl FromStr for TableCompression {
    type Err = ErrorCode;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "" | "none" => Ok(TableCompression::None),
            "lz4" => Ok(TableCompression::LZ4Raw),
            other => Err(ErrorCode::UnknownFormat(format!(
                "unsupported table compression: {}",
                other
            ))),
        }
    }
}

/// Convert to parquet CompressionOptions.
impl From<TableCompression> for CompressionOptions {
    fn from(value: TableCompression) -> Self {
        match value {
            TableCompression::None => CompressionOptions::Uncompressed,
            TableCompression::LZ4Raw => CompressionOptions::Lz4Raw,
            TableCompression::Snappy => CompressionOptions::Snappy,
            TableCompression::Zstd => CompressionOptions::Zstd(None),
        }
    }
}

/// Convert to native Compression.
impl From<TableCompression> for Compression {
    fn from(value: TableCompression) -> Self {
        match value {
            TableCompression::None => Compression::None,
            TableCompression::LZ4Raw => Compression::LZ4,
            // Others to ZSTD
            _ => Compression::ZSTD,
        }
    }
}
