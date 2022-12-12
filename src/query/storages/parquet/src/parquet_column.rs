// Copyright 2022 Datafuse Labs.
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

use common_arrow::parquet::compression::Compression as ParquetCompression;

#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy, serde::Deserialize, serde::Serialize)]
pub enum Compression {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    Zstd,
    Lz4Raw,
}

impl From<Compression> for ParquetCompression {
    fn from(value: Compression) -> ParquetCompression {
        match value {
            Compression::Uncompressed => ParquetCompression::Uncompressed,
            Compression::Snappy => ParquetCompression::Snappy,
            Compression::Gzip => ParquetCompression::Gzip,
            Compression::Lzo => ParquetCompression::Lzo,
            Compression::Brotli => ParquetCompression::Brotli,
            Compression::Lz4 => ParquetCompression::Lz4,
            Compression::Zstd => ParquetCompression::Zstd,
            Compression::Lz4Raw => ParquetCompression::Lz4Raw,
        }
    }
}

impl From<ParquetCompression> for Compression {
    fn from(value: ParquetCompression) -> Self {
        match value {
            ParquetCompression::Uncompressed => Compression::Uncompressed,
            ParquetCompression::Snappy => Compression::Snappy,
            ParquetCompression::Gzip => Compression::Gzip,
            ParquetCompression::Lzo => Compression::Lzo,
            ParquetCompression::Brotli => Compression::Brotli,
            ParquetCompression::Lz4 => Compression::Lz4,
            ParquetCompression::Zstd => Compression::Zstd,
            ParquetCompression::Lz4Raw => Compression::Lz4Raw,
        }
    }
}
