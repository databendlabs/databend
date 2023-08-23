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

use std::collections::HashMap;

use common_expression::FieldIndex;
use parquet::arrow::arrow_reader::RowSelector;
use parquet::basic::BrotliLevel;
use parquet::basic::Compression;
use parquet::basic::GzipLevel;
use parquet::basic::ZstdLevel;

/// Serializable compression types.
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Copy, Clone, Debug)]
pub enum SerdeCompression {
    Uncompressed,
    Snappy,
    Gzip(u32),
    Lzo,
    Brotli(u32),
    Lz4,
    Zstd(i32),
    Lz4Raw,
}

impl From<Compression> for SerdeCompression {
    fn from(value: Compression) -> Self {
        match value {
            Compression::UNCOMPRESSED => SerdeCompression::Uncompressed,
            Compression::SNAPPY => SerdeCompression::Snappy,
            Compression::GZIP(l) => SerdeCompression::Gzip(l.compression_level()),
            Compression::LZO => SerdeCompression::Lzo,
            Compression::BROTLI(l) => SerdeCompression::Brotli(l.compression_level()),
            Compression::LZ4 => SerdeCompression::Lz4,
            Compression::ZSTD(l) => SerdeCompression::Zstd(l.compression_level()),
            Compression::LZ4_RAW => SerdeCompression::Lz4Raw,
        }
    }
}

impl From<SerdeCompression> for Compression {
    fn from(value: SerdeCompression) -> Self {
        match value {
            SerdeCompression::Uncompressed => Compression::UNCOMPRESSED,
            SerdeCompression::Snappy => Compression::SNAPPY,
            SerdeCompression::Gzip(l) => Compression::GZIP(GzipLevel::try_new(l).unwrap()),
            SerdeCompression::Lzo => Compression::LZO,
            SerdeCompression::Brotli(l) => Compression::BROTLI(BrotliLevel::try_new(l).unwrap()),
            SerdeCompression::Lz4 => Compression::LZ4,
            SerdeCompression::Zstd(l) => Compression::ZSTD(ZstdLevel::try_new(l).unwrap()),
            SerdeCompression::Lz4Raw => Compression::LZ4_RAW,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct ColumnMeta {
    pub offset: u64,
    pub length: u64,
    pub num_values: i64,
    pub compression: SerdeCompression,
    pub uncompressed_size: u64,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct ParquetRSRowGroupPart {
    pub location: String,
    pub num_rows: usize,
    pub column_metas: HashMap<FieldIndex, ColumnMeta>,
    pub row_selection: Option<Vec<SerdeRowSelector>>,
}

impl ParquetRSRowGroupPart {
    pub fn uncompressed_size(&self) -> u64 {
        self.column_metas
            .values()
            .map(|c| c.uncompressed_size)
            .sum()
    }

    pub fn compressed_size(&self) -> u64 {
        self.column_metas.values().map(|c| c.length).sum()
    }
}

/// Serializable row selector.
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct SerdeRowSelector {
    pub row_count: usize,
    pub skip: bool,
}

impl From<RowSelector> for SerdeRowSelector {
    fn from(value: RowSelector) -> Self {
        SerdeRowSelector {
            row_count: value.row_count,
            skip: value.skip,
        }
    }
}

impl From<SerdeRowSelector> for RowSelector {
    fn from(value: SerdeRowSelector) -> Self {
        RowSelector {
            row_count: value.row_count,
            skip: value.skip,
        }
    }
}
