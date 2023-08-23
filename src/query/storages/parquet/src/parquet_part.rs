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

use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hash;
use std::hash::Hasher;
use std::mem;
use std::sync::Arc;

use common_arrow::parquet::compression::Compression as Compression2;
use common_arrow::parquet::indexes::Interval;
use common_catalog::plan::PartInfo;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::PartStatistics;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::FieldIndex;
use common_expression::Scalar;
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

impl From<Compression2> for SerdeCompression {
    fn from(value: Compression2) -> Self {
        match value {
            Compression2::Uncompressed => SerdeCompression::Uncompressed,
            Compression2::Snappy => SerdeCompression::Snappy,
            Compression2::Gzip => SerdeCompression::Gzip(6),
            Compression2::Lzo => SerdeCompression::Lzo,
            Compression2::Brotli => SerdeCompression::Brotli(1),
            Compression2::Lz4 => SerdeCompression::Lz4,
            Compression2::Zstd => SerdeCompression::Zstd(1),
            Compression2::Lz4Raw => SerdeCompression::Lz4Raw,
        }
    }
}

impl From<SerdeCompression> for Compression2 {
    fn from(value: SerdeCompression) -> Self {
        match value {
            SerdeCompression::Uncompressed => Compression2::Uncompressed,
            SerdeCompression::Snappy => Compression2::Snappy,
            SerdeCompression::Gzip(_) => Compression2::Gzip,
            SerdeCompression::Lzo => Compression2::Lzo,
            SerdeCompression::Brotli(_) => Compression2::Brotli,
            SerdeCompression::Lz4 => Compression2::Lz4,
            SerdeCompression::Zstd(_) => Compression2::Zstd,
            SerdeCompression::Lz4Raw => Compression2::Lz4Raw,
        }
    }
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
    pub min_max: Option<(Scalar, Scalar)>,

    // if has dictionary, we can not push down predicate to deserialization.
    pub has_dictionary: bool,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone)]
pub enum ParquetPart {
    Parquet2RowGroup(Parquet2RowGroupPart),
    SmallFiles(ParquetSmallFilesPart),
    ParquetRSRowGroup(ParquetRSRowGroupPart),
}

impl ParquetPart {
    pub fn convert_to_part_info(self) -> PartInfoPtr {
        Arc::new(Box::new(self))
    }

    pub fn uncompressed_size(&self) -> u64 {
        match self {
            ParquetPart::Parquet2RowGroup(r) => r.uncompressed_size(),
            ParquetPart::SmallFiles(p) => p.uncompressed_size(),
            ParquetPart::ParquetRSRowGroup(p) => p.uncompressed_size(),
        }
    }

    pub fn compressed_size(&self) -> u64 {
        match self {
            ParquetPart::Parquet2RowGroup(r) => r.compressed_size(),
            ParquetPart::SmallFiles(p) => p.compressed_size(),
            ParquetPart::ParquetRSRowGroup(p) => p.compressed_size(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct ParquetSmallFilesPart {
    pub files: Vec<(String, u64)>,
    pub estimated_uncompressed_size: u64,
}

impl ParquetSmallFilesPart {
    pub fn compressed_size(&self) -> u64 {
        self.files.iter().map(|(_, s)| *s).sum()
    }
    pub fn uncompressed_size(&self) -> u64 {
        self.estimated_uncompressed_size
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct Parquet2RowGroupPart {
    pub location: String,
    pub num_rows: usize,
    pub column_metas: HashMap<FieldIndex, ColumnMeta>,
    pub row_selection: Option<Vec<Interval>>,

    pub sort_min_max: Option<(Scalar, Scalar)>,
}

impl Parquet2RowGroupPart {
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

#[typetag::serde(name = "parquet_part")]
impl PartInfo for ParquetPart {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<ParquetPart>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        let path = match self {
            ParquetPart::Parquet2RowGroup(r) => &r.location,
            ParquetPart::SmallFiles(p) => &p.files[0].0,
            ParquetPart::ParquetRSRowGroup(p) => &p.location,
        };
        let mut s = DefaultHasher::new();
        path.hash(&mut s);
        s.finish()
    }
}

impl ParquetPart {
    pub fn from_part(info: &PartInfoPtr) -> Result<&ParquetPart> {
        info.as_any()
            .downcast_ref::<ParquetPart>()
            .ok_or(ErrorCode::Internal(
                "Cannot downcast from PartInfo to ParquetPart.",
            ))
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

/// files smaller than setting fast_read_part_bytes is small file,
/// which is load in one read, without reading its meta in advance.
/// some considerations:
/// 1. to fully utilize the IO, multiple small files are loaded in one part.
/// 2. to avoid OOM, the total size of small files in one part is limited,
///    and we need compression_ratio to estimate the uncompressed size.
pub(crate) fn collect_small_file_parts(
    small_files: Vec<(String, u64)>,
    mut max_compression_ratio: f64,
    mut max_compressed_size: u64,
    partitions: &mut Vec<ParquetPart>,
    stats: &mut PartStatistics,
    num_columns_to_read: usize,
) {
    if max_compression_ratio <= 0.0 || max_compression_ratio >= 1.0 {
        // just incase
        max_compression_ratio = 1.0;
    }
    if max_compressed_size == 0 {
        // there are no large files, so we choose a default value.
        max_compressed_size = ((128usize << 20) as f64 / max_compression_ratio) as u64;
    }
    let mut num_small_files = small_files.len();
    stats.read_rows += num_small_files;
    let mut small_part = vec![];
    let mut part_size = 0;
    let mut make_small_files_part = |files: Vec<(String, u64)>, part_size| {
        let estimated_uncompressed_size = (part_size as f64 / max_compression_ratio) as u64;
        num_small_files -= files.len();
        partitions.push(ParquetPart::SmallFiles(ParquetSmallFilesPart {
            files,
            estimated_uncompressed_size,
        }));
        stats.partitions_scanned += 1;
        stats.partitions_total += 1;
    };
    let max_files = num_columns_to_read * 2;
    for (path, size) in small_files.into_iter() {
        stats.read_bytes += size as usize;
        if !small_part.is_empty()
            && (part_size + size > max_compressed_size || small_part.len() + 1 >= max_files)
        {
            make_small_files_part(mem::take(&mut small_part), part_size);
            part_size = 0;
        }
        small_part.push((path, size));
        part_size += size;
    }
    if !small_part.is_empty() {
        make_small_files_part(mem::take(&mut small_part), part_size);
    }
    assert_eq!(num_small_files, 0);
}
