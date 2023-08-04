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

use common_arrow::parquet::compression::Compression;
use common_arrow::parquet::indexes::Interval;
use common_catalog::plan::PartInfo;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::PartStatistics;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::FieldIndex;
use common_expression::Scalar;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ColumnMeta {
    pub offset: u64,
    pub length: u64,
    pub num_values: i64,
    pub compression: Compression,
    pub uncompressed_size: u64,
    pub min_max: Option<(Scalar, Scalar)>,

    // if has dictionary, we can not push down predicate to deserialization.
    pub has_dictionary: bool,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ParquetPart {
    RowGroup(ParquetRowGroupPart),
    SmallFiles(ParquetSmallFilesPart),
}

impl ParquetPart {
    pub fn convert_to_part_info(self) -> PartInfoPtr {
        Arc::new(Box::new(self))
    }

    pub fn uncompressed_size(&self) -> u64 {
        match self {
            ParquetPart::RowGroup(r) => r.uncompressed_size(),
            ParquetPart::SmallFiles(p) => p.uncompressed_size(),
        }
    }

    pub fn compressed_size(&self) -> u64 {
        match self {
            ParquetPart::RowGroup(r) => r.compressed_size(),
            ParquetPart::SmallFiles(p) => p.compressed_size(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
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

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ParquetRowGroupPart {
    pub location: String,
    pub num_rows: usize,
    pub column_metas: HashMap<FieldIndex, ColumnMeta>,
    pub row_selection: Option<Vec<Interval>>,

    pub sort_min_max: Option<(Scalar, Scalar)>,
}

impl ParquetRowGroupPart {
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
            ParquetPart::RowGroup(r) => &r.location,
            ParquetPart::SmallFiles(p) => &p.files[0].0,
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
