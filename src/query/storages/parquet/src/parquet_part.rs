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
use std::hash::Hash;
use std::hash::Hasher;
use std::mem;
use std::sync::Arc;

use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::parquet_rs::ParquetRSRowGroupPart;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone)]
pub enum ParquetPart {
    ParquetFiles(ParquetFilesPart),
    ParquetRSRowGroup(ParquetRSRowGroupPart),
}

impl ParquetPart {
    pub fn uncompressed_size(&self) -> u64 {
        match self {
            ParquetPart::ParquetFiles(p) => p.uncompressed_size(),
            ParquetPart::ParquetRSRowGroup(p) => p.uncompressed_size(),
        }
    }

    pub fn compressed_size(&self) -> u64 {
        match self {
            ParquetPart::ParquetFiles(p) => p.compressed_size(),
            ParquetPart::ParquetRSRowGroup(p) => p.compressed_size(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct ParquetFilesPart {
    pub files: Vec<(String, u64)>,
    pub estimated_uncompressed_size: u64,
}

impl ParquetFilesPart {
    pub fn compressed_size(&self) -> u64 {
        self.files.iter().map(|(_, s)| *s).sum()
    }
    pub fn uncompressed_size(&self) -> u64 {
        self.estimated_uncompressed_size
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
            ParquetPart::ParquetFiles(p) => &p.files[0].0,
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
            .ok_or_else(|| ErrorCode::Internal("Cannot downcast from PartInfo to ParquetPart."))
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
    partitions: &mut Partitions,
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
        partitions.partitions.push(Arc::new(
            Box::new(ParquetPart::ParquetFiles(ParquetFilesPart {
                files,
                estimated_uncompressed_size,
            })) as Box<dyn PartInfo>,
        ));
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
