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
    ParquetFile(ParquetFilePart),
    ParquetRSRowGroup(ParquetRSRowGroupPart),
}

impl ParquetPart {
    pub fn uncompressed_size(&self) -> u64 {
        match self {
            ParquetPart::ParquetFile(p) => p.uncompressed_size(),
            ParquetPart::ParquetRSRowGroup(p) => p.uncompressed_size(),
        }
    }

    pub fn compressed_size(&self) -> u64 {
        match self {
            ParquetPart::ParquetFile(p) => p.compressed_size(),
            ParquetPart::ParquetRSRowGroup(p) => p.compressed_size(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct ParquetFilePart {
    pub file: String,
    pub compressed_size: u64,
    pub estimated_uncompressed_size: u64,
    pub dedup_key: String,
}

impl ParquetFilePart {
    pub fn compressed_size(&self) -> u64 {
        self.compressed_size
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
            ParquetPart::ParquetFile(p) => &p.file,
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

pub(crate) fn collect_file_parts(
    files: Vec<(String, u64, String)>,
    compress_ratio: f64,
    partitions: &mut Partitions,
    stats: &mut PartStatistics,
    num_columns_to_read: usize,
    total_columns_to_read: usize,
) {
    for (file, size, dedup_key) in files.into_iter() {
        stats.read_bytes += size as usize;
        let estimated_read_rows: f64 = size as f64 / (total_columns_to_read * 8) as f64;
        let read_bytes =
            (size as f64) * (num_columns_to_read as f64) / (total_columns_to_read as f64);

        let estimated_uncompressed_size = read_bytes * compress_ratio;

        partitions.partitions.push(Arc::new(
            Box::new(ParquetPart::ParquetFile(ParquetFilePart {
                file,
                compressed_size: size,
                estimated_uncompressed_size: estimated_uncompressed_size as u64,
                dedup_key,
            })) as Box<dyn PartInfo>,
        ));

        stats.read_bytes += read_bytes as usize;
        stats.read_rows += estimated_read_rows as usize;
        stats.is_exact = false;
        stats.partitions_scanned += 1;
        stats.partitions_total += 1;
    }
}
