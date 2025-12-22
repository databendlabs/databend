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
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::partition::ParquetRowGroupPart;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone)]
pub enum DeleteType {
    Position,
    Equality,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct DeleteTask {
    pub path: String,
    pub ty: DeleteType,
    /// equality ids for equality deletes (empty for positional deletes)
    pub equality_ids: Vec<i32>,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone)]
pub enum ParquetPart {
    File(ParquetFilePart),
    SmallFiles(Vec<ParquetFilePart>),
    RowGroup(ParquetRowGroupPart),
    FileWithDeletes {
        inner: ParquetFilePart,
        deletes: Vec<DeleteTask>,
    },
}

impl ParquetPart {
    pub fn uncompressed_size(&self) -> u64 {
        match self {
            ParquetPart::File(p) => p.uncompressed_size(),
            ParquetPart::SmallFiles(p) => p.iter().map(|p| p.uncompressed_size()).sum(),
            ParquetPart::RowGroup(p) => p.uncompressed_size(),
            ParquetPart::FileWithDeletes { inner, deletes: _ } => inner.uncompressed_size(),
        }
    }

    pub fn compressed_size(&self) -> u64 {
        match self {
            ParquetPart::File(p) => p.compressed_size(),
            ParquetPart::SmallFiles(p) => p.iter().map(|p| p.compressed_size()).sum(),
            ParquetPart::RowGroup(p) => p.compressed_size(),
            ParquetPart::FileWithDeletes { inner, deletes: _ } => inner.compressed_size(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct ParquetFilePart {
    pub file: String,
    pub compressed_size: u64,
    pub estimated_uncompressed_size: u64,
    // used to cache parquet metadata
    pub dedup_key: String,

    // For large parquet files, we will split the file into multiple parts
    // But we don't read metadata during plan stage, so we split them by 128MB into buckets
    // (bucket_idx, bucket_num)
    pub bucket_option: Option<(usize, usize)>,
}

impl ParquetFilePart {
    pub fn compressed_size(&self) -> u64 {
        match self.bucket_option {
            Some((_, num)) => self.compressed_size / num as u64,
            None => self.compressed_size,
        }
    }
    pub fn uncompressed_size(&self) -> u64 {
        match self.bucket_option {
            Some((_, num)) => self.estimated_uncompressed_size / num as u64,
            None => self.estimated_uncompressed_size,
        }
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
        let paths = match self {
            ParquetPart::File(p) => vec![&p.file],
            ParquetPart::SmallFiles(p) => {
                let mut s = DefaultHasher::new();
                for part in p.iter() {
                    part.file.hash(&mut s);
                }
                return s.finish();
            }
            ParquetPart::RowGroup(p) => vec![&p.location],
            ParquetPart::FileWithDeletes { inner, deletes } => {
                let mut paths = Vec::with_capacity(deletes.len() + 1);
                paths.push(&inner.file);
                for delete in deletes {
                    paths.push(&delete.path);
                }
                paths
            }
        };
        let mut s = DefaultHasher::new();
        paths.hash(&mut s);
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
fn collect_small_file_parts(
    small_files: Vec<(String, u64, String)>,
    mut max_compression_ratio: f64,
    mut max_compressed_size: u64,
    partitions: &mut Partitions,
    stats: &mut PartStatistics,
) {
    if max_compression_ratio <= 0.0 || max_compression_ratio >= 1.0 {
        // just incase
        max_compression_ratio = 1.0;
    }
    if max_compressed_size == 0 {
        // there are no large files, so we choose a default value.
        max_compressed_size = 128u64 << 20;
    }
    let num_small_files = small_files.len();
    stats.read_rows += num_small_files;
    let mut small_part = vec![];
    let mut part_size = 0;

    let total_len = small_files.len();
    let max_files = 8;

    for (i, (path, size, dedup_key)) in small_files.into_iter().enumerate() {
        small_part.push((path.clone(), size, dedup_key));
        stats.read_bytes += size as usize;
        part_size += size;
        let is_last = i + 1 == total_len;

        if !small_part.is_empty()
            && (part_size > max_compressed_size || small_part.len() >= max_files || is_last)
        {
            let files = small_part
                .iter()
                .cloned()
                .map(|(path, size, dedup_key)| ParquetFilePart {
                    file: path,
                    compressed_size: size,
                    estimated_uncompressed_size: (size as f64 * max_compression_ratio) as u64,
                    dedup_key,
                    bucket_option: None,
                })
                .collect::<Vec<_>>();

            partitions.partitions.push(Arc::new(
                Box::new(ParquetPart::SmallFiles(files)) as Box<dyn PartInfo>
            ));
            stats.partitions_scanned += 1;
            stats.partitions_total += 1;
            part_size = 0;
            small_part.clear();
        }
    }
}

fn collect_file_parts(
    files: Vec<(String, u64, String)>,
    compress_ratio: f64,
    partitions: &mut Partitions,
    stats: &mut PartStatistics,
    num_columns_to_read: usize,
    total_columns_to_read: usize,
    rowgroup_hint_bytes: u64,
) {
    for (file, size, dedup_key) in files.into_iter() {
        stats.read_bytes += size as usize;
        let estimated_read_rows: f64 = size as f64 / (total_columns_to_read * 8) as f64;
        let read_bytes =
            (size as f64) * (num_columns_to_read as f64) / (total_columns_to_read as f64);

        let estimated_uncompressed_size = read_bytes * compress_ratio;
        let bucket_num = size.div_ceil(rowgroup_hint_bytes) as usize;
        for bucket in 0..bucket_num {
            partitions
                .partitions
                .push(Arc::new(Box::new(ParquetPart::File(ParquetFilePart {
                    file: file.clone(),
                    compressed_size: size,
                    estimated_uncompressed_size: estimated_uncompressed_size as u64,
                    dedup_key: dedup_key.clone(),
                    bucket_option: Some((bucket, bucket_num)),
                })) as Box<dyn PartInfo>));

            stats.partitions_scanned += 1;
            stats.partitions_total += 1;
        }

        stats.read_bytes += read_bytes as usize;
        stats.read_rows += estimated_read_rows as usize;
        stats.is_exact = false;
    }
}

pub(crate) fn collect_parts(
    ctx: Arc<dyn TableContext>,
    files: Vec<(String, u64, String)>,
    compression_ratio: f64,
    num_columns_to_read: usize,
    total_columns_to_read: usize,
) -> Result<(PartStatistics, Partitions)> {
    let mut partitions = Partitions::default();
    let mut stats = PartStatistics::default();

    let fast_read_bytes = ctx.get_settings().get_parquet_fast_read_bytes()?;
    let rowgroup_hint_bytes = ctx.get_settings().get_parquet_rowgroup_hint_bytes()?;

    let mut large_files = vec![];
    let mut small_files = vec![];
    for (location, size, dedup_key) in files.into_iter() {
        if size > fast_read_bytes {
            large_files.push((location, size, dedup_key));
        } else if size > 0 {
            small_files.push((location, size, dedup_key));
        }
    }

    collect_file_parts(
        large_files,
        compression_ratio,
        &mut partitions,
        &mut stats,
        num_columns_to_read,
        total_columns_to_read,
        rowgroup_hint_bytes,
    );

    if !small_files.is_empty() {
        let mut max_compressed_size = 0u64;
        for part in partitions.partitions.iter() {
            let p = part.as_any().downcast_ref::<ParquetPart>().unwrap();
            max_compressed_size = max_compressed_size.max(p.compressed_size());
        }

        collect_small_file_parts(
            small_files,
            compression_ratio,
            max_compressed_size,
            &mut partitions,
            &mut stats,
        );
    }

    Ok((stats, partitions))
}
