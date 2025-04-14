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

use databend_common_expression::converts::meta::LegacyScalar;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use serde::Deserialize;
use serde::Serialize;

use super::statistics::ColumnStatistics;
use crate::meta::Location;

#[derive(Serialize, Deserialize)]
pub struct BlockMeta {
    pub row_count: u64,
    pub block_size: u64,
    pub file_size: u64,
    pub col_stats: HashMap<ColumnId, ColumnStatistics>,
    pub col_metas: HashMap<ColumnId, ColumnMeta>,
    pub cluster_stats: Option<ClusterStatistics>,
    /// location of data block
    pub location: Location,
    /// location of bloom filter index
    pub bloom_filter_index_location: Option<Location>,

    #[serde(default)]
    pub bloom_filter_index_size: u64,
    pub compression: Compression,
}

impl From<BlockMeta> for crate::meta::BlockMeta {
    fn from(value: BlockMeta) -> Self {
        Self {
            row_count: value.row_count,
            block_size: value.block_size,
            file_size: value.file_size,
            col_stats: value
                .col_stats
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            col_metas: value
                .col_metas
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            cluster_stats: value.cluster_stats.map(|v| v.into()),
            location: value.location,
            bloom_filter_index_location: value.bloom_filter_index_location,
            ngram_filter_index_location: None,
            bloom_filter_index_size: value.bloom_filter_index_size,
            inverted_index_size: None,
            ngram_filter_index_size: None,
            compression: value.compression.into(),
            create_on: None,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum Compression {
    // Lz4 will be deprecated.
    Lz4,
    Lz4Raw,
    Snappy,
    Zstd,
    Gzip,
    // New: Added by bohu.
    None,
}

impl From<Compression> for crate::meta::Compression {
    fn from(value: Compression) -> Self {
        match value {
            Compression::Lz4 => Self::Lz4,
            Compression::Lz4Raw => Self::Lz4Raw,
            Compression::Snappy => Self::Snappy,
            Compression::Zstd => Self::Zstd,
            Compression::Gzip => Self::Gzip,
            Compression::None => Self::None,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum ColumnMeta {
    Parquet(ParquetColumnMeta),
    Native(NativeColumnMeta),
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct NativeColumnMeta {
    pub offset: u64,
    pub pages: Vec<PageMeta>,
}

impl From<NativeColumnMeta> for databend_common_native::ColumnMeta {
    fn from(value: NativeColumnMeta) -> Self {
        Self {
            offset: value.offset,
            pages: value.pages.into_iter().map(|v| v.into()).collect(),
        }
    }
}

impl From<PageMeta> for databend_common_native::PageMeta {
    fn from(value: PageMeta) -> Self {
        Self {
            length: value.length,
            num_values: value.num_values,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct PageMeta {
    // compressed size of this page
    pub length: u64,
    // num values(rows) of this page
    pub num_values: u64,
}

impl From<ColumnMeta> for crate::meta::ColumnMeta {
    fn from(value: ColumnMeta) -> Self {
        match value {
            ColumnMeta::Parquet(v) => Self::Parquet(v.into()),
            ColumnMeta::Native(v) => Self::Native(v.into()),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ParquetColumnMeta {
    /// where the data of column start
    pub offset: u64,
    /// the length of the column
    pub len: u64,
    /// num of "rows"
    pub num_values: u64,
}

impl From<ParquetColumnMeta> for crate::meta::v0::ColumnMeta {
    fn from(value: ParquetColumnMeta) -> Self {
        Self {
            offset: value.offset,
            len: value.len,
            num_values: value.num_values,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ClusterStatistics {
    pub cluster_key_id: u32,
    pub min: Vec<LegacyScalar>,
    pub max: Vec<LegacyScalar>,
    pub level: i32,

    // currently it's only used in native engine
    pub pages: Option<Vec<LegacyScalar>>,
}

impl From<ClusterStatistics> for crate::meta::ClusterStatistics {
    fn from(value: ClusterStatistics) -> Self {
        let min: Vec<_> = value.min.iter().map(|c| Scalar::from(c.clone())).collect();

        let max: Vec<_> = value.max.iter().map(|c| Scalar::from(c.clone())).collect();

        let pages = value
            .pages
            .map(|pages| pages.into_iter().map(Scalar::from).collect());

        Self {
            cluster_key_id: value.cluster_key_id,
            min,
            max,
            level: value.level,
            pages,
        }
    }
}
