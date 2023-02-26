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

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use common_arrow::native::ColumnMeta as NativeColumnMeta;
use common_expression::ColumnId;
use common_expression::TableField;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::statistics::ClusterStatistics;
use crate::meta::statistics::ColumnStatistics;
use crate::meta::statistics::FormatVersion;
use crate::meta::Compression;
use crate::meta::Location;
use crate::meta::Statistics;
use crate::meta::Versioned;

/// A segment comprises one or more blocks
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct SegmentInfo {
    /// format version
    format_version: FormatVersion,
    /// blocks belong to this segment
    pub blocks: Vec<Arc<BlockMeta>>,
    /// summary statistics
    pub summary: Statistics,
}

/// Meta information of a block
/// Part of and kept inside the [SegmentInfo]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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

impl BlockMeta {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        row_count: u64,
        block_size: u64,
        file_size: u64,
        col_stats: HashMap<ColumnId, ColumnStatistics>,
        col_metas: HashMap<ColumnId, ColumnMeta>,
        cluster_stats: Option<ClusterStatistics>,
        location: Location,
        bloom_filter_index_location: Option<Location>,
        bloom_filter_index_size: u64,
        compression: Compression,
    ) -> Self {
        Self {
            row_count,
            block_size,
            file_size,
            col_stats,
            col_metas,
            cluster_stats,
            location,
            bloom_filter_index_location,
            bloom_filter_index_size,
            compression,
        }
    }

    pub fn compression(&self) -> Compression {
        self.compression
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, EnumAsInner)]
pub enum ColumnMeta {
    Parquet(v0::ColumnMeta),
    Native(NativeColumnMeta),
}

impl ColumnMeta {
    pub fn total_rows(&self) -> usize {
        match self {
            ColumnMeta::Parquet(v) => v.num_values as usize,
            ColumnMeta::Native(v) => v.pages.iter().map(|page| page.num_values as usize).sum(),
        }
    }

    pub fn offset_length(&self) -> (u64, u64) {
        match self {
            ColumnMeta::Parquet(v) => (v.offset, v.len),
            ColumnMeta::Native(v) => (v.offset, v.pages.iter().map(|page| page.length).sum()),
        }
    }

    pub fn read_rows(&self, range: &Option<Range<usize>>) -> u64 {
        match self {
            ColumnMeta::Parquet(v) => v.num_values,
            ColumnMeta::Native(v) => match range {
                Some(range) => v
                    .pages
                    .iter()
                    .skip(range.start)
                    .take(range.end - range.start)
                    .map(|page| page.num_values)
                    .sum(),
                None => v.pages.iter().map(|page| page.num_values).sum(),
            },
        }
    }

    pub fn read_bytes(&self, range: &Option<Range<usize>>) -> u64 {
        match self {
            ColumnMeta::Parquet(v) => v.len,
            ColumnMeta::Native(v) => match range {
                Some(range) => v
                    .pages
                    .iter()
                    .skip(range.start)
                    .take(range.end - range.start)
                    .map(|page| page.length)
                    .sum(),
                None => v.pages.iter().map(|page| page.length).sum(),
            },
        }
    }
}

impl SegmentInfo {
    pub fn new(blocks: Vec<Arc<BlockMeta>>, summary: Statistics) -> Self {
        Self {
            format_version: SegmentInfo::VERSION,
            blocks,
            summary,
        }
    }

    pub fn format_version(&self) -> u64 {
        self.format_version
    }

    // Total block bytes of this segment.
    pub fn total_bytes(&self) -> u64 {
        self.blocks.iter().map(|v| v.block_size).sum()
    }
}

use super::super::v0;
use super::super::v1;

impl SegmentInfo {
    pub fn from_v0(s: v0::SegmentInfo, fields: &[TableField]) -> Self {
        let summary = Statistics::from_v0(s.summary, fields);
        Self {
            format_version: SegmentInfo::VERSION,
            blocks: s
                .blocks
                .into_iter()
                .map(|b| Arc::new(BlockMeta::from_v0(&b, fields)))
                .collect::<_>(),
            summary,
        }
    }

    pub fn from_v1(s: v1::SegmentInfo, fields: &[TableField]) -> Self {
        let summary = Statistics::from_v0(s.summary, fields);
        Self {
            format_version: SegmentInfo::VERSION,
            blocks: s
                .blocks
                .into_iter()
                .map(|b| Arc::new(BlockMeta::from_v1(b.as_ref(), fields)))
                .collect::<_>(),
            summary,
        }
    }
}

impl BlockMeta {
    pub fn from_v0(s: &v0::BlockMeta, fields: &[TableField]) -> Self {
        let col_stats = s
            .col_stats
            .iter()
            .map(|(k, v)| {
                let data_type = fields[*k as usize].data_type();
                (*k, ColumnStatistics::from_v0(v, data_type))
            })
            .collect();

        let col_metas = s
            .col_metas
            .iter()
            .map(|(k, v)| (*k, ColumnMeta::Parquet(v.clone())))
            .collect();

        Self {
            row_count: s.row_count,
            block_size: s.block_size,
            file_size: s.file_size,
            col_stats,
            col_metas,
            cluster_stats: None,
            location: (s.location.path.clone(), 0),
            bloom_filter_index_location: None,
            bloom_filter_index_size: 0,
            compression: Compression::Lz4,
        }
    }

    pub fn from_v1(s: &v1::BlockMeta, fields: &[TableField]) -> Self {
        let col_stats = s
            .col_stats
            .iter()
            .map(|(k, v)| {
                let data_type = fields[*k as usize].data_type();
                (*k, ColumnStatistics::from_v0(v, data_type))
            })
            .collect();

        let col_metas = s
            .col_metas
            .iter()
            .map(|(k, v)| (*k, ColumnMeta::Parquet(v.clone())))
            .collect();

        Self {
            row_count: s.row_count,
            block_size: s.block_size,
            file_size: s.file_size,
            col_stats,
            col_metas,
            cluster_stats: None,
            location: s.location.clone(),
            bloom_filter_index_location: s.bloom_filter_index_location.clone(),
            bloom_filter_index_size: s.bloom_filter_index_size,
            compression: s.compression,
        }
    }
}
