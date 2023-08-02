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
use std::sync::Arc;

use common_expression::ColumnId;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::v0::statistics::ClusterStatistics;
use crate::meta::v0::statistics::ColumnStatistics;
use crate::meta::v0::statistics::Statistics;
use crate::meta::v0::ColumnMeta;
use crate::meta::Compression;
use crate::meta::FormatVersion;
use crate::meta::Location;
use crate::meta::Versioned;

/// A segment comprises one or more blocks
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct SegmentInfo {
    /// format version
    pub format_version: FormatVersion,
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

    /// Compression algo used to compress the columns of blocks
    ///
    /// If not specified, the legacy algo `Lz4` will be used.
    /// `Lz4` is merely for backward compatibility, it will NO longer be
    /// used in the write path.
    #[serde(default = "Compression::legacy")]
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

impl SegmentInfo {
    pub fn new(blocks: Vec<Arc<BlockMeta>>, summary: Statistics) -> Self {
        Self {
            format_version: SegmentInfo::VERSION,
            blocks,
            summary,
        }
    }
}

use super::super::v0;

impl From<v0::SegmentInfo> for SegmentInfo {
    fn from(s: v0::SegmentInfo) -> Self {
        Self {
            // the is no version before v0, and no versions other then 0 can be converted into v0
            format_version: v0::SegmentInfo::VERSION,
            blocks: s
                .blocks
                .into_iter()
                .map(|b| Arc::new(b.into()))
                .collect::<_>(),
            summary: s.summary,
        }
    }
}

impl From<v0::BlockMeta> for BlockMeta {
    fn from(s: v0::BlockMeta) -> Self {
        Self {
            row_count: s.row_count,
            block_size: s.block_size,
            file_size: s.file_size,
            col_stats: s.col_stats,
            col_metas: s.col_metas,
            cluster_stats: None,
            location: (s.location.path, 0),
            bloom_filter_index_location: None,
            bloom_filter_index_size: 0,
            compression: Compression::Lz4Raw,
        }
    }
}
