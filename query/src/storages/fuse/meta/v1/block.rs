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

use crate::storages::fuse::meta::v0::ColumnMeta;
use crate::storages::fuse::meta::ColumnId;
use crate::storages::index::ColumnStatistics;

/// Meta information of a block
/// Kept inside the [SegmentInfo]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct BlockMeta {
    pub row_count: u64,
    pub block_size: u64,
    pub file_size: u64,
    pub col_stats: HashMap<ColumnId, ColumnStatistics>,
    pub col_metas: HashMap<ColumnId, ColumnMeta>,
    pub location: BlockLocation,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct BlockLocation {
    pub path: String,
    pub format_version: u64,
}

impl BlockLocation {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            format_version: 1,
        }
    }
}

use super::super::v0::block::BlockMeta as BlockMetaV0;
impl From<BlockMetaV0> for BlockMeta {
    fn from(s: BlockMetaV0) -> Self {
        Self {
            row_count: s.row_count,
            block_size: s.block_size,
            file_size: s.file_size,
            col_stats: s.col_stats,
            col_metas: s.col_metas,
            location: BlockLocation {
                path: s.location.path,
                format_version: 1,
            },
        }
    }
}
