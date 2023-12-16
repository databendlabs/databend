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

use databend_common_expression::ColumnId;

use super::statistics::*;

/// A segment comprised of one or more blocks
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct SegmentInfo {
    /// blocks belong to this segment
    pub blocks: Vec<BlockMeta>,
    /// summary statistics
    pub summary: Statistics,
}

/// Meta information of a block (currently, the parquet file)
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct BlockMeta {
    /// Pointer of the data Block
    pub row_count: u64,
    pub block_size: u64,
    pub file_size: u64,
    pub col_stats: HashMap<ColumnId, ColumnStatistics>,
    pub col_metas: HashMap<ColumnId, ColumnMeta>,
    pub location: BlockLocation,
}

// TODO move it to common
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct ColumnMeta {
    /// where the data of column start
    pub offset: u64,
    /// the length of the column
    pub len: u64,
    /// num of "rows"
    pub num_values: u64,
}

impl ColumnMeta {
    pub fn new(offset: u64, len: u64, num_values: u64) -> Self {
        Self {
            offset,
            len,
            num_values,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct BlockLocation {
    pub path: String,
    // for parquet, this field can be used to fetch the meta data without seeking around
    pub meta_size: u64,
}
