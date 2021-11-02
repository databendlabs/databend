// Copyright 2020 Datafuse Labs.
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

use common_base::uuid;
use common_datavalues::DataSchema;
use common_datavalues::DataValue;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

pub type SnapshotId = Uuid; // TODO String might be better
pub type ColumnId = u32;
pub type Location = String;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableSnapshot {
    // TODO format_version
    // pub format_version: u32,
    /// id of snapshot
    pub snapshot_id: SnapshotId,

    pub prev_snapshot_id: Option<SnapshotId>,

    /// For each snapshot, we keep a schema for it (in case of schema evolution)
    pub schema: DataSchema,

    /// Summary Statistics
    pub summary: Stats,

    /// Pointers to SegmentInfos (may be of different format)
    ///
    /// We rely on background merge tasks to keep merging segments, so that
    /// this the size of this vector could be kept reasonable
    pub segments: Vec<Location>,
}

impl TableSnapshot {
    pub fn append_segment(mut self, location: Location) -> TableSnapshot {
        self.segments.push(location);
        self
    }
}

/// A segment comprised of one or more blocks
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct SegmentInfo {
    // TODO format_version
    // pub format_version: u32,
    /// blocks belong to this segment
    pub blocks: Vec<BlockMeta>,

    /// summary statistics
    pub summary: Stats,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct Stats {
    pub row_count: u64,
    pub block_count: u64,

    // TODO rename these two fields
    pub uncompressed_byte_size: u64,
    pub compressed_byte_size: u64,

    pub col_stats: HashMap<ColumnId, ColStats>,
}

/// Meta information of a block (currently, the parquet file)
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct BlockMeta {
    /// Pointer of the data Block
    pub row_count: u64,
    pub block_size: u64,
    pub col_stats: HashMap<ColumnId, ColStats>,
    pub location: BlockLocation,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct BlockLocation {
    pub location: Location,
    // for parquet, this filed can be used to fetch the meta data without seeking around
    pub meta_size: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct ColStats {
    pub min: DataValue,
    pub max: DataValue,
    pub null_count: usize,
}
