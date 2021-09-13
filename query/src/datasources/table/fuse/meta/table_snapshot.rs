//  Copyright 2021 Datafuse Labs.
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
//

use std::collections::HashMap;

use common_arrow::parquet::statistics::Statistics;
use common_datavalues::DataSchema;
use common_datavalues::DataValue;
use uuid::Uuid;

pub type SnapshotId = Uuid;
pub type ColumnId = u32;
pub type Location = String;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct TableSnapshot {
    pub snapshot_id: SnapshotId,
    pub prev_snapshot_id: Option<SnapshotId>,
    /// For each snapshot, we keep a schema for it (in case of schema evolution)
    pub schema: DataSchema,
    /// Summary Statistics
    pub summary: Stats,
    /// Pointers to SegmentInfos
    ///
    /// We rely on background merge tasks to keep merging segments, so that
    /// this the size of this vector could be kept reasonable
    pub segments: Vec<Location>,
}

impl TableSnapshot {
    pub fn new() -> Self {
        todo!()
    }
}

/// A segment comprised of one or more blocks
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct SegmentInfo {
    pub blocks: Vec<BlockMeta>,
    pub summary: Stats,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Stats {
    pub row_count: u64,
    pub block_count: u64,
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

#[allow(dead_code)]
pub type RawBlockStats = HashMap<u32, std::sync::Arc<dyn Statistics>>;
