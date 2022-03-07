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

use common_datavalues::DataSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::storages::fuse::meta::v0::snapshot::Statistics;

pub type ColumnId = u32;
pub type SnapshotId = Uuid;
pub type FormatVersion = u64;
pub type Location = (String, FormatVersion);

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableSnapshot {
    /// format version of snapshot
    pub format_version: FormatVersion,

    /// id of snapshot
    pub snapshot_id: SnapshotId,

    pub prev_snapshot_id: Option<(SnapshotId, FormatVersion)>,

    /// For each snapshot, we keep a schema for it (in case of schema evolution)
    pub schema: DataSchema,

    /// Summary Statistics
    pub summary: Statistics,

    /// Pointers to SegmentInfos (may be of different format)
    ///
    /// We rely on background merge tasks to keep merging segments, so that
    /// this the size of this vector could be kept reasonable
    pub segments: Vec<Location>,
}

impl TableSnapshot {
    pub const fn current_format_version() -> u64 {
        1
    }
}

use super::super::v0::snapshot::TableSnapshot as V0;

impl From<V0> for TableSnapshot {
    fn from(s: V0) -> Self {
        Self {
            format_version: 1,
            snapshot_id: s.snapshot_id,
            prev_snapshot_id: s.prev_snapshot_id.map(|id| (id, 0)),
            schema: s.schema,
            summary: s.summary,
            segments: s.segments.into_iter().map(|l| (l, 0)).collect(),
        }
    }
}
