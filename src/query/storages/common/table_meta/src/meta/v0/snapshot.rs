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

use databend_common_datavalues as dv;
use serde::Deserialize;
use serde::Serialize;

use super::statistics::Statistics;
use crate::meta::SnapshotId;

pub type Location = String;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableSnapshot {
    pub snapshot_id: SnapshotId,
    pub prev_snapshot_id: Option<SnapshotId>,
    /// For each snapshot, we keep a schema for it (in case of schema evolution)
    pub schema: dv::DataSchema,
    /// Summary Statistics
    pub summary: Statistics,
    /// Pointers to SegmentInfos (may be of different format)
    pub segments: Vec<Location>,
}

impl TableSnapshot {
    #[must_use]
    pub fn append_segment(mut self, location: Location) -> TableSnapshot {
        self.segments.push(location);
        self
    }
}
