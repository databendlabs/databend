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
use serde::Deserialize;
use serde::Serialize;
use databend_common_storage::Histogram;
use crate::meta::v1;
use crate::meta::FormatVersion;
use crate::meta::SnapshotId;
use crate::meta::Versioned;

pub type MetaHLL = simple_hll::HyperLogLog<12>;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableSnapshotStatistics {
    /// format version of snapshot
    pub format_version: FormatVersion,

    /// id of snapshot
    pub snapshot_id: SnapshotId,
    pub hll: HashMap<ColumnId, MetaHLL>,
    pub histograms: HashMap<ColumnId, Histogram>
}

impl TableSnapshotStatistics {
    pub fn new(hll: HashMap<ColumnId, MetaHLL>, histograms: HashMap<ColumnId, Histogram>,snapshot_id: SnapshotId) -> Self {
        Self {
            format_version: TableSnapshotStatistics::VERSION,
            snapshot_id,
            hll,
            histograms,
        }
    }

    pub fn format_version(&self) -> u64 {
        self.format_version
    }

    pub fn column_distinct_values(&self) -> HashMap<ColumnId, u64> {
        self.hll
            .iter()
            .map(|hll| (*hll.0, hll.1.count() as u64))
            .collect()
    }
}

impl From<v1::TableSnapshotStatistics> for TableSnapshotStatistics {
    fn from(value: v1::TableSnapshotStatistics) -> Self {
        Self {
            format_version: TableSnapshotStatistics::VERSION,
            snapshot_id: value.snapshot_id,
            hll: HashMap::new(),
            histograms: HashMap::new(),
        }
    }
}
