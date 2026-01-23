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
use databend_common_frozen_api::FrozenAPI;
use databend_common_frozen_api::frozen_api;
use databend_common_statistics::Histogram;
use databend_common_storage::MetaHLL;

use crate::meta::FormatVersion;
use crate::meta::SnapshotId;
use crate::meta::Versioned;
use crate::meta::v1;
use crate::meta::v2;
use crate::meta::v3;

#[frozen_api("446eb231")]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, FrozenAPI)]
pub struct TableSnapshotStatistics {
    /// format version of snapshot
    pub format_version: FormatVersion,

    pub snapshot_id: SnapshotId,
    pub row_count: u64,
    pub hll: HashMap<ColumnId, MetaHLL>,
    pub histograms: HashMap<ColumnId, Histogram>,
}

impl TableSnapshotStatistics {
    pub fn empty_with_id(snapshot_id: SnapshotId) -> Self {
        Self {
            format_version: TableSnapshotStatistics::VERSION,
            snapshot_id,
            hll: HashMap::new(),
            histograms: HashMap::new(),
            row_count: 0,
        }
    }

    pub fn new(
        hll: HashMap<ColumnId, MetaHLL>,
        histograms: HashMap<ColumnId, Histogram>,
        snapshot_id: SnapshotId,
        row_count: u64,
    ) -> Self {
        Self {
            format_version: TableSnapshotStatistics::VERSION,
            snapshot_id,
            hll,
            histograms,
            row_count,
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
            row_count: 0,
            hll: HashMap::new(),
            histograms: HashMap::new(),
        }
    }
}

impl From<v2::TableSnapshotStatistics> for TableSnapshotStatistics {
    fn from(value: v2::TableSnapshotStatistics) -> Self {
        let hll = value.hll.into_iter().map(|(k, v)| (k, v.into())).collect();
        Self {
            format_version: TableSnapshotStatistics::VERSION,
            snapshot_id: value.snapshot_id,
            row_count: 0,
            hll,
            histograms: HashMap::new(),
        }
    }
}

impl From<v3::TableSnapshotStatistics> for TableSnapshotStatistics {
    fn from(value: v3::TableSnapshotStatistics) -> Self {
        let hll = value.hll.into_iter().map(|(k, v)| (k, v.into())).collect();
        Self {
            format_version: TableSnapshotStatistics::VERSION,
            snapshot_id: value.snapshot_id,
            row_count: 0,
            hll,
            histograms: value.histograms,
        }
    }
}
