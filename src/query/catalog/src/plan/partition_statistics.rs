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

use std::collections::BTreeMap;
use std::fmt::Debug;

use databend_storages_common_table_meta::meta::Location;

use crate::plan::PruningStatistics;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone, Debug, Default)]
pub struct PartStatistics {
    pub snapshot: Option<String>,
    /// Total rows of the query read.
    pub read_rows: usize,
    /// Total bytes of the query read.
    pub read_bytes: usize,
    /// Number of partitions scanned, (after pruning)
    pub partitions_scanned: usize,
    /// Number of partitions, (before pruning)
    pub partitions_total: usize,
    /// Is the statistics exact.
    pub is_exact: bool,
    /// inverted index locations
    pub index_info_locations: Option<BTreeMap<String, Location>>,
    /// Pruning stats.
    pub pruning_stats: PruningStatistics,
}

impl PartStatistics {
    pub fn new_estimated(
        snapshot: Option<String>,
        read_rows: usize,
        read_bytes: usize,
        partitions_scanned: usize,
        partitions_total: usize,
        index_info_locations: Option<BTreeMap<String, Location>>,
    ) -> Self {
        PartStatistics {
            snapshot,
            read_rows,
            read_bytes,
            partitions_scanned,
            partitions_total,
            is_exact: false,
            index_info_locations,
            pruning_stats: Default::default(),
        }
    }

    pub fn new_exact(
        read_rows: usize,
        read_bytes: usize,
        partitions_scanned: usize,
        partitions_total: usize,
    ) -> Self {
        PartStatistics {
            read_rows,
            read_bytes,
            partitions_scanned,
            partitions_total,
            is_exact: true,
            snapshot: None,
            index_info_locations: Default::default(),
            pruning_stats: Default::default(),
        }
    }

    pub fn default_exact() -> Self {
        Self {
            is_exact: true,
            ..Default::default()
        }
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }

    pub fn get_description(&self, table_desc: &str) -> String {
        if self.read_rows > 0 {
            format!(
                "(Read from {} table, {} Read Rows:{}, Read Bytes:{}, Partitions Scanned:{}, Partitions Total:{})",
                table_desc,
                if self.is_exact {
                    "Exactly"
                } else {
                    "Approximately"
                },
                self.read_rows,
                self.read_bytes,
                self.partitions_scanned,
                self.partitions_total,
            )
        } else {
            format!("(Read from {} table)", table_desc)
        }
    }

    pub fn merge(&mut self, other: &Self) {
        self.read_rows += other.read_rows;
        self.read_bytes += other.read_bytes;
        self.partitions_scanned += other.partitions_scanned;
        self.partitions_total += other.partitions_total;
        self.pruning_stats.merge(&other.pruning_stats);
    }
}
