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

use std::fmt::Debug;

use crate::plan::PartitionsShuffleKind;
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
    /// Pruning stats.
    pub pruning_stats: PruningStatistics,
    /// Shuffle kind used for this query (helps determine merge strategy)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shuffle_kind: Option<PartitionsShuffleKind>,
}

impl PartStatistics {
    pub fn new_estimated(
        snapshot: Option<String>,
        read_rows: usize,
        read_bytes: usize,
        partitions_scanned: usize,
        partitions_total: usize,
    ) -> Self {
        PartStatistics {
            snapshot,
            read_rows,
            read_bytes,
            partitions_scanned,
            partitions_total,
            is_exact: false,
            pruning_stats: Default::default(),
            shuffle_kind: None,
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
            pruning_stats: Default::default(),
            shuffle_kind: None,
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
        // Preserve shuffle_kind if not set
        if self.shuffle_kind.is_none() && other.shuffle_kind.is_some() {
            self.shuffle_kind = other.shuffle_kind.clone();
        }

        // Determine merge strategy based on shuffle kind
        let shuffle_kind = self.shuffle_kind.as_ref().or(other.shuffle_kind.as_ref());
        let use_max_for_counts = matches!(
            shuffle_kind,
            Some(PartitionsShuffleKind::BlockMod)
                | Some(PartitionsShuffleKind::BroadcastCluster)
                | Some(PartitionsShuffleKind::BroadcastWarehouse)
        );

        if use_max_for_counts {
            // BlockMod/Broadcast: all nodes see the same data, use max
            self.read_rows = self.read_rows.max(other.read_rows);
            self.read_bytes = self.read_bytes.max(other.read_bytes);
            self.partitions_scanned = self.partitions_scanned.max(other.partitions_scanned);
        } else {
            // Mod/Seq: each node sees different data, use sum
            self.read_rows += other.read_rows;
            self.read_bytes += other.read_bytes;
            self.partitions_scanned += other.partitions_scanned;
        }

        // partitions_total represents the total number of partitions in the table (before pruning),
        // which should be the same across all nodes. Always use max to avoid inflation.
        self.partitions_total = self.partitions_total.max(other.partitions_total);

        self.pruning_stats
            .merge(&other.pruning_stats, use_max_for_counts);
    }
}
