// Copyright 2026 Datafuse Labs
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

//! Table snapshots, their statistics, and pruning results.

use std::mem::size_of;
use std::sync::Arc;

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;

use crate::CacheDef;
use crate::CachedItem;
use crate::Pool;
use crate::size::hash_map;
use crate::size::location;
use crate::size::option;
use crate::size::scalar;
use crate::size::statistics;
use crate::size::table_schema;
use crate::size::vec;

/// Table snapshots, by snapshot location.
pub struct Snapshot;

impl CacheDef for Snapshot {
    type Value = Arc<TableSnapshot>;
    const NAME: &'static str = "memory_cache_table_snapshot";
    const POOL: Pool = Pool::Meta;
}

/// Table statistics (HLL, histograms, ...) of snapshots, by location.
pub struct SnapshotStats;

impl CacheDef for SnapshotStats {
    type Value = Arc<TableSnapshotStatistics>;
    const NAME: &'static str = "memory_cache_table_statistics";
    const POOL: Pool = Pool::Meta;
}

/// Pruning results of deterministic scans (`enable_prune_cache`).
pub struct PruneResult;

impl CacheDef for PruneResult {
    type Value = (PartStatistics, Partitions);
    const NAME: &'static str = "memory_cache_prune_partitions";
    const POOL: Pool = Pool::Meta;
}

impl CachedItem for TableSnapshot {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>()
            + table_schema(&self.schema)
            + statistics(&self.summary)
            + vec(&self.segments, location)
            + option(&self.cluster_key_meta, |(_, expr)| expr.capacity())
            + option(&self.table_statistics_location, |s| s.capacity())
    }
}

impl CachedItem for TableSnapshotStatistics {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>()
            + hash_map(&self.hll, |_, hll| hll.memory_size())
            + hash_map(&self.top_n, |_, top_n| {
                vec(&top_n.values, |e| scalar(&e.scalar))
            })
            + hash_map(&self.count_min_sketch, |_, cms| cms.memory_size())
            + hash_map(&self.histograms, |_, h| h.memory_size())
    }
}

/// Partitions are opaque `dyn PartInfo`; this is the typical size of a fuse block part.
const PART_INFO_BYTES: usize = 512;

impl CachedItem for (PartStatistics, Partitions) {
    fn mem_bytes(&self) -> usize {
        let parts = &self.1.partitions;
        size_of::<Self>()
            + parts.capacity() * size_of::<PartInfoPtr>()
            + parts.len() * PART_INFO_BYTES
    }
}
