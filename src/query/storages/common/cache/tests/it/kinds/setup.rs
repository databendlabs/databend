// Copyright 2026 Datafuse Labs.
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

//! Managers with every kind enabled, as the full and the small configurations of each backend.

use databend_common_config::CacheConfig;
use databend_common_config::CacheStorageTypeInnerConfig;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::Pool;
use tempfile::TempDir;

const MIB: u64 = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backend {
    Legacy,
    Shared,
}

impl Backend {
    pub const ALL: [Backend; 2] = [Backend::Legacy, Backend::Shared];

    /// Every kind enabled with room to spare, under a memory limit of `memory` bytes.
    pub fn config(self, memory: u64) -> CacheConfig {
        let room = memory / 8;
        match self {
            Backend::Legacy => CacheConfig {
                segment_block_metas_count: 64,
                data_cache_storage: CacheStorageTypeInnerConfig::Disk,
                data_cache_in_memory_bytes: room,
                table_data_deserialized_data_bytes: room,
                ..CacheConfig::default()
            },
            Backend::Shared => CacheConfig {
                shared_memory_enabled: true,
                // The kinds off until sized.
                kind_max_bytes: [
                    "memory_cache_segment_block_metas",
                    "memory_cache_column_data",
                    "memory_cache_table_data",
                ]
                .map(|name| (name.to_string(), room))
                .into(),
                ..CacheConfig::default()
            },
        }
    }

    /// Every kind enabled, small enough to evict all the time.
    pub fn small_config(self) -> CacheConfig {
        let config = self.config(8 * MIB);
        match self {
            Backend::Shared => config,
            Backend::Legacy => CacheConfig {
                table_meta_snapshot_count: 8,
                table_meta_statistic_count: 8,
                table_meta_segment_bytes: 256 << 10,
                segment_block_metas_count: 8,
                segment_statistics_bytes: 256 << 10,
                table_bloom_index_meta_count: 8,
                inverted_index_meta_count: 8,
                vector_index_meta_count: 8,
                spatial_index_meta_count: 8,
                virtual_column_meta_count: 8,
                iceberg_table_meta_count: 8,
                table_prune_partitions_count: 8,
                table_bloom_index_filter_size: 256 << 10,
                vector_index_filter_size: 256 << 10,
                vector_index_refine_size: 256 << 10,
                inverted_index_lookup_size: 256 << 10,
                inverted_index_payload_size: 256 << 10,
                spatial_index_filter_size: 256 << 10,
                data_cache_in_memory_bytes: 256 << 10,
                table_data_deserialized_data_bytes: 256 << 10,
                ..config
            },
        }
    }

    /// Loads before a read value of `kind` is cached.
    pub fn admit_after(self, kind: CacheKind) -> usize {
        match (self, kind.pool()) {
            (Backend::Shared, Pool::Data) => 2,
            _ => 1,
        }
    }
}

/// Every disk tier enabled, of 1 MiB, under `dir`.
pub fn with_disk(config: CacheConfig, dir: &TempDir) -> CacheConfig {
    let mut config = CacheConfig {
        data_cache_storage: CacheStorageTypeInnerConfig::Disk,
        disk_cache_table_bloom_index_data_size: MIB,
        disk_cache_table_bloom_index_meta_size: MIB,
        disk_cache_inverted_index_meta_size: MIB,
        disk_cache_inverted_index_lookup_size: MIB,
        disk_cache_inverted_index_payload_size: MIB,
        disk_cache_vector_index_meta_size: MIB,
        disk_cache_vector_index_data_size: MIB,
        disk_cache_spatial_index_meta_size: MIB,
        disk_cache_spatial_index_data_size: MIB,
        disk_cache_virtual_column_meta_size: MIB,
        ..config
    };
    config.disk_cache_config.max_bytes = MIB;
    config.disk_cache_config.path = dir.path().to_string_lossy().to_string();
    config
}

/// Disk tiers exist only in EE mode.
pub fn manager(config: &CacheConfig, memory: u64, ee_mode: bool) -> CacheManager {
    CacheManager::try_new(config, &memory, "test_tenant", ee_mode).unwrap()
}
