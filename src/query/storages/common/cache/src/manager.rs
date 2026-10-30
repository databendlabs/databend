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
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_config::CacheConfig;
use databend_common_config::CacheStorageTypeInnerConfig;
use databend_common_config::DiskCacheKeyReloadPolicy;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::info;
use log::warn;

use crate::DiskCacheAccessor;
use crate::DiskCacheBuilder;
use crate::KindLimit;
use crate::PoolCaps;
use crate::backend::KindStore;
use crate::cache_manager::CacheManager as KindCacheManager;
use crate::cache_manager::CacheRow;
use crate::legacy::LegacyStore;
use crate::legacy::legacy_capacities;
use crate::legacy::legacy_total_bytes;
use crate::registry::CacheKind;
use crate::shared::SharedStore;

// The existing payload size is the total memory budget for page and full-component caches.
pub(crate) fn inverted_index_payload_capacities(total: usize) -> (usize, usize) {
    let full = total / 4;
    (total - full, full)
}

/// Legacy capacity options set to a non-default value; the shared backend does not read them.
fn ignored_legacy_options(config: &CacheConfig) -> Vec<&'static str> {
    let default = CacheConfig::default();
    macro_rules! changed {
        ($($field:ident),* $(,)?) => {
            [$((stringify!($field), config.$field != default.$field)),*]
        };
    }
    changed!(
        enable_table_meta_cache,
        table_meta_snapshot_count,
        table_meta_segment_bytes,
        segment_block_metas_count,
        table_meta_statistic_count,
        segment_statistics_bytes,
        table_bloom_index_meta_count,
        table_prune_partitions_count,
        table_bloom_index_filter_size,
        inverted_index_meta_count,
        inverted_index_lookup_size,
        inverted_index_payload_size,
        vector_index_meta_count,
        vector_index_filter_size,
        vector_index_filter_memory_ratio,
        vector_index_refine_size,
        spatial_index_meta_count,
        spatial_index_filter_size,
        spatial_index_filter_memory_ratio,
        virtual_column_meta_count,
        data_cache_in_memory_bytes,
        table_data_deserialized_data_bytes,
        table_data_deserialized_memory_ratio,
        iceberg_table_meta_count,
    )
    .into_iter()
    .filter_map(|(name, changed)| changed.then_some(name))
    .collect()
}

// Minimum threshold for table data disk cache size (in bytes).
// Any configuration value less than this threshold will be ignored,
// and table data disk cache will not be enabled.
// This threshold exists to accommodate the current cloud platform logic:
// When attempting to disable table data cache in compute node configurations, setting the disk
// cache size to zero prevents the physical volume from being loaded, so this threshold provides
// a better approach.
// Eventually, we should refactor the compute node configurations instead, to make those options more sensible.
const TABLE_DATA_DISK_CACHE_SIZE_THRESHOLD: usize = 1024;

/// Builds the process-wide caches from `CacheConfig`: the legacy capacities, and the disk tiers
/// in their pre-refactor directories. Typed caches are handed out by `kind_manager`.
pub struct CacheManager {
    kinds: Arc<KindCacheManager>,
    shared: bool,
}

impl CacheManager {
    /// Initialize the caches according to the relevant configurations.
    pub fn init(
        config: &CacheConfig,
        max_server_memory_usage: &u64,
        tenant_id: impl Into<String>,
        ee_mode: bool,
    ) -> Result<()> {
        let instance = Arc::new(Self::try_new(
            config,
            max_server_memory_usage,
            tenant_id,
            ee_mode,
        )?);
        GlobalInstance::set(instance);
        Ok(())
    }

    /// Disk tiers exist only in EE mode, and can then be turned off at runtime by
    /// `set_allows_disk_cache`.
    fn try_new(
        config: &CacheConfig,
        max_server_memory_usage: &u64,
        tenant_id: impl Into<String>,
        ee_mode: bool,
    ) -> Result<Self> {
        let tenant_id = tenant_id.into();
        let queue_size: u32 = if config.table_data_cache_population_queue_size > 0 {
            config.table_data_cache_population_queue_size
        } else {
            std::cmp::max(
                1,
                std::thread::available_parallelism()
                    .expect("Cannot get thread count")
                    .get() as u32,
            ) * 5
        };
        info!("[CacheManager] On-disk cache population queue size: {queue_size}");

        let mut tiers = Vec::new();
        if matches!(config.data_cache_storage, CacheStorageTypeInnerConfig::Disk) {
            tiers.push((
                CacheKind::ColumnData,
                "v1",
                config.disk_cache_config.max_bytes,
                config.data_cache_key_reload_policy.clone(),
            ));
        }
        // The shared backend ignores `enable_table_meta_cache`; disk options apply to both.
        if config.enable_table_meta_cache || config.shared_memory_enabled {
            let fuzzy = DiskCacheKeyReloadPolicy::Fuzzy;
            tiers.extend(
                [
                    (
                        CacheKind::BloomIndexMeta,
                        "bloom_meta_v1",
                        config.disk_cache_table_bloom_index_meta_size,
                    ),
                    (
                        CacheKind::BloomFilter,
                        "bloom_filter_v1",
                        config.disk_cache_table_bloom_index_data_size,
                    ),
                    (
                        CacheKind::InvertedIndexMeta,
                        "inverted_index_meta_v1",
                        config.disk_cache_inverted_index_meta_size,
                    ),
                    (
                        CacheKind::InvertedIndexLookup,
                        "inverted_index_lookup_v1",
                        config.disk_cache_inverted_index_lookup_size,
                    ),
                    (
                        CacheKind::InvertedIndexPayload,
                        "inverted_index_payload_v1",
                        config.disk_cache_inverted_index_payload_size,
                    ),
                    (
                        CacheKind::VectorIndexMeta,
                        "vector_index_meta_v1",
                        config.disk_cache_vector_index_meta_size,
                    ),
                    (
                        CacheKind::VectorRefine,
                        "vector_index_file_v1",
                        config.disk_cache_vector_index_data_size,
                    ),
                    (
                        CacheKind::SpatialIndexMeta,
                        "spatial_index_meta_v1",
                        config.disk_cache_spatial_index_meta_size,
                    ),
                    (
                        CacheKind::SpatialIndex,
                        "spatial_index_file_v1",
                        config.disk_cache_spatial_index_data_size,
                    ),
                    (
                        CacheKind::VirtualColumnMeta,
                        "virtual_column_meta_v1",
                        config.disk_cache_virtual_column_meta_size,
                    ),
                ]
                .map(|(kind, dir, bytes)| (kind, dir, bytes, fuzzy.clone())),
            );
        }

        let mut disks = HashMap::new();
        for (kind, dir, bytes, reload_policy) in tiers {
            let bytes = usize::try_from(bytes).map_err(|_| {
                ErrorCode::BadArguments(format!(
                    "disk cache size of {} exceeds this platform",
                    kind.legacy_name()
                ))
            })?;
            // Same names as the pre-refactor hybrid caches: `disk_cache_<...>`.
            let name = format!("disk_{}", kind.legacy_name().trim_start_matches("memory_"));
            let path = PathBuf::from(&config.disk_cache_config.path)
                .join(&tenant_id)
                .join(dir);
            if let Some(disk) = Self::new_on_disk_cache(
                name,
                &path,
                queue_size,
                bytes,
                reload_policy,
                config.disk_cache_config.sync_data,
                ee_mode,
            )? {
                disks.insert(kind, Arc::new(disk));
            }
        }

        let store: Arc<dyn KindStore> = if config.shared_memory_enabled {
            Arc::new(Self::shared_store(config, *max_server_memory_usage)?)
        } else {
            let capacities = legacy_capacities(config, *max_server_memory_usage);
            let total = legacy_total_bytes(&capacities);
            if *max_server_memory_usage > 0 && total > *max_server_memory_usage as u128 {
                warn!(
                    "[CacheManager] Cache capacities sum to {total} bytes, more than max_server_memory_usage {max_server_memory_usage}"
                );
            }
            Arc::new(LegacyStore::new(capacities))
        };
        let kinds = Arc::new(KindCacheManager::new(store, disks));
        kinds.set_disk_allowed(ee_mode);
        Ok(CacheManager {
            kinds,
            shared: config.shared_memory_enabled,
        })
    }

    /// Pools sized by ratio of the memory limit: `max_server_memory_usage`, or 80% of the
    /// system memory when it is 0, as for the `max_memory_usage` setting.
    fn shared_store(config: &CacheConfig, max_server_memory_usage: u64) -> Result<SharedStore> {
        let memory_limit = match max_server_memory_usage {
            0 => {
                let info = sys_info::mem_info().map_err(ErrorCode::from_std_error)?;
                1024 * info.total * 80 / 100
            }
            limit => limit,
        };
        let memory_limit = usize::try_from(memory_limit)
            .map_err(|_| ErrorCode::InvalidConfig("memory limit exceeds this platform"))?;
        let caps = PoolCaps::from_ratios(
            memory_limit,
            config.meta_max_ratio,
            config.index_max_ratio,
            config.data_max_ratio,
        )?;
        let limits = config
            .kind_max_bytes
            .iter()
            .map(|(name, bytes)| {
                let kind = CacheKind::from_any_name(name).ok_or_else(|| {
                    ErrorCode::InvalidConfig(format!(
                        "unknown cache '{name}' in cache.kind_max_bytes"
                    ))
                })?;
                let bytes = usize::try_from(*bytes).map_err(|_| {
                    ErrorCode::InvalidConfig(format!(
                        "cache.kind_max_bytes.{name} exceeds this platform"
                    ))
                })?;
                Ok((kind, KindLimit::bytes(bytes)))
            })
            .collect::<Result<Vec<_>>>()?;
        let ignored = ignored_legacy_options(config);
        if !ignored.is_empty() {
            warn!(
                "[CacheManager] The shared cache backend ignores: {}",
                ignored.join(", ")
            );
        }
        let shards = std::thread::available_parallelism().map_or(8, |n| n.get().max(8));
        info!(
            "[CacheManager] Shared cache pools: meta {} bytes, index {} bytes, data {} bytes",
            caps.meta, caps.index, caps.data
        );
        Ok(SharedStore::new(caps, limits, shards))
    }

    pub fn instance() -> Arc<CacheManager> {
        GlobalInstance::get()
    }

    pub fn kind_manager(&self) -> &KindCacheManager {
        &self.kinds
    }

    /// Whether the shared backend (`shared_memory_enabled`) is in use.
    pub fn is_shared(&self) -> bool {
        self.shared
    }

    /// Evicts unreferenced entries until `relieved` returns true; returns the bytes freed.
    /// Registered with the memory manager for the shared backend. It runs on an allocating
    /// thread that may hold any lock, so it neither logs nor blocks on cache shard locks.
    pub fn shrink_for_pressure(&self, relieved: &dyn Fn() -> bool) -> usize {
        self.kinds.shrink_for_pressure(relieved).freed
    }

    /// `system.caches` rows: enabled memory tiers, and the disk tiers while allowed.
    pub fn cache_rows(&self) -> Vec<CacheRow> {
        self.kinds
            .rows()
            .into_iter()
            .filter(|row| row.enabled)
            .collect()
    }

    /// Accepts a kind name, a legacy memory cache name (`memory_cache_*`) or a legacy hybrid
    /// cache name (`cache_*`). The legacy backend reads the capacity in the kind's legacy unit,
    /// items or bytes; the shared backend reads bytes and also accepts `pool:<name>` and `all`.
    pub fn set_cache_capacity(&self, name: &str, new_capacity: u64) -> Result<()> {
        let capacity = usize::try_from(new_capacity)
            .map_err(|_| ErrorCode::BadArguments("cache capacity exceeds this platform"))?;
        if name == "all" || name.starts_with("pool:") {
            return self.kinds.set_capacity_by_name(name, capacity);
        }
        match CacheKind::from_any_name(name) {
            // As before, the payload size is the budget of the page and full-component caches.
            Some(CacheKind::InvertedIndexPayload) if !self.shared => {
                let (pages, full) = inverted_index_payload_capacities(capacity);
                self.kinds.set_capacity_by_name(name, pages)?;
                self.kinds
                    .set_capacity_by_name(CacheKind::InvertedIndexFullPayload.name(), full)
            }
            Some(_) => self.kinds.set_capacity_by_name(name, capacity),
            None => Err(ErrorCode::BadArguments(format!(
                "cache {name} not found, or not allowed to be adjusted"
            ))),
        }
    }

    pub fn set_allows_disk_cache(&self, flag: bool) {
        self.kinds.set_disk_allowed(flag);
    }

    fn new_on_disk_cache(
        cache_name: String,
        path: &Path,
        population_queue_size: u32,
        disk_cache_bytes_size: usize,
        disk_cache_key_reload_policy: DiskCacheKeyReloadPolicy,
        sync_data: bool,
        ee_mode: bool,
    ) -> Result<Option<DiskCacheAccessor>> {
        if disk_cache_bytes_size <= TABLE_DATA_DISK_CACHE_SIZE_THRESHOLD || !ee_mode {
            info!(
                "[CacheManager] On-disk cache {cache_name} disabled, size {disk_cache_bytes_size}, threshold {TABLE_DATA_DISK_CACHE_SIZE_THRESHOLD}, ee mode {ee_mode}"
            );
            Ok(None)
        } else {
            let cache_holder = DiskCacheBuilder::try_build_disk_cache(
                cache_name,
                &path.to_path_buf(),
                population_queue_size,
                disk_cache_bytes_size,
                disk_cache_key_reload_policy,
                sync_data,
            )?;
            Ok(Some(cache_holder))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::time::Instant;

    use bytes::Bytes;
    use databend_common_config::DiskCacheInnerConfig;
    use databend_common_expression::TableSchema;
    use databend_storages_common_table_meta::meta::Statistics;
    use databend_storages_common_table_meta::meta::TableMetaTimestamps;
    use databend_storages_common_table_meta::meta::TableSnapshot;
    use tempfile::TempDir;

    use super::*;
    use crate::ColumnData;
    use crate::Unit;
    use crate::registry::Pool;

    /// Disk tiers configured for every kind that has one: column data, the index metas,
    /// bloom filters, inverted lookup / payload, vector refine pages and spatial files.
    const DISK_TIERS: usize = 11;

    fn config_with_disk_cache_enabled(cache_path: &str) -> CacheConfig {
        CacheConfig {
            data_cache_storage: CacheStorageTypeInnerConfig::Disk,
            disk_cache_config: DiskCacheInnerConfig {
                max_bytes: 1024 * 1024,
                path: cache_path.to_string(),
                ..Default::default()
            },
            disk_cache_table_bloom_index_data_size: 1024 * 1024,
            disk_cache_table_bloom_index_meta_size: 1024 * 1024,
            disk_cache_inverted_index_meta_size: 1024 * 1024,
            disk_cache_inverted_index_lookup_size: 1024 * 1024,
            disk_cache_inverted_index_payload_size: 1024 * 1024,
            disk_cache_vector_index_meta_size: 1024 * 1024,
            disk_cache_vector_index_data_size: 1024 * 1024,
            disk_cache_spatial_index_meta_size: 1024 * 1024,
            disk_cache_spatial_index_data_size: 1024 * 1024,
            disk_cache_virtual_column_meta_size: 1024 * 1024,
            ..CacheConfig::default()
        }
    }

    fn config_with_disk_cache_disabled() -> CacheConfig {
        CacheConfig {
            data_cache_storage: CacheStorageTypeInnerConfig::None,
            disk_cache_table_bloom_index_data_size: 0,
            disk_cache_table_bloom_index_meta_size: 0,
            disk_cache_inverted_index_meta_size: 0,
            disk_cache_inverted_index_lookup_size: 0,
            disk_cache_inverted_index_payload_size: 0,
            disk_cache_vector_index_meta_size: 0,
            disk_cache_vector_index_data_size: 0,
            disk_cache_spatial_index_meta_size: 0,
            disk_cache_spatial_index_data_size: 0,
            disk_cache_virtual_column_meta_size: 0,
            ..CacheConfig::default()
        }
    }

    fn disk_tiers(manager: &CacheManager) -> usize {
        manager
            .cache_rows()
            .iter()
            .filter(|r| r.name.starts_with("disk_"))
            .count()
    }

    fn row(manager: &CacheManager, name: &str) -> Option<CacheRow> {
        manager.cache_rows().into_iter().find(|r| r.name == name)
    }

    fn capacity(manager: &CacheManager, name: &str) -> Option<u64> {
        row(manager, name).and_then(|r| r.capacity)
    }

    /// EE mode: disk tiers enabled in the config can be toggled off and on at runtime; disk
    /// tiers disabled in the config cannot be toggled on.
    #[test]
    fn test_cache_manager_ee_mode() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let cache_path = temp_dir.path().to_string_lossy();
        let config = config_with_disk_cache_enabled(&cache_path);
        let manager = CacheManager::try_new(&config, &(1024 * 1024), "test_tenant_id", true)?;
        assert_eq!(disk_tiers(&manager), DISK_TIERS);
        manager.set_allows_disk_cache(false);
        assert_eq!(disk_tiers(&manager), 0);
        manager.set_allows_disk_cache(true);
        assert_eq!(disk_tiers(&manager), DISK_TIERS);

        let config = config_with_disk_cache_disabled();
        let manager = CacheManager::try_new(&config, &(1024 * 1024), "test_tenant_id", true)?;
        assert_eq!(disk_tiers(&manager), 0);
        manager.set_allows_disk_cache(true);
        assert_eq!(disk_tiers(&manager), 0);
        Ok(())
    }

    /// Non-EE mode: disk tiers are never built, even if enabled in the config.
    #[test]
    fn test_cache_manager_non_ee_mode() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let cache_path = temp_dir.path().to_string_lossy();
        let config = config_with_disk_cache_enabled(&cache_path);
        let manager = CacheManager::try_new(&config, &(1024 * 1024), "test_tenant_id", false)?;
        assert_eq!(disk_tiers(&manager), 0);
        manager.set_allows_disk_cache(true);
        assert_eq!(disk_tiers(&manager), 0);
        Ok(())
    }

    /// Memory pressure reclaims the memory tier of column data; its disk tier is kept, and a
    /// later lookup refills memory from disk. Same as `release_cache_memory(Basic)` before.
    #[test]
    fn test_pressure_shrink_keeps_disk_tier() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let cache_path = temp_dir.path().to_string_lossy();
        let config = CacheConfig {
            data_cache_in_memory_bytes: 1024 * 1024,
            ..config_with_disk_cache_enabled(&cache_path)
        };
        let manager = CacheManager::try_new(&config, &(1024 * 1024), "test_tenant_id", true)?;
        let cache = manager
            .kinds
            .hybrid_cache::<ColumnData>(CacheKind::ColumnData);
        cache.put("k", ColumnData::from_bytes(Bytes::from_static(b"data")));
        let deadline = Instant::now() + Duration::from_secs(10);
        while row(&manager, "disk_cache_column_data").map_or(0, |r| r.num_items) == 0 {
            assert!(Instant::now() < deadline, "disk tier was not populated");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            row(&manager, "memory_cache_column_data").unwrap().num_items,
            1
        );

        manager.kinds.shrink_for_pressure(&|| false);
        assert_eq!(
            row(&manager, "memory_cache_column_data").unwrap().num_items,
            0
        );
        assert_eq!(
            cache.lookup("k").unwrap().bytes(),
            Bytes::from_static(b"data")
        );
        assert_eq!(
            row(&manager, "memory_cache_column_data").unwrap().num_items,
            1
        );
        Ok(())
    }

    #[test]
    fn test_vector_index_cache_budgets() -> Result<()> {
        let max_server_memory_usage = 4 * 1024 * 1024;
        let default_config = CacheConfig::default();
        let default_capacity = default_config.vector_index_filter_size;
        let cases = [
            (default_config, Some(default_capacity)),
            (
                CacheConfig {
                    vector_index_filter_size: 54321,
                    ..Default::default()
                },
                Some(54321),
            ),
            (
                CacheConfig {
                    vector_index_filter_size: 1,
                    vector_index_filter_memory_ratio: 25,
                    ..Default::default()
                },
                Some(max_server_memory_usage / 4),
            ),
            (
                CacheConfig {
                    vector_index_filter_size: 0,
                    vector_index_filter_memory_ratio: 25,
                    ..Default::default()
                },
                Some(max_server_memory_usage / 4),
            ),
            (
                CacheConfig {
                    vector_index_filter_size: 0,
                    ..Default::default()
                },
                None,
            ),
            (
                CacheConfig {
                    enable_table_meta_cache: false,
                    ..Default::default()
                },
                None,
            ),
        ];
        for (config, expected) in cases {
            let manager = CacheManager::try_new(&config, &max_server_memory_usage, "test", false)?;
            assert_eq!(
                capacity(&manager, "memory_cache_vector_index_file"),
                expected
            );
            // The refine cache has its own memory budget, independent of the HNSW
            // cache. Non-EE mode: no disk tier (see `test_vector_refine_cache_tiers`).
            assert_eq!(
                capacity(&manager, "memory_cache_vector_index_refine"),
                config
                    .enable_table_meta_cache
                    .then_some(config.vector_index_refine_size)
            );
            assert_eq!(disk_tiers(&manager), 0);
        }
        Ok(())
    }

    #[test]
    fn test_vector_index_capacity_names_resize_the_same_budget() -> Result<()> {
        let config = CacheConfig {
            vector_index_filter_size: 0,
            ..Default::default()
        };
        let manager = CacheManager::try_new(&config, &(1024 * 1024), "test", false)?;
        let name = "memory_cache_vector_index_file";
        assert_eq!(capacity(&manager, name), None);
        for alias in ["cache_vector_index_file", name] {
            manager.set_cache_capacity(alias, 12345)?;
            assert_eq!(capacity(&manager, name), Some(12345));
            manager.set_cache_capacity(alias, 0)?;
            assert_eq!(capacity(&manager, name), None);
        }
        Ok(())
    }

    /// Refine pages are reclaimed under memory pressure, as `release_cache_memory(Basic)` did.
    #[test]
    fn test_vector_refine_pages_are_reclaimable() -> Result<()> {
        use crate::VectorIndexFile;

        let manager =
            CacheManager::try_new(&CacheConfig::default(), &(1024 * 1024), "test", false)?;
        let pages = manager
            .kinds
            .mem_cache::<VectorIndexFile>(CacheKind::VectorRefine);
        pages.put(
            "a.index:0_fp16_data#bytes=0..9",
            VectorIndexFile::create(
                "0_fp16_data#bytes=0..9".to_string(),
                Bytes::from_static(b"fp16 page"),
            ),
        );
        assert!(pages.contains("a.index:0_fp16_data#bytes=0..9"));
        manager.kinds.shrink_for_pressure(&|| false);
        assert!(!pages.contains("a.index:0_fp16_data#bytes=0..9"));
        Ok(())
    }

    /// The refine cache always has a memory tier (`cache-vector-index-refine-size`,
    /// 0 disables it). Like other disk tiers, its disk tier
    /// (`disk-cache-vector-index-data-size`, 0 by default) exists only in EE mode
    /// and above `TABLE_DATA_DISK_CACHE_SIZE_THRESHOLD`.
    #[test]
    fn test_vector_refine_cache_tiers() -> Result<()> {
        use crate::VectorIndexFile;

        let temp_dir = TempDir::new()?;
        let cache_path = temp_dir.path().to_string_lossy();
        let config = config_with_disk_cache_enabled(&cache_path);
        assert!(
            config.disk_cache_vector_index_data_size as usize
                > TABLE_DATA_DISK_CACHE_SIZE_THRESHOLD
        );
        assert_eq!(CacheConfig::default().disk_cache_vector_index_data_size, 0);

        let capacity_of = |manager: &CacheManager, name: &str| {
            manager
                .cache_rows()
                .into_iter()
                .find(|r| r.name == name)
                .and_then(|r| r.capacity)
        };
        for ee_mode in [true, false] {
            let manager = CacheManager::try_new(&config, &(1024 * 1024), "test", ee_mode)?;
            assert_eq!(
                capacity_of(&manager, "memory_cache_vector_index_refine"),
                Some(config.vector_index_refine_size)
            );
            assert_eq!(
                capacity_of(&manager, "disk_cache_vector_index_refine"),
                ee_mode.then_some(config.disk_cache_vector_index_data_size)
            );
        }

        let no_memory = CacheConfig {
            vector_index_refine_size: 0,
            ..CacheConfig::default()
        };
        let manager = CacheManager::try_new(&no_memory, &(1024 * 1024), "test", false)?;
        assert_eq!(
            capacity_of(&manager, "memory_cache_vector_index_refine"),
            None
        );

        // Resizing the HNSW object cache leaves cached pages alone.
        let manager =
            CacheManager::try_new(&CacheConfig::default(), &(1024 * 1024), "test", false)?;
        let pages = manager
            .kinds
            .mem_cache::<VectorIndexFile>(CacheKind::VectorRefine);
        let data = Bytes::from_static(b"fp16 page");
        pages.put(
            "a.index:0_fp16_data#bytes=0..9",
            VectorIndexFile::create("0_fp16_data#bytes=0..9".to_string(), data.clone()),
        );
        manager.set_cache_capacity("memory_cache_vector_index_file", 0)?;
        assert_eq!(
            pages.lookup("a.index:0_fp16_data#bytes=0..9").unwrap().data,
            data
        );
        Ok(())
    }

    #[test]
    fn test_inverted_index_cache_capacities_are_independent() -> Result<()> {
        let config = CacheConfig {
            inverted_index_lookup_size: 128 * 1024,
            inverted_index_payload_size: 512 * 1024,
            ..Default::default()
        };
        let manager = CacheManager::try_new(&config, &(1024 * 1024), "test_tenant_id", false)?;
        let lookup = "memory_cache_inverted_index_lookup";
        let payload = "memory_cache_inverted_index_payload";
        let full_payload = "memory_cache_inverted_index_full_payload";
        assert_eq!(capacity(&manager, lookup), Some(128 * 1024));
        assert_eq!(capacity(&manager, payload), Some(384 * 1024));
        assert_eq!(capacity(&manager, full_payload), Some(128 * 1024));

        manager.set_cache_capacity("cache_inverted_index_meta_data", 12345)?;
        let meta = row(&manager, "memory_cache_inverted_index_meta_data").unwrap();
        assert_eq!((meta.capacity, meta.unit), (Some(12345), Unit::Count));

        manager.set_cache_capacity("cache_inverted_index_lookup", 256 * 1024)?;
        assert_eq!(capacity(&manager, lookup), Some(256 * 1024));
        assert_eq!(capacity(&manager, payload), Some(384 * 1024));

        // The payload size is split between pages and full components.
        manager.set_cache_capacity(payload, 768 * 1024)?;
        assert_eq!(capacity(&manager, lookup), Some(256 * 1024));
        assert_eq!(capacity(&manager, payload), Some(576 * 1024));
        assert_eq!(capacity(&manager, full_payload), Some(192 * 1024));
        Ok(())
    }

    #[test]
    fn test_segment_kinds_keep_legacy_capacity() -> Result<()> {
        let cache_config = CacheConfig {
            segment_block_metas_count: 10,
            table_meta_segment_bytes: 1000,
            segment_statistics_bytes: 2000,
            ..Default::default()
        };
        let manager = CacheManager::try_new(&cache_config, &(1 << 20), "test_tenant_id", false)?;
        let rows = manager.cache_rows();
        let capacity = |name: &str| {
            let row = rows.iter().find(|r| r.name == name).unwrap();
            (row.capacity, row.unit)
        };
        assert_eq!(
            capacity("memory_cache_segment_block_metas"),
            (Some(10), Unit::Count)
        );
        assert_eq!(
            capacity("memory_cache_compact_segment_info"),
            (Some(1000), Unit::Bytes)
        );
        assert_eq!(
            capacity("memory_cache_column_oriented_segment_info"),
            (Some(1000), Unit::Bytes)
        );
        assert_eq!(
            capacity("memory_cache_segment_statistics"),
            (Some(2000), Unit::Bytes)
        );
        // The per-block meta cache is gone; its name is no longer adjustable.
        assert!(
            manager
                .set_cache_capacity("memory_cache_block_meta", 1)
                .is_err()
        );
        Ok(())
    }

    fn empty_snapshot() -> TableSnapshot {
        TableSnapshot::try_new(
            None,
            None,
            TableSchema::empty(),
            Statistics::default(),
            vec![],
            None,
            None,
            TableMetaTimestamps::new(None, chrono::Duration::hours(1)),
        )
        .unwrap()
    }

    #[test]
    fn test_snapshot_caches_keep_legacy_capacity() -> Result<()> {
        let config = CacheConfig {
            table_meta_snapshot_count: 2,
            table_meta_statistic_count: 3,
            ..Default::default()
        };
        let manager = CacheManager::try_new(&config, &(1 << 20), "test_tenant_id", false)?;
        let rows: Vec<_> = manager.cache_rows().into_iter().take(2).collect();
        let names: Vec<_> = rows.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, [
            "memory_cache_table_snapshot",
            "memory_cache_table_statistics"
        ]);
        assert!(
            rows.iter()
                .all(|r| r.unit == Unit::Count && r.num_items == 0)
        );
        assert_eq!((rows[0].capacity, rows[1].capacity), (Some(2), Some(3)));

        let snapshots = manager
            .kinds
            .mem_cache::<Arc<TableSnapshot>>(CacheKind::TableSnapshot);
        for i in 0..3 {
            snapshots.put(&i.to_string(), Arc::new(empty_snapshot()));
        }
        assert_eq!(manager.cache_rows()[0].num_items, 2);

        // Legacy names resize in the legacy unit.
        manager.set_cache_capacity("memory_cache_table_snapshot", 1)?;
        let row = &manager.cache_rows()[0];
        assert_eq!((row.num_items, row.capacity), (1, Some(1)));
        assert!(manager.set_cache_capacity("no_such_cache", 1).is_err());
        Ok(())
    }

    #[test]
    fn test_snapshot_caches_follow_enable_table_meta_cache() -> Result<()> {
        let config = CacheConfig {
            enable_table_meta_cache: false,
            ..Default::default()
        };
        let manager = CacheManager::try_new(&config, &(1 << 20), "test_tenant_id", false)?;
        assert!(
            manager
                .cache_rows()
                .iter()
                .all(|r| r.pool == Some(Pool::Data))
        );

        // Enabling at runtime creates an item-bounded cache, as before.
        manager.set_cache_capacity("memory_cache_table_snapshot", 4)?;
        let row = row(&manager, "memory_cache_table_snapshot").unwrap();
        assert_eq!((row.capacity, row.unit), (Some(4), Unit::Count));
        Ok(())
    }

    fn shared_config() -> CacheConfig {
        CacheConfig {
            shared_memory_enabled: true,
            ..CacheConfig::default()
        }
    }

    #[test]
    fn test_shared_backend_sizes_pools_by_ratio() -> Result<()> {
        let config = CacheConfig {
            // Legacy options are ignored by the shared backend.
            enable_table_meta_cache: false,
            table_meta_snapshot_count: 1,
            kind_max_bytes: [
                ("vector_index".to_string(), 1234),
                ("memory_cache_column_data".to_string(), 5000),
            ]
            .into(),
            ..shared_config()
        };
        assert_eq!(ignored_legacy_options(&config), [
            "enable_table_meta_cache",
            "table_meta_snapshot_count"
        ]);
        let manager = CacheManager::try_new(&config, &1_000_000, "test", false)?;
        assert!(manager.is_shared());
        assert_eq!(capacity(&manager, "__pool:meta"), Some(100_000));
        assert_eq!(capacity(&manager, "__pool:index"), Some(400_000));
        assert_eq!(capacity(&manager, "__pool:data"), Some(100_000));
        let snapshot = row(&manager, "memory_cache_table_snapshot").unwrap();
        assert_eq!((snapshot.capacity, snapshot.unit), (None, Unit::Bytes));
        assert_eq!(
            capacity(&manager, "memory_cache_vector_index_file"),
            Some(1234)
        );
        assert_eq!(capacity(&manager, "memory_cache_column_data"), Some(5000));
        // Off unless given a limit.
        assert!(row(&manager, "memory_cache_table_data").is_none());

        // Pools can be resized; the payload size is not split as in the legacy backend.
        manager.set_cache_capacity("pool:index", 0)?;
        assert!(row(&manager, "__pool:index").is_none());
        assert!(row(&manager, "memory_cache_bloom_index_filter").is_none());
        manager.set_cache_capacity("pool:index", 1000)?;
        manager.set_cache_capacity("memory_cache_inverted_index_payload", 800)?;
        assert_eq!(
            capacity(&manager, "memory_cache_inverted_index_payload"),
            Some(800)
        );
        assert_eq!(
            capacity(&manager, "memory_cache_inverted_index_full_payload"),
            None
        );
        Ok(())
    }

    #[test]
    fn test_shared_backend_rejects_bad_config() {
        let too_much = CacheConfig {
            meta_max_ratio: 50,
            index_max_ratio: 50,
            ..shared_config()
        };
        assert!(CacheManager::try_new(&too_much, &1_000_000, "test", false).is_err());
        let unknown = CacheConfig {
            kind_max_bytes: [("no_such_cache".to_string(), 1)].into(),
            ..shared_config()
        };
        assert!(CacheManager::try_new(&unknown, &1_000_000, "test", false).is_err());
    }

    /// Pool names exist only in the shared backend.
    #[test]
    fn test_legacy_backend_rejects_pool_names() -> Result<()> {
        let manager = CacheManager::try_new(&CacheConfig::default(), &(1 << 20), "test", false)?;
        assert!(!manager.is_shared());
        assert!(manager.set_cache_capacity("pool:meta", 1).is_err());
        assert!(manager.set_cache_capacity("all", 0).is_err());
        assert!(
            !manager
                .cache_rows()
                .iter()
                .any(|r| r.name.starts_with("__pool"))
        );
        Ok(())
    }

    #[test]
    fn test_disk_cache_size_threshold() -> Result<()> {
        // Create a temporary directory for the test
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_path_buf();

        // Test parameters
        let cache_name = "test_threshold_cache".to_string();
        let population_queue_size = 5;
        let ee_mode = true; // Always use EE mode for this test
        let sync_data = false;
        let key_reload_policy = DiskCacheKeyReloadPolicy::Fuzzy;

        // Case 1: Size below threshold (should disable cache)
        let below_threshold_size = TABLE_DATA_DISK_CACHE_SIZE_THRESHOLD - 1;
        let result = CacheManager::new_on_disk_cache(
            cache_name.clone(),
            &cache_path,
            population_queue_size,
            below_threshold_size,
            key_reload_policy.clone(),
            sync_data,
            ee_mode,
        )?;
        assert!(
            result.is_none(),
            "Disk cache should be disabled when size is below threshold"
        );

        // Case 2: Size exactly at threshold (should disable cache)
        let at_threshold_size = TABLE_DATA_DISK_CACHE_SIZE_THRESHOLD;
        let result = CacheManager::new_on_disk_cache(
            cache_name.clone(),
            &cache_path,
            population_queue_size,
            at_threshold_size,
            key_reload_policy.clone(),
            sync_data,
            ee_mode,
        )?;
        assert!(
            result.is_none(),
            "Disk cache should be disabled when size equals threshold"
        );

        // Case 3: Size above threshold (should enable cache)
        let above_threshold_size = TABLE_DATA_DISK_CACHE_SIZE_THRESHOLD + 1024;
        let result = CacheManager::new_on_disk_cache(
            cache_name.clone(),
            &cache_path,
            population_queue_size,
            above_threshold_size,
            key_reload_policy.clone(),
            sync_data,
            ee_mode,
        )?;
        assert!(
            result.is_some(),
            "Disk cache should be enabled when size is above threshold"
        );

        Ok(())
    }
}
