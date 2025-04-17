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

use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_config::CacheConfig;
use databend_common_config::CacheStorageTypeInnerConfig;
use databend_common_config::DiskCacheKeyReloadPolicy;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::info;
use parking_lot::RwLock;

use crate::caches::BlockMetaCache;
use crate::caches::BloomIndexFilterCache;
use crate::caches::BloomIndexMetaCache;
use crate::caches::CacheValue;
use crate::caches::ColumnArrayCache;
use crate::caches::ColumnOrientedSegmentInfoCache;
use crate::caches::CompactSegmentInfoCache;
use crate::caches::IcebergTableCache;
use crate::caches::InvertedIndexFileCache;
use crate::caches::InvertedIndexMetaCache;
use crate::caches::ParquetMetaDataCache;
use crate::caches::PrunePartitionsCache;
use crate::caches::SegmentBlockMetasCache;
use crate::caches::TableSnapshotCache;
use crate::caches::TableSnapshotStatisticCache;
use crate::providers::HybridCache;
use crate::DiskCacheAccessor;
use crate::DiskCacheBuilder;
use crate::InMemoryLruCache;
use crate::Unit;

static DEFAULT_PARQUET_META_DATA_CACHE_ITEMS: usize = 3000;

struct CacheSlot<T> {
    cache: RwLock<Option<T>>,
}

impl<T> CacheSlot<T> {
    fn new(t: Option<T>) -> CacheSlot<T> {
        CacheSlot {
            cache: RwLock::new(t),
        }
    }

    fn set(&self, t: Option<T>) {
        let mut guard = self.cache.write();
        *guard = t
    }
}

impl<T: Clone> CacheSlot<T> {
    fn get(&self) -> Option<T> {
        self.cache.read().clone()
    }
}

/// Where all the caches reside
pub struct CacheManager {
    table_snapshot_cache: CacheSlot<TableSnapshotCache>,
    table_statistic_cache: CacheSlot<TableSnapshotStatisticCache>,
    compact_segment_info_cache: CacheSlot<CompactSegmentInfoCache>,
    column_oriented_segment_info_cache: CacheSlot<ColumnOrientedSegmentInfoCache>,
    bloom_index_filter_cache: CacheSlot<BloomIndexFilterCache>,
    bloom_index_meta_cache: CacheSlot<BloomIndexMetaCache>,
    inverted_index_meta_cache: CacheSlot<InvertedIndexMetaCache>,
    inverted_index_file_cache: CacheSlot<InvertedIndexFileCache>,
    prune_partitions_cache: CacheSlot<PrunePartitionsCache>,
    parquet_meta_data_cache: CacheSlot<ParquetMetaDataCache>,
    table_data_cache: CacheSlot<DiskCacheAccessor>,
    in_memory_table_data_cache: CacheSlot<ColumnArrayCache>,
    segment_block_metas_cache: CacheSlot<SegmentBlockMetasCache>,
    block_meta_cache: CacheSlot<BlockMetaCache>,

    // icebergs
    iceberg_table_meta_cache: CacheSlot<IcebergTableCache>,

    /// Determines whether disk caches can be used at runtime.
    ///
    /// This flag allows or disallows disk caches, but the cache configuration takes precedence:
    /// - If set to `false`, disk caches are disabled, even if enabled in the configuration.
    /// - If set to `true`, disk caches are only enabled if also enabled in the configuration.
    ///
    /// In non-EE mode, disk caches are always disabled.
    allows_on_disk_cache: AtomicBool,
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

    /// Initialize the caches according to the relevant configurations.
    fn try_new(
        config: &CacheConfig,
        max_server_memory_usage: &u64,
        tenant_id: impl Into<String>,
        ee_mode: bool,
    ) -> Result<Self> {
        let tenant_id = tenant_id.into();
        let on_disk_cache_sync_data = config.disk_cache_config.sync_data;
        let on_disk_cache_need_sync_data = config.disk_cache_config.sync_data;
        let allows_on_disk_cache = AtomicBool::new(ee_mode);

        let on_disk_cache_queue_size: u32 = if config.table_data_cache_population_queue_size > 0 {
            config.table_data_cache_population_queue_size
        } else {
            std::cmp::max(
                1,
                std::thread::available_parallelism()
                    .expect("Cannot get thread count")
                    .get() as u32,
            ) * 5
        };

        // setup table data cache
        let table_data_cache = {
            match config.data_cache_storage {
                CacheStorageTypeInnerConfig::None => CacheSlot::new(None),
                CacheStorageTypeInnerConfig::Disk => {
                    let table_data_on_disk_cache_path =
                        PathBuf::from(&config.disk_cache_config.path)
                            .join(tenant_id.clone())
                            .join("v1");

                    info!(
                        "On-disk table data cache enabled, cache population queue size {}",
                        on_disk_cache_queue_size
                    );

                    let cache = Self::new_on_disk_cache(
                        DISK_TABLE_DATA_CACHE_NAME.to_owned(),
                        &table_data_on_disk_cache_path,
                        on_disk_cache_queue_size,
                        config.disk_cache_config.max_bytes as usize,
                        config.data_cache_key_reload_policy.clone(),
                        on_disk_cache_need_sync_data,
                        ee_mode,
                    )?;
                    CacheSlot::new(cache)
                }
            }
        };

        // setup in-memory table column cache
        let memory_cache_capacity = if config.table_data_deserialized_data_bytes != 0 {
            config.table_data_deserialized_data_bytes as usize
        } else {
            (*max_server_memory_usage as usize)
                * config.table_data_deserialized_memory_ratio as usize
                / 100
        };

        // Cache of deserialized table data
        let in_memory_table_data_cache =
            Self::new_bytes_cache_slot(MEMORY_CACHE_TABLE_DATA, memory_cache_capacity);

        let instance =
        // setup in-memory table meta cache
        if !config.enable_table_meta_cache {
            Self {
                table_snapshot_cache: CacheSlot::new(None),
                compact_segment_info_cache: CacheSlot::new(None),
                bloom_index_filter_cache: CacheSlot::new(None),
                bloom_index_meta_cache: CacheSlot::new(None),
                column_oriented_segment_info_cache: CacheSlot::new(None),
                inverted_index_meta_cache: CacheSlot::new(None),
                inverted_index_file_cache: CacheSlot::new(None),
                prune_partitions_cache: CacheSlot::new(None),
                parquet_meta_data_cache: CacheSlot::new(None),
                table_statistic_cache: CacheSlot::new(None),
                table_data_cache,
                in_memory_table_data_cache,
                segment_block_metas_cache: CacheSlot::new(None),
                block_meta_cache: CacheSlot::new(None),
                iceberg_table_meta_cache: CacheSlot::new(None),
                allows_on_disk_cache,
            }
        } else {
            let table_snapshot_cache = Self::new_items_cache_slot(
                MEMORY_CACHE_TABLE_SNAPSHOT,
                config.table_meta_snapshot_count as usize,
            );
            let table_statistic_cache = Self::new_items_cache_slot(
                MEMORY_CACHE_TABLE_STATISTICS,
                config.table_meta_statistic_count as usize,
            );
            let compact_segment_info_cache = Self::new_bytes_cache_slot(
                MEMORY_CACHE_COMPACT_SEGMENT_INFO,
                config.table_meta_segment_bytes as usize,
            );
            let column_oriented_segment_info_cache = Self::new_bytes_cache_slot(
                MEMORY_CACHE_COLUMN_ORIENTED_SEGMENT_INFO,
                config.table_meta_segment_bytes as usize,
            );
            let bloom_index_filter_cache = {
                let bloom_filter_on_disk_cache_path = PathBuf::from(&config.disk_cache_config.path)
                    .join(tenant_id.clone())
                    .join("bloom_filter_v1");
                Self::new_hybrid_cache_slot(
                    HYBRID_CACHE_BLOOM_INDEX_FILTER,
                    config.table_bloom_index_filter_size as usize,
                    Unit::Bytes,
                    &bloom_filter_on_disk_cache_path,
                    on_disk_cache_queue_size,
                    config.disk_cache_table_bloom_index_data_size as usize,
                    DiskCacheKeyReloadPolicy::Fuzzy,
                    on_disk_cache_sync_data,
                    ee_mode,
                )?
            };

            let bloom_index_meta_cache = {
                let bloom_filter_meta_on_disk_cache_path =
                    PathBuf::from(&config.disk_cache_config.path)
                        .join(tenant_id)
                        .join("bloom_meta_v1");
                Self::new_hybrid_cache_slot(
                    HYBRID_CACHE_BLOOM_INDEX_FILE_META_DATA,
                    config.table_bloom_index_meta_count as usize,
                    Unit::Count,
                    &bloom_filter_meta_on_disk_cache_path,
                    on_disk_cache_queue_size,
                    config.disk_cache_table_bloom_index_meta_size as usize,
                    DiskCacheKeyReloadPolicy::Fuzzy,
                    on_disk_cache_sync_data,
                    ee_mode,
                )?
            };

            let inverted_index_meta_cache = Self::new_items_cache_slot(
                MEMORY_CACHE_INVERTED_INDEX_FILE_META_DATA,
                config.inverted_index_meta_count as usize,
            );

            // setup in-memory inverted index filter cache
            let inverted_index_file_size = if config.inverted_index_filter_memory_ratio != 0 {
                (*max_server_memory_usage as usize)
                    * config.inverted_index_filter_memory_ratio as usize
                    / 100
            } else {
                config.inverted_index_filter_size as usize
            };
            let inverted_index_file_cache = Self::new_bytes_cache_slot(
                MEMORY_CACHE_INVERTED_INDEX_FILE,
                inverted_index_file_size,
            );
            let prune_partitions_cache = Self::new_items_cache_slot(
                MEMORY_CACHE_PRUNE_PARTITIONS,
                config.table_prune_partitions_count as usize,
            );

            let parquet_meta_data_cache = Self::new_items_cache_slot(
                MEMORY_CACHE_PARQUET_META_DATA,
                DEFAULT_PARQUET_META_DATA_CACHE_ITEMS,
            );

            let segment_block_metas_cache = Self::new_items_cache_slot(
                MEMORY_CACHE_SEGMENT_BLOCK_METAS,
                config.block_meta_count as usize,
            );

            let block_meta_cache = Self::new_items_cache_slot(
                MEMORY_CACHE_BLOCK_META,
                // TODO replace this config
                config.block_meta_count as usize,
            );

            let iceberg_table_meta_cache = Self::new_items_cache_slot(
                MEMORY_CACHE_ICEBERG_TABLE,
                config.iceberg_table_meta_count as usize,
            );

            Self {
                table_snapshot_cache,
                compact_segment_info_cache,
                column_oriented_segment_info_cache,
                bloom_index_filter_cache,
                bloom_index_meta_cache,
                inverted_index_meta_cache,
                inverted_index_file_cache,
                prune_partitions_cache,
                table_statistic_cache,
                table_data_cache,
                in_memory_table_data_cache,
                segment_block_metas_cache,
                parquet_meta_data_cache,
                block_meta_cache,
                iceberg_table_meta_cache,
                allows_on_disk_cache,
            }
        };

        Ok(instance)
    }

    pub fn instance() -> Arc<CacheManager> {
        GlobalInstance::get()
    }

    pub fn get_table_snapshot_cache(&self) -> Option<TableSnapshotCache> {
        self.table_snapshot_cache.get()
    }

    pub fn set_cache_capacity(&self, name: &str, new_capacity: u64) -> Result<()> {
        match name {
            MEMORY_CACHE_TABLE_DATA => {
                let cache = &self.in_memory_table_data_cache;
                Self::set_bytes_capacity(cache, new_capacity, name);
            }
            MEMORY_CACHE_PARQUET_META_DATA => {
                let cache = &self.parquet_meta_data_cache;
                Self::set_items_capacity(cache, new_capacity, name)
            }
            MEMORY_CACHE_PRUNE_PARTITIONS => {
                let cache = &self.prune_partitions_cache;
                Self::set_items_capacity(cache, new_capacity, name)
            }
            MEMORY_CACHE_INVERTED_INDEX_FILE => {
                let cache = &self.inverted_index_file_cache;
                Self::set_bytes_capacity(cache, new_capacity, name);
            }
            MEMORY_CACHE_INVERTED_INDEX_FILE_META_DATA => {
                let cache = &self.inverted_index_meta_cache;
                Self::set_items_capacity(cache, new_capacity, name);
            }
            HYBRID_CACHE_BLOOM_INDEX_FILE_META_DATA
            | IN_MEMORY_CACHE_BLOOM_INDEX_FILE_META_DATA => {
                Self::set_hybrid_cache_items_capacity(
                    &self.bloom_index_meta_cache,
                    new_capacity,
                    name,
                );
            }
            HYBRID_CACHE_BLOOM_INDEX_FILTER | IN_MEMORY_HYBRID_CACHE_BLOOM_INDEX_FILTER => {
                Self::set_hybrid_cache_bytes_capacity(
                    &self.bloom_index_filter_cache,
                    new_capacity,
                    name,
                );
            }
            MEMORY_CACHE_COMPACT_SEGMENT_INFO => {
                Self::set_bytes_capacity(&self.compact_segment_info_cache, new_capacity, name);
            }
            MEMORY_CACHE_TABLE_STATISTICS => {
                Self::set_items_capacity(&self.table_statistic_cache, new_capacity, name);
            }
            MEMORY_CACHE_TABLE_SNAPSHOT => {
                Self::set_items_capacity(&self.table_snapshot_cache, new_capacity, name);
            }
            MEMORY_CACHE_SEGMENT_BLOCK_METAS => {
                Self::set_items_capacity(&self.segment_block_metas_cache, new_capacity, name);
            }
            MEMORY_CACHE_BLOCK_META => {
                Self::set_items_capacity(&self.block_meta_cache, new_capacity, name);
            }

            DISK_TABLE_DATA_CACHE_NAME => {
                return Err(ErrorCode::BadArguments(format!(
                    "set capacity of cache {} is not allowed",
                    name
                )));
            }
            _ => {
                return Err(ErrorCode::BadArguments(format!(
                    "cache {} not found, or not allowed to be adjusted",
                    name
                )));
            }
        }
        Ok(())
    }

    fn set_bytes_capacity<T: Into<CacheValue<T>>>(
        cache: &CacheSlot<InMemoryLruCache<T>>,
        new_capacity: u64,
        name: impl Into<String>,
    ) {
        if let Some(v) = cache.get() {
            v.set_bytes_capacity(new_capacity as usize);
        } else {
            let new_cache = Self::new_bytes_cache(name, new_capacity as usize);
            cache.set(new_cache)
        }
    }

    fn set_items_capacity<T: Into<CacheValue<T>>>(
        cache: &CacheSlot<InMemoryLruCache<T>>,
        new_capacity: u64,
        name: impl Into<String>,
    ) {
        if let Some(v) = cache.get() {
            v.set_items_capacity(new_capacity as usize);
        } else {
            let new_cache = Self::new_items_cache(name, new_capacity as usize);
            cache.set(new_cache)
        }
    }

    fn set_hybrid_cache_items_capacity<T: Into<CacheValue<T>>>(
        cache: &CacheSlot<HybridCache<T>>,
        new_capacity: u64,
        name: impl Into<String>,
    ) {
        if let Some(v) = cache.get() {
            v.in_memory_cache()
                .set_items_capacity(new_capacity as usize);
        } else {
            // In this case, only in-memory cache will be built, on-disk cache is NOT allowed
            // to be enabled this way yet.
            let name = name.into();
            let in_memory_cache_name = HybridCache::<T>::in_memory_cache_name(&name);
            if let Some(new_cache) =
                Self::new_items_cache(in_memory_cache_name, new_capacity as usize)
            {
                let hybrid_cache = HybridCache::new(name, new_cache, None);
                cache.set(Some(hybrid_cache));
            }
        }
    }

    fn set_hybrid_cache_bytes_capacity<T: Into<CacheValue<T>>>(
        cache: &CacheSlot<HybridCache<T>>,
        new_capacity: u64,
        name: impl Into<String>,
    ) {
        if let Some(v) = cache.get() {
            v.in_memory_cache()
                .set_bytes_capacity(new_capacity as usize);
        } else {
            // In this case, only in-memory cache will be built, on-disk cache is NOT allowed
            // to be enabled this way yet.
            let name = name.into();
            let in_memory_cache_name = HybridCache::<T>::in_memory_cache_name(&name);
            if let Some(new_cache) =
                Self::new_bytes_cache(in_memory_cache_name, new_capacity as usize)
            {
                let hybrid_cache = HybridCache::new(name, new_cache, None);
                cache.set(Some(hybrid_cache));
            }
        }
    }

    pub fn get_segment_block_metas_cache(&self) -> Option<SegmentBlockMetasCache> {
        self.segment_block_metas_cache.get()
    }

    pub fn get_block_meta_cache(&self) -> Option<BlockMetaCache> {
        self.block_meta_cache.get()
    }

    pub fn get_iceberg_table_cache(&self) -> Option<IcebergTableCache> {
        self.iceberg_table_meta_cache.get()
    }

    pub fn get_table_snapshot_statistics_cache(&self) -> Option<TableSnapshotStatisticCache> {
        self.table_statistic_cache.get()
    }

    pub fn get_table_segment_cache(&self) -> Option<CompactSegmentInfoCache> {
        self.compact_segment_info_cache.get()
    }

    pub fn get_bloom_index_filter_cache(&self) -> Option<BloomIndexFilterCache> {
        self.get_hybrid_cache(self.bloom_index_filter_cache.get())
    }

    pub fn get_bloom_index_meta_cache(&self) -> Option<BloomIndexMetaCache> {
        self.get_hybrid_cache(self.bloom_index_meta_cache.get())
    }

    fn get_hybrid_cache<T>(&self, cache: Option<HybridCache<T>>) -> Option<HybridCache<T>>
    where CacheValue<T>: From<T> {
        if let Some(cache) = cache {
            if self.allows_on_disk_cache.load(Ordering::Relaxed) {
                // Returns the original cache as it is, which may or may not have on-disk cache
                Some(cache)
            } else {
                // Toggle off on-disk cache
                Some(cache.toggle_off_disk_cache())
            }
        } else {
            None
        }
    }

    pub fn get_inverted_index_meta_cache(&self) -> Option<InvertedIndexMetaCache> {
        self.inverted_index_meta_cache.get()
    }

    pub fn get_inverted_index_file_cache(&self) -> Option<InvertedIndexFileCache> {
        self.inverted_index_file_cache.get()
    }

    pub fn get_prune_partitions_cache(&self) -> Option<PrunePartitionsCache> {
        self.prune_partitions_cache.get()
    }

    pub fn get_parquet_meta_data_cache(&self) -> Option<ParquetMetaDataCache> {
        self.parquet_meta_data_cache.get()
    }

    pub fn get_table_data_cache(&self) -> Option<DiskCacheAccessor> {
        if self.allows_on_disk_cache.load(Ordering::Relaxed) {
            // If on-disk cache is allowed, return it as it is (which may be some cache, or none)
            self.table_data_cache.get()
        } else {
            None
        }
    }

    pub fn get_table_data_array_cache(&self) -> Option<ColumnArrayCache> {
        self.in_memory_table_data_cache.get()
    }

    pub fn get_column_oriented_segment_info_cache(&self) -> Option<ColumnOrientedSegmentInfoCache> {
        self.column_oriented_segment_info_cache.get()
    }

    pub fn set_allows_disk_cache(&self, flag: bool) {
        self.allows_on_disk_cache.store(flag, Ordering::Relaxed)
    }

    fn new_items_cache_slot<V: Into<CacheValue<V>>>(
        name: impl Into<String>,
        capacity: usize,
    ) -> CacheSlot<InMemoryLruCache<V>> {
        CacheSlot::new(Self::new_items_cache(name, capacity))
    }

    fn new_items_cache<V: Into<CacheValue<V>>>(
        name: impl Into<String>,
        capacity: usize,
    ) -> Option<InMemoryLruCache<V>> {
        match capacity {
            0 => None,
            _ => Some(InMemoryLruCache::with_items_capacity(name.into(), capacity)),
        }
    }

    fn new_bytes_cache_slot<V: Into<CacheValue<V>>>(
        name: impl Into<String>,
        bytes_capacity: usize,
    ) -> CacheSlot<InMemoryLruCache<V>> {
        CacheSlot::new(Self::new_bytes_cache(name, bytes_capacity))
    }

    fn new_bytes_cache<V: Into<CacheValue<V>>>(
        name: impl Into<String>,
        bytes_capacity: usize,
    ) -> Option<InMemoryLruCache<V>> {
        match bytes_capacity {
            0 => None,
            _ => Some(InMemoryLruCache::with_bytes_capacity(
                name.into(),
                bytes_capacity,
            )),
        }
    }

    fn new_on_disk_cache(
        cache_name: String,
        path: &PathBuf,
        population_queue_size: u32,
        disk_cache_bytes_size: usize,
        disk_cache_key_reload_policy: DiskCacheKeyReloadPolicy,
        sync_data: bool,
        ee_mode: bool,
    ) -> Result<Option<DiskCacheAccessor>> {
        if disk_cache_bytes_size == 0 || !ee_mode {
            Ok(None)
        } else {
            let cache_holder = DiskCacheBuilder::try_build_disk_cache(
                cache_name,
                path,
                population_queue_size,
                disk_cache_bytes_size,
                disk_cache_key_reload_policy,
                sync_data,
            )?;
            Ok(Some(cache_holder))
        }
    }

    fn new_hybrid_cache_slot<V: Into<CacheValue<V>>>(
        name: impl Into<String>,
        in_memory_cache_capacity: usize,
        unit: Unit,
        disk_cache_path: &PathBuf,
        disk_cache_population_queue_size: u32,
        disk_cache_bytes_size: usize,
        disk_cache_key_reload_policy: DiskCacheKeyReloadPolicy,
        sync_data: bool,
        ee_mode: bool,
    ) -> Result<CacheSlot<HybridCache<V>>> {
        let name = name.into();
        let in_mem_cache_name = HybridCache::<V>::in_memory_cache_name(&name);
        let disk_cache_name = HybridCache::<V>::on_disk_cache_name(&name);

        // Note that here we allow the capacity of in-memory cache to be zero,
        // this may be handy for some performance testing scenarios
        let in_memory_cache = match unit {
            Unit::Bytes => {
                InMemoryLruCache::with_bytes_capacity(in_mem_cache_name, in_memory_cache_capacity)
            }
            Unit::Count => {
                InMemoryLruCache::with_items_capacity(in_mem_cache_name, in_memory_cache_capacity)
            }
        };

        let on_disk_cache = Self::new_on_disk_cache(
            disk_cache_name,
            disk_cache_path,
            disk_cache_population_queue_size,
            disk_cache_bytes_size,
            disk_cache_key_reload_policy,
            sync_data,
            ee_mode,
        )?;

        Ok(CacheSlot::new(Some(HybridCache::new(
            name,
            in_memory_cache,
            on_disk_cache,
        ))))
    }
}

const MEMORY_CACHE_TABLE_DATA: &str = "memory_cache_table_data";
const MEMORY_CACHE_PARQUET_META_DATA: &str = "memory_cache_parquet_meta_data";
const MEMORY_CACHE_PRUNE_PARTITIONS: &str = "memory_cache_prune_partitions";
const MEMORY_CACHE_INVERTED_INDEX_FILE: &str = "memory_cache_inverted_index_file";
const MEMORY_CACHE_INVERTED_INDEX_FILE_META_DATA: &str =
    "memory_cache_inverted_index_file_meta_data";

const HYBRID_CACHE_BLOOM_INDEX_FILE_META_DATA: &str = "cache_bloom_index_file_meta_data";

const IN_MEMORY_CACHE_BLOOM_INDEX_FILE_META_DATA: &str = "memory_cache_bloom_index_file_meta_data";
const HYBRID_CACHE_BLOOM_INDEX_FILTER: &str = "cache_bloom_index_filter";
const IN_MEMORY_HYBRID_CACHE_BLOOM_INDEX_FILTER: &str = "memory_cache_bloom_index_filter";
const MEMORY_CACHE_COMPACT_SEGMENT_INFO: &str = "memory_cache_compact_segment_info";
const MEMORY_CACHE_COLUMN_ORIENTED_SEGMENT_INFO: &str = "memory_cache_column_oriented_segment_info";
const MEMORY_CACHE_TABLE_STATISTICS: &str = "memory_cache_table_statistics";
const MEMORY_CACHE_TABLE_SNAPSHOT: &str = "memory_cache_table_snapshot";
const MEMORY_CACHE_SEGMENT_BLOCK_METAS: &str = "memory_cache_segment_block_metas";
const MEMORY_CACHE_ICEBERG_TABLE: &str = "memory_cache_iceberg_table";

const MEMORY_CACHE_BLOCK_META: &str = "memory_cache_block_meta";

const DISK_TABLE_DATA_CACHE_NAME: &str = "disk_cache_table_data";

#[cfg(test)]
mod tests {
    use databend_common_config::CacheConfig;
    use databend_common_config::DiskCacheInnerConfig;
    use tempfile::TempDir;

    use super::*;
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
            ..CacheConfig::default()
        }
    }

    fn config_with_disk_cache_disabled() -> CacheConfig {
        CacheConfig {
            data_cache_storage: CacheStorageTypeInnerConfig::None,
            disk_cache_table_bloom_index_data_size: 0,
            disk_cache_table_bloom_index_meta_size: 0,
            ..CacheConfig::default()
        }
    }

    fn all_disk_cache_enabled(cache_manager: &CacheManager) -> bool {
        cache_manager.get_table_data_cache().is_some()
            && cache_manager
                .get_bloom_index_meta_cache()
                .unwrap()
                .on_disk_cache()
                .is_some()
            && cache_manager
                .get_bloom_index_meta_cache()
                .unwrap()
                .on_disk_cache()
                .is_some()
    }

    fn all_disk_cache_disabled(cache_manager: &CacheManager) -> bool {
        cache_manager.get_table_data_cache().is_none()
            && cache_manager
                .get_bloom_index_meta_cache()
                .unwrap()
                .on_disk_cache()
                .is_none()
            && cache_manager
                .get_bloom_index_meta_cache()
                .unwrap()
                .on_disk_cache()
                .is_none()
    }

    #[test]
    fn test_cache_manager_ee_mode() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_string_lossy().clone();

        let max_server_memory_usage = 1024 * 1024;

        // The behaviors we expected in the EE mode:
        // - If disk caches are enabled in the configuration:
        //   They could be toggled off and on at runtime
        // - If disk caches are disabled in the configuration:
        //   They could not be toggled on at runtime

        let ee_mode = true;

        // Suite 1: Create cache config with disk cache enabled
        let cache_config = config_with_disk_cache_enabled(&cache_path);
        // In EE mode
        let cache_manager = CacheManager::try_new(
            &cache_config,
            &max_server_memory_usage,
            "test_tenant_id",
            ee_mode,
        )?;

        // All disk caches should be enabled as specified in the configuration
        assert!(all_disk_cache_enabled(&cache_manager));

        // All disk caches should be disabled, if toggled off disk caches
        cache_manager.set_allows_disk_cache(false);
        assert!(all_disk_cache_disabled(&cache_manager));

        // All disk caches should be enabled, if toggled on disk caches
        cache_manager.set_allows_disk_cache(true);
        assert!(all_disk_cache_enabled(&cache_manager));

        // Suite 2: Create cache config with disk cache disabled
        let cache_config = config_with_disk_cache_disabled();
        let cache_manager = CacheManager::try_new(
            &cache_config,
            &max_server_memory_usage,
            "test_tenant_id",
            ee_mode,
        )?;

        // All disk caches should be disabled as specified in the configuration
        assert!(all_disk_cache_disabled(&cache_manager));

        // All disk caches should still be disabled, even if toggled on disk caches
        cache_manager.set_allows_disk_cache(true);
        assert!(all_disk_cache_disabled(&cache_manager));

        Ok(())
    }

    #[test]
    fn test_cache_manager_non_ee_mode() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_string_lossy().clone();
        let max_server_memory_usage = 1024 * 1024;

        // The behaviors we expected in the NON-EE mode:
        // Even if disk caches are enabled in the configuration:
        // - Disk caches will not be instantiated
        // - They could not be toggled on at runtime

        // Create cache config with disk cache enabled
        let cache_config = config_with_disk_cache_enabled(&cache_path);

        // In NON-EE mode
        let ee_mode = false;
        let cache_manager = CacheManager::try_new(
            &cache_config,
            &max_server_memory_usage,
            "test_tenant_id",
            ee_mode,
        )?;

        // All disk caches should be disabled, even if they are enabled in the configuration
        assert!(all_disk_cache_disabled(&cache_manager));

        // All disk caches should be disabled, even if disk caches are toggled on
        cache_manager.set_allows_disk_cache(true);
        assert!(all_disk_cache_disabled(&cache_manager));

        Ok(())
    }
}
