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
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use databend_common_base::base::GlobalInstance;
use databend_common_config::CacheConfig;
use databend_common_config::CacheStorageTypeInnerConfig;
use databend_common_config::DiskCacheKeyReloadPolicy;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::info;
use parking_lot::RwLock;

use crate::CacheAccessor;
use crate::DiskCacheAccessor;
use crate::DiskCacheBuilder;
use crate::InMemoryLruCache;
use crate::SegmentStatisticsCache;
use crate::Unit;
use crate::caches::BlockMetaCache;
use crate::caches::BloomIndexFilterCache;
use crate::caches::BloomIndexMetaCache;
use crate::caches::CacheValue;
use crate::caches::ColumnArrayCache;
use crate::caches::ColumnDataCache;
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
use crate::caches::VectorIndexFileCache;
use crate::caches::VectorIndexMetaCache;
use crate::caches::VirtualColumnMetaCache;
use crate::providers::HybridCache;
use crate::providers::HybridCacheExt;

static DEFAULT_PARQUET_META_DATA_CACHE_ITEMS: usize = 3000;

// Minimum threshold for table data disk cache size (in bytes).
// Any configuration value less than this threshold will be ignored,
// and table data disk cache will not be enabled.
// This threshold exists to accommodate the current cloud platform logic:
// When attempting to disable table data cache in compute node configurations, setting the disk
// cache size to zero prevents the physical volume from being loaded, so this threshold provides
// a better approach.
// Eventually, we should refactor the compute node configurations instead, to make those options more sensible.
const TABLE_DATA_DISK_CACHE_SIZE_THRESHOLD: usize = 1024;

#[derive(Default)]
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

/// Specifies the level of aggression when clearing memory caches.
pub enum CacheClearanceLevel {
    /// Clears only basic caches that may consume significant memory
    Basic,
    /// Performs a more aggressive clearing, including both basic caches and extra caches
    Deep,
}

/// Where all the caches reside
pub struct CacheManager {
    table_snapshot_cache: CacheSlot<TableSnapshotCache>,
    table_statistic_cache: CacheSlot<TableSnapshotStatisticCache>,
    segment_statistics_cache: CacheSlot<SegmentStatisticsCache>,
    compact_segment_info_cache: CacheSlot<CompactSegmentInfoCache>,
    column_oriented_segment_info_cache: CacheSlot<ColumnOrientedSegmentInfoCache>,
    bloom_index_filter_cache: CacheSlot<BloomIndexFilterCache>,
    bloom_index_meta_cache: CacheSlot<BloomIndexMetaCache>,
    inverted_index_meta_cache: CacheSlot<InvertedIndexMetaCache>,
    inverted_index_file_cache: CacheSlot<InvertedIndexFileCache>,
    vector_index_meta_cache: CacheSlot<VectorIndexMetaCache>,
    vector_index_file_cache: CacheSlot<VectorIndexFileCache>,
    virtual_column_meta_cache: CacheSlot<VirtualColumnMetaCache>,
    prune_partitions_cache: CacheSlot<PrunePartitionsCache>,
    parquet_meta_data_cache: CacheSlot<ParquetMetaDataCache>,
    in_memory_table_data_cache: CacheSlot<ColumnArrayCache>,
    segment_block_metas_cache: CacheSlot<SegmentBlockMetasCache>,
    block_meta_cache: CacheSlot<BlockMetaCache>,

    column_data_cache: CacheSlot<ColumnDataCache>,
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

        info!(
            "[CacheManager] On-disk cache population queue size: {}",
            on_disk_cache_queue_size
        );

        // setup table data cache
        let column_data_cache = {
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

                    Self::new_hybrid_cache_slot(
                        HYBRID_CACHE_COLUMN_DATA,
                        config.data_cache_in_memory_bytes as usize,
                        Unit::Bytes,
                        &table_data_on_disk_cache_path,
                        on_disk_cache_queue_size,
                        config.disk_cache_config.max_bytes as usize,
                        config.data_cache_key_reload_policy.clone(),
                        on_disk_cache_sync_data,
                        ee_mode,
                    )?
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
                vector_index_meta_cache: CacheSlot::new(None),
                vector_index_file_cache: CacheSlot::new(None),
                virtual_column_meta_cache: CacheSlot::new(None),
                prune_partitions_cache: CacheSlot::new(None),
                parquet_meta_data_cache: CacheSlot::new(None),
                table_statistic_cache: CacheSlot::new(None),
                segment_statistics_cache: CacheSlot::new(None),
                in_memory_table_data_cache,
                segment_block_metas_cache: CacheSlot::new(None),
                block_meta_cache: CacheSlot::new(None),
                column_data_cache,
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
            let segment_statistics_cache = Self::new_bytes_cache_slot(
                MEMORY_CACHE_SEGMENT_STATISTICS,
                config.segment_statistics_bytes as usize,
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
                        .join(tenant_id.clone())
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

            let inverted_index_meta_cache = {
                let inverted_index_meta_on_disk_cache_path =
                    PathBuf::from(&config.disk_cache_config.path)
                        .join(tenant_id.clone())
                        .join("inverted_index_meta_v1");
                Self::new_hybrid_cache_slot(
                    HYBRID_CACHE_INVERTED_INDEX_FILE_META_DATA,
                    config.inverted_index_meta_count as usize,
                    Unit::Count,
                    &inverted_index_meta_on_disk_cache_path,
                    on_disk_cache_queue_size,
                    config.disk_cache_inverted_index_meta_size as usize,
                    DiskCacheKeyReloadPolicy::Fuzzy,
                    on_disk_cache_sync_data,
                    ee_mode,
                )?
            };

            // setup inverted index filter cache
            let inverted_index_file_size = if config.inverted_index_filter_memory_ratio != 0 {
                (*max_server_memory_usage as usize)
                    * config.inverted_index_filter_memory_ratio as usize
                    / 100
            } else {
                config.inverted_index_filter_size as usize
            };
            let inverted_index_file_cache = {
                let inverted_index_file_on_disk_cache_path =
                    PathBuf::from(&config.disk_cache_config.path)
                        .join(tenant_id.clone())
                        .join("inverted_index_file_v1");
                Self::new_hybrid_cache_slot(
                    HYBRID_CACHE_INVERTED_INDEX_FILE,
                    inverted_index_file_size,
                    Unit::Bytes,
                    &inverted_index_file_on_disk_cache_path,
                    on_disk_cache_queue_size,
                    config.disk_cache_inverted_index_data_size as usize,
                    DiskCacheKeyReloadPolicy::Fuzzy,
                    on_disk_cache_sync_data,
                    ee_mode,
                )?
            };

            let vector_index_meta_cache = {
                let vector_index_meta_on_disk_cache_path =
                    PathBuf::from(&config.disk_cache_config.path)
                        .join(tenant_id.clone())
                        .join("vector_index_meta_v1");
                Self::new_hybrid_cache_slot(
                    HYBRID_CACHE_VECTOR_INDEX_FILE_META_DATA,
                    config.vector_index_meta_count as usize,
                    Unit::Count,
                    &vector_index_meta_on_disk_cache_path,
                    on_disk_cache_queue_size,
                    config.disk_cache_vector_index_meta_size as usize,
                    DiskCacheKeyReloadPolicy::Fuzzy,
                    on_disk_cache_sync_data,
                    ee_mode,
                )?
            };

            // setup vector index filter cache
            let vector_index_file_size = if config.vector_index_filter_memory_ratio != 0 {
                (*max_server_memory_usage as usize)
                    * config.vector_index_filter_memory_ratio as usize
                    / 100
            } else {
                config.vector_index_filter_size as usize
            };
            let vector_index_file_cache = {
                let vector_index_file_on_disk_cache_path =
                    PathBuf::from(&config.disk_cache_config.path)
                        .join(tenant_id.clone())
                        .join("vector_index_file_v1");
                Self::new_hybrid_cache_slot(
                    HYBRID_CACHE_VECTOR_INDEX_FILE,
                    vector_index_file_size,
                    Unit::Bytes,
                    &vector_index_file_on_disk_cache_path,
                    on_disk_cache_queue_size,
                    config.disk_cache_vector_index_data_size as usize,
                    DiskCacheKeyReloadPolicy::Fuzzy,
                    on_disk_cache_sync_data,
                    ee_mode,
                )?
            };

            let virtual_column_meta_cache = {
                let virtual_column_meta_on_disk_cache_path =
                    PathBuf::from(&config.disk_cache_config.path)
                        .join(tenant_id.clone())
                        .join("virtual_column_meta_v1");
                Self::new_hybrid_cache_slot(
                    HYBRID_CACHE_VIRTUAL_COLUMN_FILE_META_DATA,
                    config.virtual_column_meta_count as usize,
                    Unit::Count,
                    &virtual_column_meta_on_disk_cache_path,
                    on_disk_cache_queue_size,
                    config.disk_cache_virtual_column_meta_size as usize,
                    DiskCacheKeyReloadPolicy::Fuzzy,
                    on_disk_cache_sync_data,
                    ee_mode,
                )?
            };

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
                config.segment_block_metas_count as usize,
            );

            let block_meta_cache = Self::new_items_cache_slot(
                MEMORY_CACHE_BLOCK_META,
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
                vector_index_meta_cache,
                vector_index_file_cache,
                virtual_column_meta_cache,
                prune_partitions_cache,
                table_statistic_cache,
                segment_statistics_cache,
                in_memory_table_data_cache,
                segment_block_metas_cache,
                parquet_meta_data_cache,
                block_meta_cache,
                iceberg_table_meta_cache,
                allows_on_disk_cache,
                column_data_cache,
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

    /// Releases memory occupied by caches
    ///
    /// This method is used to free up memory resources by clearing various caches
    /// in the system. The extent of cache clearing depends on the specified clearance level.
    ///
    /// # Parameters
    /// * `clearance_level` - Determines how aggressively to clear caches:
    ///   * `CacheClearanceLevel::Basic`: Clears basic caches that may consume significant memory
    ///   * `CacheClearanceLevel::Deep`: Performs a more aggressive clearing, including both basic caches and extra caches
    pub fn release_cache_memory(&self, clearance_level: CacheClearanceLevel) {
        /// Clears basic caches that may consume a significant amount of memory
        fn clear_basic_caches(me: &CacheManager) {
            CacheManager::clear_cache(&me.in_memory_table_data_cache);
            CacheManager::clear_cache(&me.segment_block_metas_cache);
            // Only the in-memory part of column_data_cache will be cleared
            CacheManager::clear_cache(&me.column_data_cache);
            CacheManager::clear_cache(&me.block_meta_cache);
        }

        fn clear_extra_caches(me: &CacheManager) {
            // These caches are typically not memory usage heavy, and have more significant impact
            // for the query performance, consider clearing them when the memory pressure is
            // high.
            CacheManager::clear_cache(&me.compact_segment_info_cache);
            CacheManager::clear_cache(&me.bloom_index_filter_cache);
            CacheManager::clear_cache(&me.bloom_index_meta_cache);
        }

        match clearance_level {
            CacheClearanceLevel::Basic => clear_basic_caches(self),
            CacheClearanceLevel::Deep => {
                clear_basic_caches(self);
                clear_extra_caches(self)
            }
        }
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
            HYBRID_CACHE_INVERTED_INDEX_FILE_META_DATA
            | IN_MEMORY_HYBRID_CACHE_INVERTED_INDEX_FILE_META_DATA => {
                Self::set_hybrid_cache_items_capacity(
                    &self.inverted_index_meta_cache,
                    new_capacity,
                    name,
                );
            }
            HYBRID_CACHE_INVERTED_INDEX_FILE | IN_MEMORY_HYBRID_CACHE_INVERTED_INDEX_FILE => {
                Self::set_hybrid_cache_bytes_capacity(
                    &self.inverted_index_file_cache,
                    new_capacity,
                    name,
                );
            }
            HYBRID_CACHE_VECTOR_INDEX_FILE_META_DATA
            | IN_MEMORY_HYBRID_CACHE_VECTOR_INDEX_FILE_META_DATA => {
                Self::set_hybrid_cache_items_capacity(
                    &self.vector_index_meta_cache,
                    new_capacity,
                    name,
                );
            }
            HYBRID_CACHE_VECTOR_INDEX_FILE | IN_MEMORY_HYBRID_CACHE_VECTOR_INDEX_FILE => {
                Self::set_hybrid_cache_bytes_capacity(
                    &self.vector_index_file_cache,
                    new_capacity,
                    name,
                );
            }
            HYBRID_CACHE_VIRTUAL_COLUMN_FILE_META_DATA
            | IN_MEMORY_HYBRID_CACHE_VIRTUAL_COLUMN_FILE_META_DATA => {
                Self::set_hybrid_cache_items_capacity(
                    &self.virtual_column_meta_cache,
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

            HYBRID_CACHE_COLUMN_DATA | IN_MEMORY_HYBRID_CACHE_COLUMN_DATA => {
                Self::set_hybrid_cache_bytes_capacity(&self.column_data_cache, new_capacity, name);
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
    fn clear_cache<C: CacheAccessor + Clone>(cache_slot: &CacheSlot<C>) {
        if let Some(cache) = cache_slot.get() {
            let name = cache.name();
            info!(
                "clearing cache {}: dropping {} items, size {}",
                name,
                cache.len(),
                cache.bytes_size()
            );
            cache.clear();
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
        cache_slot: &CacheSlot<HybridCache<T>>,
        new_capacity: u64,
        name: impl Into<String>,
    ) {
        let cache = cache_slot.get();
        if let Some(mem_cache) = cache.in_memory_cache() {
            mem_cache.set_items_capacity(new_capacity as usize)
        } else {
            // In this case, only in-memory cache will be built, on-disk cache is NOT allowed
            // to be enabled this way yet.
            let name = name.into();
            let in_memory_cache_name = HybridCache::<T>::in_memory_cache_name(&name);
            if let Some(new_cache) =
                Self::new_items_cache(in_memory_cache_name, new_capacity as usize)
            {
                let hybrid_cache =
                    HybridCache::new(name, Some(new_cache), cache.on_disk_cache().cloned());
                cache_slot.set(hybrid_cache);
            }
        }
    }

    fn set_hybrid_cache_bytes_capacity<T: Into<CacheValue<T>>>(
        cache_slot: &CacheSlot<HybridCache<T>>,
        new_capacity: u64,
        name: impl Into<String>,
    ) {
        let cache = cache_slot.get();
        if let Some(mem_cache) = cache.in_memory_cache() {
            mem_cache.set_bytes_capacity(new_capacity as usize)
        } else {
            // In this case, only in-memory cache will be built, on-disk cache is NOT allowed
            // to be enabled this way yet.
            let name = name.into();
            let in_memory_cache_name = HybridCache::<T>::in_memory_cache_name(&name);
            if let Some(new_cache) =
                Self::new_bytes_cache(in_memory_cache_name, new_capacity as usize)
            {
                let hybrid_cache =
                    HybridCache::new(name, Some(new_cache), cache.on_disk_cache().cloned());
                cache_slot.set(hybrid_cache);
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

    pub fn get_segment_statistics_cache(&self) -> Option<SegmentStatisticsCache> {
        self.segment_statistics_cache.get()
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
        if self.allows_on_disk_cache.load(Ordering::Relaxed) {
            // Returns the original cache as it is, which may or may not have on-disk cache
            cache
        } else {
            // Toggle off on-disk cache
            cache.toggle_off_disk_cache()
        }
    }

    pub fn get_inverted_index_meta_cache(&self) -> Option<InvertedIndexMetaCache> {
        self.get_hybrid_cache(self.inverted_index_meta_cache.get())
    }

    pub fn get_inverted_index_file_cache(&self) -> Option<InvertedIndexFileCache> {
        self.get_hybrid_cache(self.inverted_index_file_cache.get())
    }

    pub fn get_vector_index_meta_cache(&self) -> Option<VectorIndexMetaCache> {
        self.get_hybrid_cache(self.vector_index_meta_cache.get())
    }

    pub fn get_vector_index_file_cache(&self) -> Option<VectorIndexFileCache> {
        self.get_hybrid_cache(self.vector_index_file_cache.get())
    }

    pub fn get_virtual_column_meta_cache(&self) -> Option<VirtualColumnMetaCache> {
        self.get_hybrid_cache(self.virtual_column_meta_cache.get())
    }

    pub fn get_prune_partitions_cache(&self) -> Option<PrunePartitionsCache> {
        self.prune_partitions_cache.get()
    }

    pub fn get_parquet_meta_data_cache(&self) -> Option<ParquetMetaDataCache> {
        self.parquet_meta_data_cache.get()
    }

    pub fn get_column_data_cache(&self) -> Option<ColumnDataCache> {
        self.get_hybrid_cache(self.column_data_cache.get())
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
        if disk_cache_bytes_size <= TABLE_DATA_DISK_CACHE_SIZE_THRESHOLD || !ee_mode {
            info!(
                "[CacheManager] On-disk cache {cache_name} disabled, size {disk_cache_bytes_size}, threshold {TABLE_DATA_DISK_CACHE_SIZE_THRESHOLD}, ee mode {ee_mode}"
            );
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

        let in_memory_cache = if in_memory_cache_capacity == 0 {
            None
        } else {
            Some(match unit {
                Unit::Bytes => InMemoryLruCache::with_bytes_capacity(
                    in_mem_cache_name,
                    in_memory_cache_capacity,
                ),

                Unit::Count => InMemoryLruCache::with_items_capacity(
                    in_mem_cache_name,
                    in_memory_cache_capacity,
                ),
            })
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

        Ok(CacheSlot::new(HybridCache::new(
            name,
            in_memory_cache,
            on_disk_cache,
        )))
    }
}

const MEMORY_CACHE_TABLE_DATA: &str = "memory_cache_table_data";
const MEMORY_CACHE_PARQUET_META_DATA: &str = "memory_cache_parquet_meta_data";
const MEMORY_CACHE_PRUNE_PARTITIONS: &str = "memory_cache_prune_partitions";
const HYBRID_CACHE_INVERTED_INDEX_FILE: &str = "cache_inverted_index_file";
const IN_MEMORY_HYBRID_CACHE_INVERTED_INDEX_FILE: &str = "memory_cache_inverted_index_file";
const HYBRID_CACHE_INVERTED_INDEX_FILE_META_DATA: &str = "cache_inverted_index_file_meta_data";
const IN_MEMORY_HYBRID_CACHE_INVERTED_INDEX_FILE_META_DATA: &str =
    "memory_cache_inverted_index_file_meta_data";
const HYBRID_CACHE_VECTOR_INDEX_FILE: &str = "cache_vector_index_file";
const IN_MEMORY_HYBRID_CACHE_VECTOR_INDEX_FILE: &str = "memory_cache_vector_index_file";
const HYBRID_CACHE_VECTOR_INDEX_FILE_META_DATA: &str = "cache_vector_index_file_meta_data";
const IN_MEMORY_HYBRID_CACHE_VECTOR_INDEX_FILE_META_DATA: &str =
    "memory_cache_vector_index_file_meta_data";

const HYBRID_CACHE_VIRTUAL_COLUMN_FILE_META_DATA: &str = "cache_virtual_column_file_meta_data";
const IN_MEMORY_HYBRID_CACHE_VIRTUAL_COLUMN_FILE_META_DATA: &str =
    "memory_cache_virtual_column_file_meta_data";

const HYBRID_CACHE_BLOOM_INDEX_FILE_META_DATA: &str = "cache_bloom_index_file_meta_data";
const HYBRID_CACHE_COLUMN_DATA: &str = "cache_column_data";
const IN_MEMORY_HYBRID_CACHE_COLUMN_DATA: &str = "memory_cache_column_data";

const IN_MEMORY_CACHE_BLOOM_INDEX_FILE_META_DATA: &str = "memory_cache_bloom_index_file_meta_data";
const HYBRID_CACHE_BLOOM_INDEX_FILTER: &str = "cache_bloom_index_filter";
const IN_MEMORY_HYBRID_CACHE_BLOOM_INDEX_FILTER: &str = "memory_cache_bloom_index_filter";
const MEMORY_CACHE_COMPACT_SEGMENT_INFO: &str = "memory_cache_compact_segment_info";
const MEMORY_CACHE_COLUMN_ORIENTED_SEGMENT_INFO: &str = "memory_cache_column_oriented_segment_info";
const MEMORY_CACHE_TABLE_STATISTICS: &str = "memory_cache_table_statistics";
const MEMORY_CACHE_SEGMENT_STATISTICS: &str = "memory_cache_segment_statistics";
const MEMORY_CACHE_TABLE_SNAPSHOT: &str = "memory_cache_table_snapshot";
const MEMORY_CACHE_SEGMENT_BLOCK_METAS: &str = "memory_cache_segment_block_metas";
const MEMORY_CACHE_ICEBERG_TABLE: &str = "memory_cache_iceberg_table";

const MEMORY_CACHE_BLOCK_META: &str = "memory_cache_block_meta";

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::ArrayRef;
    use arrow::array::Int32Array;
    use bytes::Bytes;
    use databend_common_config::CacheConfig;
    use databend_common_config::DiskCacheInnerConfig;
    use databend_storages_common_index::BloomIndexMeta;
    use databend_storages_common_index::filters::FilterBuilder;
    use databend_storages_common_index::filters::FilterImpl;
    use databend_storages_common_index::filters::Xor8Builder;
    use databend_storages_common_table_meta::meta::BlockMeta;
    use databend_storages_common_table_meta::meta::CompactSegmentInfo;
    use databend_storages_common_table_meta::meta::Compression;
    use databend_storages_common_table_meta::meta::MetaEncoding;
    use databend_storages_common_table_meta::meta::RawBlockMeta;
    use tempfile::TempDir;

    use super::*;
    use crate::ColumnData;
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
            disk_cache_inverted_index_data_size: 1024 * 1024,
            disk_cache_vector_index_meta_size: 1024 * 1024,
            disk_cache_vector_index_data_size: 1024 * 1024,
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
            disk_cache_inverted_index_data_size: 0,
            disk_cache_vector_index_meta_size: 0,
            disk_cache_vector_index_data_size: 0,
            disk_cache_virtual_column_meta_size: 0,
            ..CacheConfig::default()
        }
    }

    fn all_disk_cache_enabled(cache_manager: &CacheManager) -> bool {
        cache_manager
            .get_column_data_cache()
            .on_disk_cache()
            .is_some()
            && cache_manager
                .get_bloom_index_meta_cache()
                .on_disk_cache()
                .is_some()
            && cache_manager
                .get_bloom_index_filter_cache()
                .on_disk_cache()
                .is_some()
            && cache_manager
                .get_inverted_index_meta_cache()
                .on_disk_cache()
                .is_some()
            && cache_manager
                .get_inverted_index_file_cache()
                .on_disk_cache()
                .is_some()
            && cache_manager
                .get_vector_index_meta_cache()
                .on_disk_cache()
                .is_some()
            && cache_manager
                .get_vector_index_file_cache()
                .on_disk_cache()
                .is_some()
            && cache_manager
                .get_virtual_column_meta_cache()
                .on_disk_cache()
                .is_some()
    }

    fn all_disk_cache_disabled(cache_manager: &CacheManager) -> bool {
        cache_manager
            .get_column_data_cache()
            .on_disk_cache()
            .is_none()
            && cache_manager
                .get_bloom_index_meta_cache()
                .on_disk_cache()
                .is_none()
            && cache_manager
                .get_bloom_index_filter_cache()
                .on_disk_cache()
                .is_none()
            && cache_manager
                .get_inverted_index_meta_cache()
                .on_disk_cache()
                .is_none()
            && cache_manager
                .get_inverted_index_file_cache()
                .on_disk_cache()
                .is_none()
            && cache_manager
                .get_vector_index_meta_cache()
                .on_disk_cache()
                .is_none()
            && cache_manager
                .get_vector_index_file_cache()
                .on_disk_cache()
                .is_none()
            && cache_manager
                .get_virtual_column_meta_cache()
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

        //  // All disk caches should be enabled, if toggled on disk caches
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

    #[test]
    fn test_release_cache_memory() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_string_lossy().clone();
        let max_server_memory_usage = 1024 * 1024;

        let mut cache_config = config_with_disk_cache_enabled(&cache_path);

        // Configure cache with sufficient capacity for testing
        cache_config.table_data_deserialized_data_bytes = 1024 * 1024;
        cache_config.block_meta_count = 10;
        cache_config.segment_block_metas_count = 20;
        cache_config.data_cache_in_memory_bytes = 1024 * 1024;

        let ee_mode = true;
        let cache_manager = CacheManager::try_new(
            &cache_config,
            &max_server_memory_usage,
            "test_tenant_id",
            ee_mode,
        )?;

        // ----- POPULATE BASIC CACHES -----

        // 1. Populate in-memory table data cache
        let in_memory_table_data_cache = cache_manager.get_table_data_array_cache().unwrap();
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        in_memory_table_data_cache.insert("not matter".to_string(), (array, 1));

        // 2. Populate segment block metas cache
        let segment_block_metas_cache = cache_manager.get_segment_block_metas_cache().unwrap();
        segment_block_metas_cache.insert("not matter".to_string(), vec![]);

        // 3. Populate column data cache
        let column_data_cache = cache_manager.get_column_data_cache().unwrap();
        column_data_cache.insert(
            "not matter".to_string(),
            ColumnData::from_bytes(Bytes::new()),
        );

        {
            // Make sure at least the first key is written to disk cache

            column_data_cache.insert(
                "extra_key".to_string(),
                ColumnData::from_bytes(Bytes::new()),
            );

            let disk_cache = column_data_cache.on_disk_cache().unwrap();
            // Since the on-disk cache is populated asynchronously, we need to
            // wait until the populate-queue is empty. At that time, at least
            // the first key `test_key` should be written into the on-disk cache.
            disk_cache.till_no_pending_items_in_queue();
        }

        // 4. Populate block meta cache
        let block_meta_cache = cache_manager.get_block_meta_cache().unwrap();
        block_meta_cache.insert("not matter".to_string(), BlockMeta {
            row_count: 0,
            block_size: 0,
            file_size: 0,
            col_stats: Default::default(),
            col_metas: Default::default(),
            cluster_stats: None,
            location: ("".to_string(), 0),
            bloom_filter_index_location: None,
            bloom_filter_index_size: 0,
            inverted_index_size: None,
            ngram_filter_index_size: None,
            vector_index_location: None,
            vector_index_size: None,
            virtual_block_meta: None,
            compression: Compression::Lz4,
            create_on: None,
        });

        // ----- POPULATE EXTRA CACHES -----

        // 1. Populate compact segment info cache
        let compact_segment_info_cache = cache_manager.get_table_segment_cache().unwrap();
        compact_segment_info_cache.insert("not matter".to_string(), CompactSegmentInfo {
            format_version: 0,
            summary: Default::default(),
            raw_block_metas: RawBlockMeta {
                bytes: vec![],
                encoding: MetaEncoding::Bincode,
                compression: Default::default(),
            },
        });

        // 2. Populate bloom index filter cache
        let bloom_index_filter_cache = cache_manager.get_bloom_index_filter_cache().unwrap();
        let mut builder = Xor8Builder::create();
        builder.add_key(&"what ever".to_string());
        let xor8_filter = builder.build()?;
        bloom_index_filter_cache.insert("not matter".to_string(), FilterImpl::Xor(xor8_filter));

        // 3. Populate bloom index meta cache
        let bloom_index_meta_cache = cache_manager.get_bloom_index_meta_cache().unwrap();
        bloom_index_meta_cache.insert("not matter".to_string(), BloomIndexMeta { columns: vec![] });

        // ----- VERIFY INITIAL CACHE STATE -----
        // Verify all caches are correctly populated
        assert!(!cache_manager.get_table_data_array_cache().is_empty());
        assert!(
            !cache_manager
                .get_segment_block_metas_cache()
                .unwrap()
                .is_empty()
        );
        assert!(!cache_manager.get_column_data_cache().is_empty());
        assert!(!cache_manager.get_block_meta_cache().is_empty());
        assert!(!cache_manager.get_table_segment_cache().is_empty());
        assert!(!cache_manager.get_bloom_index_filter_cache().is_empty());
        assert!(!cache_manager.get_bloom_index_meta_cache().is_empty());

        // 4.
        // ----- TEST BASIC CLEARANCE LEVEL -----

        // Clear caches with Basic clearance level
        cache_manager.release_cache_memory(CacheClearanceLevel::Basic);

        // Verify basic caches are cleared
        assert!(in_memory_table_data_cache.is_empty());
        assert!(
            cache_manager
                .get_segment_block_metas_cache()
                .unwrap()
                .is_empty()
        );
        assert!(cache_manager.get_block_meta_cache().is_empty());

        // Verify hybrid column data cache behavior:
        // - On-disk cache of table data should remain populated
        assert!(!cache_manager.get_column_data_cache().is_empty());
        // - In-memory cache of table data should be cleared
        assert!(
            cache_manager
                .get_column_data_cache()
                .unwrap()
                .in_memory_cache()
                .unwrap()
                .is_empty()
        );

        // Verify extra caches remain intact after Basic clearance
        assert!(!cache_manager.get_table_segment_cache().is_empty());
        assert!(!cache_manager.get_bloom_index_filter_cache().is_empty());
        assert!(!cache_manager.get_bloom_index_meta_cache().is_empty());

        // 5.
        // ----- TEST DEEP CLEARANCE LEVEL -----

        // Clear caches with Deep clearance level
        cache_manager.release_cache_memory(CacheClearanceLevel::Deep);

        // Verify extra caches are now cleared
        assert!(cache_manager.get_table_segment_cache().is_empty());

        // Verify in-memory part of hybrid bloom index meta caches are cleared
        assert!(
            cache_manager
                .get_bloom_index_meta_cache()
                .unwrap()
                .in_memory_cache()
                .unwrap()
                .is_empty()
        );

        // Verify in-memory part of hybrid bloom index filter caches are cleared
        assert!(
            cache_manager
                .get_bloom_index_filter_cache()
                .unwrap()
                .in_memory_cache()
                .unwrap()
                .is_empty()
        );

        Ok(())
    }

    #[test]
    fn test_cache_manager_block_meta_config() -> Result<()> {
        let max_server_memory_usage = 1024 * 1024;

        let cache_config = CacheConfig {
            enable_table_meta_cache: true,
            segment_block_metas_count: 10,
            block_meta_count: 20,
            ..Default::default()
        };

        let ee_mode = false;
        let cache_manager = CacheManager::try_new(
            &cache_config,
            &max_server_memory_usage,
            "test_tenant_id",
            ee_mode,
        )?;

        assert_eq!(
            cache_manager
                .get_segment_block_metas_cache()
                .unwrap()
                .items_capacity(),
            10
        );
        assert_eq!(
            cache_manager
                .get_block_meta_cache()
                .unwrap()
                .items_capacity(),
            20
        );

        Ok(())
    }

    #[test]
    fn test_disk_cache_size_threshold() -> Result<()> {
        use tempfile::TempDir;

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
