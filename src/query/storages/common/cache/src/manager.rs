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
use crate::caches::CompactSegmentInfoCache;
use crate::caches::InvertedIndexFileCache;
use crate::caches::InvertedIndexMetaCache;
use crate::caches::ParquetMetaDataCache;
use crate::caches::PrunePartitionsCache;
use crate::caches::SegmentBlockMetasCache;
use crate::caches::TableSnapshotCache;
use crate::caches::TableSnapshotStatisticCache;
use crate::InMemoryLruCache;
use crate::TableDataCache;
use crate::TableDataCacheBuilder;

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
    bloom_index_filter_cache: CacheSlot<BloomIndexFilterCache>,
    bloom_index_meta_cache: CacheSlot<BloomIndexMetaCache>,
    inverted_index_meta_cache: CacheSlot<InvertedIndexMetaCache>,
    inverted_index_file_cache: CacheSlot<InvertedIndexFileCache>,
    prune_partitions_cache: CacheSlot<PrunePartitionsCache>,
    parquet_meta_data_cache: CacheSlot<ParquetMetaDataCache>,
    table_data_cache: CacheSlot<TableDataCache>,
    in_memory_table_data_cache: CacheSlot<ColumnArrayCache>,
    segment_block_metas_cache: CacheSlot<SegmentBlockMetasCache>,
    block_meta_cache: CacheSlot<BlockMetaCache>,
}

impl CacheManager {
    /// Initialize the caches according to the relevant configurations.
    pub fn init(
        config: &CacheConfig,
        max_server_memory_usage: &u64,
        tenant_id: impl Into<String>,
    ) -> Result<()> {
        // setup table data cache
        let table_data_cache = {
            match config.data_cache_storage {
                CacheStorageTypeInnerConfig::None => CacheSlot::new(None),
                CacheStorageTypeInnerConfig::Disk => {
                    let real_disk_cache_root = PathBuf::from(&config.disk_cache_config.path)
                        .join(tenant_id.into())
                        .join("v1");

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

                    info!(
                        "disk cache enabled, cache population queue size {}",
                        queue_size
                    );

                    Self::new_block_data_cache(
                        &real_disk_cache_root,
                        queue_size,
                        config.disk_cache_config.max_bytes as usize,
                        config.data_cache_key_reload_policy.clone(),
                        config.disk_cache_config.sync_data,
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

        // setup in-memory table meta cache
        if !config.enable_table_meta_cache {
            GlobalInstance::set(Arc::new(Self {
                table_snapshot_cache: CacheSlot::new(None),
                compact_segment_info_cache: CacheSlot::new(None),
                bloom_index_filter_cache: CacheSlot::new(None),
                bloom_index_meta_cache: CacheSlot::new(None),
                inverted_index_meta_cache: CacheSlot::new(None),
                inverted_index_file_cache: CacheSlot::new(None),
                prune_partitions_cache: CacheSlot::new(None),
                parquet_meta_data_cache: CacheSlot::new(None),
                table_statistic_cache: CacheSlot::new(None),
                table_data_cache,
                in_memory_table_data_cache,
                segment_block_metas_cache: CacheSlot::new(None),
                block_meta_cache: CacheSlot::new(None),
            }));
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

            let bloom_index_filter_cache = Self::new_bytes_cache_slot(
                MEMORY_CACHE_BLOOM_INDEX_FILTER,
                config.table_bloom_index_filter_size as usize,
            );
            let bloom_index_meta_cache = Self::new_items_cache_slot(
                MEMORY_CACHE_BLOOM_INDEX_FILE_META_DATA,
                config.table_bloom_index_meta_count as usize,
            );
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

            GlobalInstance::set(Arc::new(Self {
                table_snapshot_cache,
                compact_segment_info_cache,
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
            }));
        }

        Ok(())
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
            MEMORY_CACHE_BLOOM_INDEX_FILE_META_DATA => {
                Self::set_items_capacity(&self.bloom_index_meta_cache, new_capacity, name);
            }
            MEMORY_CACHE_BLOOM_INDEX_FILTER => {
                Self::set_bytes_capacity(&self.bloom_index_filter_cache, new_capacity, name);
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

            crate::DISK_TABLE_DATA_CACHE_NAME => {
                return Err(ErrorCode::BadArguments(format!(
                    "set capacity of cache {} is not allowed",
                    name
                )));
            }
            _ => return Err(ErrorCode::BadArguments(format!("cache {} not found", name))),
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

    pub fn get_segment_block_metas_cache(&self) -> Option<SegmentBlockMetasCache> {
        self.segment_block_metas_cache.get()
    }

    pub fn get_block_meta_cache(&self) -> Option<BlockMetaCache> {
        self.block_meta_cache.get()
    }

    pub fn get_table_snapshot_statistics_cache(&self) -> Option<TableSnapshotStatisticCache> {
        self.table_statistic_cache.get()
    }

    pub fn get_table_segment_cache(&self) -> Option<CompactSegmentInfoCache> {
        self.compact_segment_info_cache.get()
    }

    pub fn get_bloom_index_filter_cache(&self) -> Option<BloomIndexFilterCache> {
        self.bloom_index_filter_cache.get()
    }

    pub fn get_bloom_index_meta_cache(&self) -> Option<BloomIndexMetaCache> {
        self.bloom_index_meta_cache.get()
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

    pub fn get_table_data_cache(&self) -> Option<TableDataCache> {
        self.table_data_cache.get()
    }

    pub fn get_table_data_array_cache(&self) -> Option<ColumnArrayCache> {
        self.in_memory_table_data_cache.get()
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

    fn new_block_data_cache(
        path: &PathBuf,
        population_queue_size: u32,
        disk_cache_bytes_size: usize,
        disk_cache_key_reload_policy: DiskCacheKeyReloadPolicy,
        sync_data: bool,
    ) -> Result<CacheSlot<TableDataCache>> {
        if disk_cache_bytes_size > 0 {
            let cache_holder = TableDataCacheBuilder::new_table_data_disk_cache(
                path,
                population_queue_size,
                disk_cache_bytes_size,
                disk_cache_key_reload_policy,
                sync_data,
            )?;
            Ok(CacheSlot::new(Some(cache_holder)))
        } else {
            Ok(CacheSlot::new(None))
        }
    }
}

const MEMORY_CACHE_TABLE_DATA: &str = "memory_cache_table_data";
const MEMORY_CACHE_PARQUET_META_DATA: &str = "memory_cache_parquet_meta_data";
const MEMORY_CACHE_PRUNE_PARTITIONS: &str = "memory_cache_prune_partitions";
const MEMORY_CACHE_INVERTED_INDEX_FILE: &str = "memory_cache_inverted_index_file";
const MEMORY_CACHE_INVERTED_INDEX_FILE_META_DATA: &str =
    "memory_cache_inverted_index_file_meta_data";

const MEMORY_CACHE_BLOOM_INDEX_FILE_META_DATA: &str = "memory_cache_bloom_index_file_meta_data";
const MEMORY_CACHE_BLOOM_INDEX_FILTER: &str = "memory_cache_bloom_index_filter";
const MEMORY_CACHE_COMPACT_SEGMENT_INFO: &str = "memory_cache_compact_segment_info";
const MEMORY_CACHE_TABLE_STATISTICS: &str = "memory_cache_table_statistics";
const MEMORY_CACHE_TABLE_SNAPSHOT: &str = "memory_cache_table_snapshot";
const MEMORY_CACHE_SEGMENT_BLOCK_METAS: &str = "memory_cache_segment_block_metas";

const MEMORY_CACHE_BLOCK_META: &str = "memory_cache_block_meta";
