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

use common_base::base::GlobalInstance;
use common_cache::CountableMeter;
use common_cache::DefaultHashBuilder;
use common_config::CacheConfig;
use common_config::CacheStorageTypeInnerConfig;
use common_exception::Result;
use log::info;
use storages_common_cache::InMemoryCacheBuilder;
use storages_common_cache::InMemoryItemCacheHolder;
use storages_common_cache::Named;
use storages_common_cache::NamedCache;
use storages_common_cache::TableDataCache;
use storages_common_cache::TableDataCacheBuilder;

use crate::caches::BloomIndexFilterCache;
use crate::caches::BloomIndexMetaCache;
use crate::caches::ColumnArrayCache;
use crate::caches::CompactSegmentInfoCache;
use crate::caches::FileMetaDataCache;
use crate::caches::TableSnapshotCache;
use crate::caches::TableSnapshotStatisticCache;
use crate::BloomIndexFilterMeter;
use crate::ColumnArrayMeter;
use crate::CompactSegmentInfoMeter;
use crate::PrunePartitionsCache;

static DEFAULT_FILE_META_DATA_CACHE_ITEMS: u64 = 3000;

/// Where all the caches reside
pub struct CacheManager {
    table_snapshot_cache: Option<TableSnapshotCache>,
    table_statistic_cache: Option<TableSnapshotStatisticCache>,
    segment_info_cache: Option<CompactSegmentInfoCache>,
    bloom_index_filter_cache: Option<BloomIndexFilterCache>,
    bloom_index_meta_cache: Option<BloomIndexMetaCache>,
    prune_partitions_cache: Option<PrunePartitionsCache>,
    file_meta_data_cache: Option<FileMetaDataCache>,
    table_data_cache: Option<TableDataCache>,
    table_column_array_cache: Option<ColumnArrayCache>,
}

impl CacheManager {
    /// Initialize the caches according to the relevant configurations.
    pub fn init(config: &CacheConfig, tenant_id: impl Into<String>) -> Result<()> {
        // setup table data cache
        let table_data_cache = {
            match config.data_cache_storage {
                CacheStorageTypeInnerConfig::None => None,
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
                        )
                    };

                    info!(
                        "disk cache enabled, cache population queue size {}",
                        queue_size
                    );

                    Self::new_block_data_cache(
                        &real_disk_cache_root,
                        queue_size,
                        config.disk_cache_config.max_bytes,
                    )?
                }
            }
        };

        // setup in-memory table column cache
        let table_column_array_cache = Self::new_in_memory_cache(
            config.table_data_deserialized_data_bytes,
            ColumnArrayMeter,
            "table_data_column_array",
        );

        // setup in-memory table meta cache
        if !config.enable_table_meta_cache {
            GlobalInstance::set(Arc::new(Self {
                table_snapshot_cache: None,
                segment_info_cache: None,
                bloom_index_filter_cache: None,
                bloom_index_meta_cache: None,
                prune_partitions_cache: None,
                file_meta_data_cache: None,
                table_statistic_cache: None,
                table_data_cache,
                table_column_array_cache,
            }));
        } else {
            let table_snapshot_cache =
                Self::new_item_cache(config.table_meta_snapshot_count, "table_snapshot");
            let table_statistic_cache =
                Self::new_item_cache(config.table_meta_statistic_count, "table_statistics");
            let segment_info_cache = Self::new_in_memory_cache(
                config.table_meta_segment_bytes,
                CompactSegmentInfoMeter {},
                "segment_info",
            );
            let bloom_index_filter_cache = Self::new_in_memory_cache(
                config.table_bloom_index_filter_size,
                BloomIndexFilterMeter {},
                "bloom_index_filter",
            );
            let bloom_index_meta_cache = Self::new_item_cache(
                config.table_bloom_index_meta_count,
                "bloom_index_file_meta_data",
            );
            let prune_partitions_cache =
                Self::new_item_cache(config.table_prune_partitions_count, "prune_partitions");

            let file_meta_data_cache =
                Self::new_item_cache(DEFAULT_FILE_META_DATA_CACHE_ITEMS, "parquet_file_meta");
            GlobalInstance::set(Arc::new(Self {
                table_snapshot_cache,
                segment_info_cache,
                bloom_index_filter_cache,
                bloom_index_meta_cache,
                prune_partitions_cache,
                file_meta_data_cache,
                table_statistic_cache,
                table_data_cache,
                table_column_array_cache,
            }));
        }

        Ok(())
    }

    pub fn instance() -> Arc<CacheManager> {
        GlobalInstance::get()
    }

    pub fn get_table_snapshot_cache(&self) -> Option<TableSnapshotCache> {
        self.table_snapshot_cache.clone()
    }

    pub fn get_table_snapshot_statistics_cache(&self) -> Option<TableSnapshotStatisticCache> {
        self.table_statistic_cache.clone()
    }

    pub fn get_table_segment_cache(&self) -> Option<CompactSegmentInfoCache> {
        self.segment_info_cache.clone()
    }

    pub fn get_bloom_index_filter_cache(&self) -> Option<BloomIndexFilterCache> {
        self.bloom_index_filter_cache.clone()
    }

    pub fn get_bloom_index_meta_cache(&self) -> Option<BloomIndexMetaCache> {
        self.bloom_index_meta_cache.clone()
    }

    pub fn get_prune_partitions_cache(&self) -> Option<PrunePartitionsCache> {
        self.prune_partitions_cache.clone()
    }

    pub fn get_file_meta_data_cache(&self) -> Option<FileMetaDataCache> {
        self.file_meta_data_cache.clone()
    }

    pub fn get_table_data_cache(&self) -> Option<TableDataCache> {
        self.table_data_cache.clone()
    }

    pub fn get_table_data_array_cache(&self) -> Option<ColumnArrayCache> {
        self.table_column_array_cache.clone()
    }

    // create cache that meters size by `Count`
    fn new_item_cache<V>(
        capacity: u64,
        name: impl Into<String>,
    ) -> Option<NamedCache<InMemoryItemCacheHolder<V>>> {
        if capacity > 0 {
            Some(InMemoryCacheBuilder::new_item_cache(capacity).name_with(name.into()))
        } else {
            None
        }
    }

    // create cache that meters size by `meter`
    fn new_in_memory_cache<V, M>(
        capacity: u64,
        meter: M,
        name: &str,
    ) -> Option<NamedCache<InMemoryItemCacheHolder<V, DefaultHashBuilder, M>>>
    where
        M: CountableMeter<String, Arc<V>>,
    {
        if capacity > 0 {
            Some(
                InMemoryCacheBuilder::new_in_memory_cache(capacity, meter)
                    .name_with(name.to_owned()),
            )
        } else {
            None
        }
    }

    fn new_block_data_cache(
        path: &PathBuf,
        population_queue_size: u32,
        disk_cache_bytes_size: u64,
    ) -> Result<Option<TableDataCache>> {
        if disk_cache_bytes_size > 0 {
            let cache_holder = TableDataCacheBuilder::new_table_data_disk_cache(
                path,
                population_queue_size,
                disk_cache_bytes_size,
            )?;
            Ok(Some(cache_holder))
        } else {
            Ok(None)
        }
    }
}
