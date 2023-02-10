// Copyright 2023 Datafuse Labs.
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
//

use std::sync::Arc;

use common_base::base::GlobalInstance;
use common_cache::CountableMeter;
use common_cache::DefaultHashBuilder;
use common_config::QueryConfig;
use common_exception::Result;
use storages_common_cache::InMemoryCacheBuilder;
use storages_common_cache::InMemoryItemCacheHolder;
use storages_common_cache::Named;
use storages_common_cache::NamedCache;
use storages_common_cache::TableDataCache;
use storages_common_cache::TableDataCacheBuilder;

use crate::caches::BloomIndexFilterCache;
use crate::caches::BloomIndexMetaCache;
use crate::caches::ColumnArrayCache;
use crate::caches::FileMetaDataCache;
use crate::caches::SegmentInfoCache;
use crate::caches::TableSnapshotCache;
use crate::caches::TableSnapshotStatisticCache;
use crate::ColumnArrayMeter;

static DEFAULT_FILE_META_DATA_CACHE_ITEMS: u64 = 3000;

/// Where all the caches reside
pub struct CacheManager {
    table_snapshot_cache: Option<TableSnapshotCache>,
    table_statistic_cache: Option<TableSnapshotStatisticCache>,
    segment_info_cache: Option<SegmentInfoCache>,
    bloom_index_filter_cache: Option<BloomIndexFilterCache>,
    bloom_index_meta_cache: Option<BloomIndexMetaCache>,
    file_meta_data_cache: Option<FileMetaDataCache>,
    table_data_cache: Option<TableDataCache>,
    table_column_array_cache: Option<ColumnArrayCache>,
}

impl CacheManager {
    /// Initialize the caches according to the relevant configurations.
    pub fn init(config: &QueryConfig) -> Result<()> {
        // setup table data cache
        let table_data_cache = if config.table_data_cache_enabled {
            None
        } else {
            Self::new_block_data_cache(
                &config.table_disk_cache_root,
                config.table_data_cache_population_queue_size,
                config.table_disk_cache_mb_size,
            )?
        };

        // setup in-memory table column cache
        let table_column_array_cache = Self::new_in_memory_cache(
            config.table_cache_column_mb_size,
            ColumnArrayMeter,
            "table_data_cache_column_array",
        );

        // setup in-memory table meta cache
        if !config.table_meta_cache_enabled {
            GlobalInstance::set(Arc::new(Self {
                table_snapshot_cache: None,
                segment_info_cache: None,
                bloom_index_filter_cache: None,
                bloom_index_meta_cache: None,
                file_meta_data_cache: None,
                table_statistic_cache: None,
                table_data_cache,
                table_column_array_cache,
            }));
        } else {
            let table_snapshot_cache = Self::new_item_cache(config.table_cache_snapshot_count);
            let table_statistic_cache = Self::new_item_cache(config.table_cache_statistic_count);
            let segment_info_cache = Self::new_item_cache(config.table_cache_segment_count);
            let bloom_index_filter_cache =
                Self::new_item_cache(config.table_cache_bloom_index_filter_count);
            let bloom_index_meta_cache =
                Self::new_item_cache(config.table_cache_bloom_index_meta_count);
            let file_meta_data_cache = Self::new_item_cache(DEFAULT_FILE_META_DATA_CACHE_ITEMS);
            GlobalInstance::set(Arc::new(Self {
                table_snapshot_cache,
                segment_info_cache,
                bloom_index_filter_cache,
                bloom_index_meta_cache,
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

    pub fn get_table_segment_cache(&self) -> Option<SegmentInfoCache> {
        self.segment_info_cache.clone()
    }

    pub fn get_bloom_index_filter_cache(&self) -> Option<BloomIndexFilterCache> {
        self.bloom_index_filter_cache.clone()
    }

    pub fn get_bloom_index_meta_cache(&self) -> Option<BloomIndexMetaCache> {
        self.bloom_index_meta_cache.clone()
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
    fn new_item_cache<V>(capacity: u64) -> Option<InMemoryItemCacheHolder<V>> {
        if capacity > 0 {
            Some(InMemoryCacheBuilder::new_item_cache(capacity))
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
        path: &str,
        population_queue_size: u32,
        disk_cache_mb_size: u64,
    ) -> Result<Option<TableDataCache>> {
        if disk_cache_mb_size > 0 {
            let cache_holder = TableDataCacheBuilder::new_table_data_disk_cache(
                path,
                population_queue_size,
                disk_cache_mb_size,
            )?;
            Ok(Some(cache_holder))
        } else {
            Ok(None)
        }
    }
}
