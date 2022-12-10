//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_base::base::GlobalInstance;
use common_config::QueryConfig;
use common_exception::Result;

use crate::caches::memory_cache::new_bytes_cache;
use crate::caches::memory_cache::new_item_cache;
use crate::caches::memory_cache::BloomIndexCache;
use crate::caches::memory_cache::BloomIndexMetaCache;
use crate::caches::memory_cache::FileMetaDataCache;
use crate::caches::memory_cache::LabeledBytesCache;
use crate::caches::memory_cache::LabeledItemCache;
use crate::caches::SegmentInfoCache;
use crate::caches::TableSnapshotCache;
use crate::caches::TableSnapshotStatisticCache;
use crate::caches::TenantLabel;

static DEFAULT_FILE_META_DATA_CACHE_ITEMS: u64 = 3000;

/// Where all the caches reside
pub struct CacheManager {
    table_snapshot_cache: Option<TableSnapshotCache>,
    segment_info_cache: Option<SegmentInfoCache>,
    bloom_index_data_cache: Option<BloomIndexCache>,
    bloom_index_meta_cache: Option<BloomIndexMetaCache>,
    file_meta_data_cache: Option<FileMetaDataCache>,
    table_statistic_cache: Option<TableSnapshotStatisticCache>,
    cluster_id: String,
    tenant_id: String,
}

impl CacheManager {
    /// Initialize the caches according to the relevant configurations.
    ///
    /// For convenience, ids of cluster and tenant are also kept
    pub fn init(config: &QueryConfig) -> Result<()> {
        if !config.table_cache_enabled {
            GlobalInstance::set(Arc::new(Self {
                table_snapshot_cache: None,
                segment_info_cache: None,
                bloom_index_data_cache: None,
                bloom_index_meta_cache: None,
                file_meta_data_cache: None,
                table_statistic_cache: None,
                cluster_id: config.cluster_id.clone(),
                tenant_id: config.tenant_id.clone(),
            }));
        } else {
            let tenant_label = TenantLabel {
                tenant_id: config.tenant_id.clone(),
                cluster_id: config.cluster_id.clone(),
            };
            let table_snapshot_cache =
                Self::new_item_cache(config.table_cache_snapshot_count, tenant_label.clone());
            let table_statistic_cache =
                Self::new_item_cache(config.table_cache_statistic_count, tenant_label.clone());
            let segment_info_cache =
                Self::new_item_cache(config.table_cache_segment_count, tenant_label.clone());
            let bloom_index_data_cache = Self::new_bytes_cache(
                config.table_cache_bloom_index_data_bytes,
                tenant_label.clone(),
            );
            let bloom_index_meta_cache = Self::new_item_cache(
                config.table_cache_bloom_index_meta_count,
                tenant_label.clone(),
            );

            let file_meta_data_cache =
                Self::new_item_cache(DEFAULT_FILE_META_DATA_CACHE_ITEMS, tenant_label);

            GlobalInstance::set(Arc::new(Self {
                table_snapshot_cache,
                segment_info_cache,
                bloom_index_data_cache,
                bloom_index_meta_cache,
                file_meta_data_cache,
                table_statistic_cache,
                cluster_id: config.cluster_id.clone(),
                tenant_id: config.tenant_id.clone(),
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

    pub fn get_bloom_index_cache(&self) -> Option<BloomIndexCache> {
        self.bloom_index_data_cache.clone()
    }

    pub fn get_bloom_index_meta_cache(&self) -> Option<BloomIndexMetaCache> {
        self.bloom_index_meta_cache.clone()
    }

    pub fn get_file_meta_data_cache(&self) -> Option<FileMetaDataCache> {
        self.file_meta_data_cache.clone()
    }

    pub fn get_tenant_id(&self) -> &str {
        self.tenant_id.as_str()
    }

    pub fn get_cluster_id(&self) -> &str {
        self.cluster_id.as_str()
    }

    fn new_item_cache<T>(capacity: u64, tenant_label: TenantLabel) -> Option<LabeledItemCache<T>> {
        if capacity > 0 {
            Some(new_item_cache(capacity, tenant_label))
        } else {
            None
        }
    }

    fn new_bytes_cache(capacity: u64, tenant_label: TenantLabel) -> Option<LabeledBytesCache> {
        if capacity > 0 {
            Some(new_bytes_cache(capacity, tenant_label))
        } else {
            None
        }
    }
}
