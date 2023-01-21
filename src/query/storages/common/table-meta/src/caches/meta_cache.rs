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

use common_arrow::parquet::metadata::FileMetaData;
use storages_common_cache::CacheAccessor;
use storages_common_cache::InMemoryBytesCacheHolder;
use storages_common_cache::InMemoryItemCacheHolder;

use crate::caches::CacheManager;
use crate::meta::SegmentInfo;
use crate::meta::TableSnapshot;
use crate::meta::TableSnapshotStatistics;

/// In memory object cache of SegmentInfo
pub type SegmentInfoCache = InMemoryItemCacheHolder<SegmentInfo>;
/// In memory object cache of TableSnapshot
pub type TableSnapshotCache = InMemoryItemCacheHolder<TableSnapshot>;
/// In memory object cache of TableSnapshotStatistics
pub type TableSnapshotStatisticCache = InMemoryItemCacheHolder<TableSnapshotStatistics>;
/// In memory data cache of bloom index data.
/// For each indexed data block, the index data of column is cached individually
pub type BloomIndexCache = InMemoryBytesCacheHolder;
pub struct BloomIndexMeta(pub FileMetaData);
/// In memory object cache of parquet FileMetaData of bloom index data
pub type BloomIndexMetaCache = InMemoryItemCacheHolder<BloomIndexMeta>;
/// In memory object cache of parquet FileMetaData of external parquet files
pub type FileMetaDataCache = InMemoryItemCacheHolder<FileMetaData>;

// Bind Type of cached objects to Caches
//
// The `Cache` returned should
// - cache item s of Type `T`
// - and implement `CacheAccessor` properly
pub trait CachedMeta<T> {
    type Cache: CacheAccessor<String, T>;
    fn cache() -> Option<Self::Cache>;
}

impl CachedMeta<SegmentInfo> for SegmentInfo {
    type Cache = SegmentInfoCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_table_segment_cache()
    }
}

impl CachedMeta<TableSnapshot> for TableSnapshot {
    type Cache = TableSnapshotCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_table_snapshot_cache()
    }
}

impl CachedMeta<TableSnapshotStatistics> for TableSnapshotStatistics {
    type Cache = TableSnapshotStatisticCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_table_snapshot_statistics_cache()
    }
}

impl CachedMeta<BloomIndexMeta> for BloomIndexMeta {
    type Cache = BloomIndexMetaCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_bloom_index_meta_cache()
    }
}

impl CachedMeta<FileMetaData> for FileMetaData {
    type Cache = FileMetaDataCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_file_meta_data_cache()
    }
}
