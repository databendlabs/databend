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

use std::borrow::Borrow;
use std::hash::BuildHasher;
use std::sync::Arc;

use databend_common_arrow::parquet::metadata::FileMetaData;
use databend_common_cache::Count;
use databend_common_cache::CountableMeter;
use databend_common_cache::DefaultHashBuilder;
use databend_common_cache::Meter;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::InMemoryItemCacheHolder;
use databend_storages_common_cache::NamedCache;
use databend_storages_common_index::filters::Xor8Filter;
use databend_storages_common_index::BloomIndexMeta;
use databend_storages_common_index::InvertedIndexFile;
use databend_storages_common_index::InvertedIndexMeta;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;

use crate::cache_manager::CacheManager;

/// In memory object cache of SegmentInfo
pub type CompactSegmentInfoCache = NamedCache<
    InMemoryItemCacheHolder<CompactSegmentInfo, DefaultHashBuilder, CompactSegmentInfoMeter>,
>;

/// In memory object cache of TableSnapshot
pub type TableSnapshotCache = NamedCache<InMemoryItemCacheHolder<TableSnapshot>>;
/// In memory object cache of TableSnapshotStatistics
pub type TableSnapshotStatisticCache = NamedCache<InMemoryItemCacheHolder<TableSnapshotStatistics>>;
/// In memory object cache of bloom filter.
/// For each indexed data block, the bloom xor8 filter of column is cached individually
pub type BloomIndexFilterCache =
    NamedCache<InMemoryItemCacheHolder<Xor8Filter, DefaultHashBuilder, BloomIndexFilterMeter>>;
/// In memory object cache of parquet FileMetaData of bloom index data
pub type BloomIndexMetaCache = NamedCache<InMemoryItemCacheHolder<BloomIndexMeta>>;

pub type InvertedIndexMetaCache = NamedCache<InMemoryItemCacheHolder<InvertedIndexMeta>>;
pub type InvertedIndexFileCache = NamedCache<
    InMemoryItemCacheHolder<InvertedIndexFile, DefaultHashBuilder, InvertedIndexFileMeter>,
>;

/// In memory object cache of parquet FileMetaData of external parquet files
pub type FileMetaDataCache = NamedCache<InMemoryItemCacheHolder<FileMetaData>>;

pub type PrunePartitionsCache = NamedCache<InMemoryItemCacheHolder<(PartStatistics, Partitions)>>;

/// In memory object cache of table column array
pub type ColumnArrayCache =
    NamedCache<InMemoryItemCacheHolder<SizedColumnArray, DefaultHashBuilder, ColumnArrayMeter>>;
pub type ArrayRawDataUncompressedSize = usize;
pub type SizedColumnArray = (
    Box<dyn databend_common_arrow::arrow::array::Array>,
    ArrayRawDataUncompressedSize,
);

// Bind Type of cached objects to Caches
//
// The `Cache` should return
// - cache item of Type `T`
// - and implement `CacheAccessor` properly
pub trait CachedObject<T, S = DefaultHashBuilder, M = Count>
where
    S: BuildHasher,
    M: CountableMeter<String, Arc<T>>,
{
    type Cache: CacheAccessor<String, T, S, M>;
    fn cache() -> Option<Self::Cache>;
}

impl CachedObject<CompactSegmentInfo, DefaultHashBuilder, CompactSegmentInfoMeter>
    for CompactSegmentInfo
{
    type Cache = CompactSegmentInfoCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_table_segment_cache()
    }
}

impl CachedObject<CompactSegmentInfo, DefaultHashBuilder, CompactSegmentInfoMeter> for SegmentInfo {
    type Cache = CompactSegmentInfoCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_table_segment_cache()
    }
}

impl CachedObject<TableSnapshot> for TableSnapshot {
    type Cache = TableSnapshotCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_table_snapshot_cache()
    }
}

impl CachedObject<TableSnapshotStatistics> for TableSnapshotStatistics {
    type Cache = TableSnapshotStatisticCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_table_snapshot_statistics_cache()
    }
}

impl CachedObject<BloomIndexMeta> for BloomIndexMeta {
    type Cache = BloomIndexMetaCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_bloom_index_meta_cache()
    }
}

impl CachedObject<(PartStatistics, Partitions)> for (PartStatistics, Partitions) {
    type Cache = PrunePartitionsCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_prune_partitions_cache()
    }
}

impl CachedObject<Xor8Filter, DefaultHashBuilder, BloomIndexFilterMeter> for Xor8Filter {
    type Cache = BloomIndexFilterCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_bloom_index_filter_cache()
    }
}

impl CachedObject<FileMetaData> for FileMetaData {
    type Cache = FileMetaDataCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_file_meta_data_cache()
    }
}

impl CachedObject<InvertedIndexFile, DefaultHashBuilder, InvertedIndexFileMeter>
    for InvertedIndexFile
{
    type Cache = InvertedIndexFileCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_inverted_index_file_cache()
    }
}

impl CachedObject<InvertedIndexMeta> for InvertedIndexMeta {
    type Cache = InvertedIndexMetaCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_inverted_index_meta_cache()
    }
}

pub struct ColumnArrayMeter;

impl<K, V> Meter<K, Arc<(V, usize)>> for ColumnArrayMeter {
    type Measure = usize;
    fn measure<Q: ?Sized>(&self, _: &Q, v: &Arc<(V, usize)>) -> usize
    where K: Borrow<Q> {
        v.1
    }
}

pub struct CompactSegmentInfoMeter;

impl Meter<String, Arc<CompactSegmentInfo>> for CompactSegmentInfoMeter {
    type Measure = usize;

    fn measure<Q: ?Sized>(&self, _: &Q, value: &Arc<CompactSegmentInfo>) -> Self::Measure {
        std::mem::size_of::<CompactSegmentInfo>() + value.raw_block_metas.bytes.len()
    }
}

pub struct BloomIndexFilterMeter;

impl Meter<String, Arc<Xor8Filter>> for BloomIndexFilterMeter {
    type Measure = usize;

    fn measure<Q: ?Sized>(&self, _: &Q, value: &Arc<Xor8Filter>) -> Self::Measure {
        std::mem::size_of::<Xor8Filter>() + value.filter.finger_prints.len()
    }
}

pub struct InvertedIndexFileMeter;

impl Meter<String, Arc<InvertedIndexFile>> for InvertedIndexFileMeter {
    type Measure = usize;

    fn measure<Q: ?Sized>(&self, _: &Q, value: &Arc<InvertedIndexFile>) -> Self::Measure {
        std::mem::size_of::<InvertedIndexFile>() + value.data.len()
    }
}
