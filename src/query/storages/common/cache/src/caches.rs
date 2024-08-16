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

use std::sync::Arc;

use databend_common_arrow::parquet::metadata::FileMetaData;
use databend_common_cache::Count;
use databend_common_cache::CountableMeter;
use databend_common_cache::Meter;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_storages_common_index::filters::Xor8Filter;
use databend_storages_common_index::BloomIndexMeta;
use databend_storages_common_index::InvertedIndexFile;
use databend_storages_common_index::InvertedIndexMeta;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;

use crate::manager::CacheManager;
use crate::CacheAccessor;
use crate::InMemoryLruCache;

/// In memory object cache of SegmentInfo
pub type CompactSegmentInfoCache = InMemoryLruCache<CompactSegmentInfo, MemSizedMeter>;

pub type BlockMetaCache = InMemoryLruCache<Vec<Arc<BlockMeta>>>;

/// In memory object cache of TableSnapshot
pub type TableSnapshotCache = InMemoryLruCache<TableSnapshot>;
/// In memory object cache of TableSnapshotStatistics
pub type TableSnapshotStatisticCache = InMemoryLruCache<TableSnapshotStatistics>;
/// In memory object cache of bloom filter.
/// For each indexed data block, the bloom xor8 filter of column is cached individually
pub type BloomIndexFilterCache = InMemoryLruCache<Xor8Filter, MemSizedMeter>;
/// In memory object cache of parquet FileMetaData of bloom index data
pub type BloomIndexMetaCache = InMemoryLruCache<BloomIndexMeta>;

pub type InvertedIndexMetaCache = InMemoryLruCache<InvertedIndexMeta>;
pub type InvertedIndexFileCache = InMemoryLruCache<InvertedIndexFile, MemSizedMeter>;

/// In memory object cache of parquet FileMetaData of external parquet files
pub type FileMetaDataCache = InMemoryLruCache<FileMetaData>;

pub type PrunePartitionsCache = InMemoryLruCache<(PartStatistics, Partitions)>;

/// In memory object cache of table column array
pub type ColumnArrayCache = InMemoryLruCache<SizedColumnArray, MemSizedMeter>;
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
pub trait CachedObject<T, M = Count>
where M: CountableMeter<String, Arc<T>>
{
    type Cache: CacheAccessor<V = T, M = M>;
    fn cache() -> Option<Self::Cache>;
}

impl CachedObject<CompactSegmentInfo, MemSizedMeter> for CompactSegmentInfo {
    type Cache = CompactSegmentInfoCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_table_segment_cache()
    }
}

impl CachedObject<CompactSegmentInfo, MemSizedMeter> for SegmentInfo {
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

impl CachedObject<Vec<Arc<BlockMeta>>> for Vec<Arc<BlockMeta>> {
    type Cache = BlockMetaCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_block_meta_cache()
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

impl CachedObject<Xor8Filter, MemSizedMeter> for Xor8Filter {
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

impl CachedObject<InvertedIndexFile, MemSizedMeter> for InvertedIndexFile {
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

pub trait MemSized {
    fn mem_bytes(&self) -> usize;
}

impl MemSized for Xor8Filter {
    fn mem_bytes(&self) -> usize {
        std::mem::size_of::<Xor8Filter>() + self.filter.finger_prints.len()
    }
}

impl<V> MemSized for (V, usize) {
    fn mem_bytes(&self) -> usize {
        self.1
    }
}

impl MemSized for InvertedIndexFile {
    fn mem_bytes(&self) -> usize {
        std::mem::size_of::<InvertedIndexFile>() + self.data.len()
    }
}

impl MemSized for CompactSegmentInfo {
    fn mem_bytes(&self) -> usize {
        std::mem::size_of::<CompactSegmentInfo>() + self.raw_block_metas.bytes.len()
    }
}

pub struct MemSizedMeter;

impl<T: MemSized> Meter<String, Arc<T>> for MemSizedMeter {
    type Measure = usize;

    fn measure<Q: ?Sized>(&self, _: &Q, value: &Arc<T>) -> Self::Measure {
        value.mem_bytes()
    }
}
