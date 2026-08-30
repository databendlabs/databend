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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use arrow::array::ArrayRef;
use bytes::Bytes;
use databend_common_cache::Cache;
use databend_common_cache::LruCache;
use databend_common_cache::MemSized;
use parking_lot::RwLock;

use crate::CacheAccessor;
use crate::InMemoryLruCache;
pub use crate::cache_items::*;
use crate::manager::CacheManager;
use crate::providers::HybridCache;

/// In memory object cache of SegmentInfo
pub type CompactSegmentInfoCache = InMemoryLruCache<CompactSegmentInfo>;

/// In memory object cache of ColumnOrientedSegmentInfo
pub type ColumnOrientedSegmentInfoCache = InMemoryLruCache<ColumnOrientedSegment>;

/// Note that this cache may be memory-intensive, as each item of this cache
/// contains ALL the BlockMeta of a segment, for well-compacted segment, the
/// number of BlockMeta might be 1000 ~ 2000.
pub type SegmentBlockMetasCache = InMemoryLruCache<Vec<Arc<BlockMeta>>>;

/// In-memory cache of individual BlockMeta.
pub type BlockMetaCache = InMemoryLruCache<BlockMeta>;

/// In memory object cache of TableSnapshot
pub type TableSnapshotCache = InMemoryLruCache<TableSnapshot>;
/// In memory object cache of TableSnapshotStatistics
pub type TableSnapshotStatisticCache = InMemoryLruCache<TableSnapshotStatistics>;
/// In memory object cache of SegmentStatistics
pub type SegmentStatisticsCache = InMemoryLruCache<SegmentStatistics>;
/// In memory object cache of bloom filter.
/// For each indexed data block, the bloom xor8 filter of column is cached individually
pub type BloomIndexFilterCache = HybridCache<FilterImpl>;
/// In memory object cache of parquet FileMetaData of bloom index data
pub type BloomIndexMetaCache = HybridCache<BloomIndexMeta>;

pub type InvertedIndexMetaCache = HybridCache<InvertedIndexMeta>;
pub type InvertedIndexFileCache = HybridCache<InvertedIndexFile>;

pub type VectorIndexMetaCache = HybridCache<VectorIndexMeta>;
pub type VectorIndexFileCache = HybridCache<VectorIndexFile>;

pub type SpatialIndexMetaCache = HybridCache<SpatialIndexMeta>;
pub type SpatialIndexFileCache = HybridCache<SpatialIndexFile>;

pub type VirtualColumnMetaCache = HybridCache<VirtualColumnFileMeta>;

/// In memory object cache of parquet FileMetaData of external parquet rs files
pub type ParquetMetaDataCache = InMemoryLruCache<ParquetMetaData>;

/// Temporary per-query diagnostics for cache lock contention. Remove this
/// together with the `[FUSE-PRUNER-DIAG]` logs after the investigation.
#[derive(Debug, Default)]
pub struct CacheLockStats {
    memory_wait_ns: AtomicU64,
    memory_hold_ns: AtomicU64,
    memory_acquires: AtomicU64,
    disk_wait_ns: AtomicU64,
    disk_hold_ns: AtomicU64,
    disk_acquires: AtomicU64,
}

impl CacheLockStats {
    pub fn record_memory(&self, wait: Duration, hold: Duration) {
        self.memory_wait_ns
            .fetch_add(duration_ns(wait), Ordering::Relaxed);
        self.memory_hold_ns
            .fetch_add(duration_ns(hold), Ordering::Relaxed);
        self.memory_acquires.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_disk(&self, wait: Duration, hold: Duration) {
        self.disk_wait_ns
            .fetch_add(duration_ns(wait), Ordering::Relaxed);
        self.disk_hold_ns
            .fetch_add(duration_ns(hold), Ordering::Relaxed);
        self.disk_acquires.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> CacheLockStatsSnapshot {
        CacheLockStatsSnapshot {
            memory_wait_ns: self.memory_wait_ns.load(Ordering::Relaxed),
            memory_hold_ns: self.memory_hold_ns.load(Ordering::Relaxed),
            memory_acquires: self.memory_acquires.load(Ordering::Relaxed),
            disk_wait_ns: self.disk_wait_ns.load(Ordering::Relaxed),
            disk_hold_ns: self.disk_hold_ns.load(Ordering::Relaxed),
            disk_acquires: self.disk_acquires.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CacheLockStatsSnapshot {
    pub memory_wait_ns: u64,
    pub memory_hold_ns: u64,
    pub memory_acquires: u64,
    pub disk_wait_ns: u64,
    pub disk_hold_ns: u64,
    pub disk_acquires: u64,
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

/// Raw bytes of one immutable granule-index file.
#[derive(Clone)]
pub struct GranuleIndexFile(Bytes);

impl GranuleIndexFile {
    pub fn new(data: Bytes) -> Self {
        Self(data)
    }

    pub fn data(&self) -> Bytes {
        self.0.clone()
    }
}

impl MemSized for GranuleIndexFile {
    fn mem_bytes(&self) -> usize {
        self.0.len()
    }
}

/// A direct byte-sized LRU for granule-index files. This intentionally does
/// not implement `CacheAccessor`: granule-index files are immutable and callers
/// use the underlying LRU semantics directly.
#[derive(Clone)]
pub struct GranuleIndexFileCache {
    inner: Arc<RwLock<LruCache<String, GranuleIndexFile>>>,
}

impl GranuleIndexFileCache {
    pub fn new(bytes_capacity: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(LruCache::with_bytes_capacity(bytes_capacity))),
        }
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        self.get_with_stats(key, None)
    }

    pub fn get_with_stats(&self, key: &str, stats: Option<&CacheLockStats>) -> Option<Bytes> {
        let wait_start = Instant::now();
        let mut cache = self.inner.write();
        let wait = wait_start.elapsed();
        let hold_start = Instant::now();
        let result = cache.get(key).map(GranuleIndexFile::data);
        let hold = hold_start.elapsed();
        drop(cache);
        if let Some(stats) = stats {
            stats.record_memory(wait, hold);
        }
        result
    }

    pub fn insert(&self, key: String, data: Bytes) {
        self.insert_with_stats(key, data, None)
    }

    pub fn insert_with_stats(&self, key: String, data: Bytes, stats: Option<&CacheLockStats>) {
        let wait_start = Instant::now();
        let mut cache = self.inner.write();
        let wait = wait_start.elapsed();
        let hold_start = Instant::now();
        cache.insert(key, GranuleIndexFile::new(data));
        let hold = hold_start.elapsed();
        drop(cache);
        if let Some(stats) = stats {
            stats.record_memory(wait, hold);
        }
    }

    pub fn clear(&self) {
        self.inner.write().clear();
    }

    pub fn set_bytes_capacity(&self, capacity: usize) {
        self.inner.write().set_bytes_capacity(capacity);
    }
}

pub type PrunePartitionsCache = InMemoryLruCache<(PartStatistics, Partitions)>;

pub struct IcebergTableCacheValue {
    table: Arc<dyn Table>,
    refreshing: AtomicBool,
    loaded_at: Instant,
    credential_refresh_at: Option<Instant>,
}

impl IcebergTableCacheValue {
    pub fn new(table: Arc<dyn Table>, credential_refresh_at: Option<Instant>) -> Self {
        Self {
            table,
            refreshing: AtomicBool::new(false),
            loaded_at: Instant::now(),
            credential_refresh_at,
        }
    }

    pub fn table(&self) -> Arc<dyn Table> {
        self.table.clone()
    }

    pub(crate) fn loaded_at(&self) -> Instant {
        self.loaded_at
    }

    pub(crate) fn credential_refresh_at(&self) -> Option<Instant> {
        self.credential_refresh_at
    }

    pub(crate) fn is_refreshing(&self) -> bool {
        self.refreshing.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn set_refreshing(&self) {
        self.refreshing
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

pub type IcebergTableCache = InMemoryLruCache<IcebergTableCacheValue>;

/// In memory object cache of table column array
pub type ColumnArrayCache = InMemoryLruCache<SizedColumnArray>;
pub type ArrayRawDataUncompressedSize = usize;
pub type SizedColumnArray = (ArrayRef, ArrayRawDataUncompressedSize);

// Bind Type of cached objects to Caches
//
// The `Cache` should return
// - cache item of Type `T`
// - and implement `CacheAccessor` properly
pub trait CachedObject<T> {
    type Cache: CacheAccessor<V = T>;
    fn cache() -> Option<Self::Cache>;
}

impl CachedObject<CompactSegmentInfo> for SegmentInfo {
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
    type Cache = SegmentBlockMetasCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_segment_block_metas_cache()
    }
}

impl CachedObject<IcebergTableCacheValue> for IcebergTableCacheValue {
    type Cache = IcebergTableCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_iceberg_table_cache()
    }
}

impl CachedObject<TableSnapshotStatistics> for TableSnapshotStatistics {
    type Cache = TableSnapshotStatisticCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_table_snapshot_statistics_cache()
    }
}

impl CachedObject<SegmentStatistics> for SegmentStatistics {
    type Cache = SegmentStatisticsCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_segment_statistics_cache()
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

impl CachedObject<FilterImpl> for FilterImpl {
    type Cache = BloomIndexFilterCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_bloom_index_filter_cache()
    }
}

impl CachedObject<ParquetMetaData> for ParquetMetaData {
    type Cache = ParquetMetaDataCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_parquet_meta_data_cache()
    }
}

impl CachedObject<InvertedIndexFile> for InvertedIndexFile {
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

impl CachedObject<VirtualColumnFileMeta> for VirtualColumnFileMeta {
    type Cache = VirtualColumnMetaCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_virtual_column_meta_cache()
    }
}

pub struct CacheValue<T> {
    inner: Arc<T>,
    mem_bytes: usize,
}

impl<T> CacheValue<T> {
    pub fn new(inner: T, mem_bytes: usize) -> Self {
        Self {
            inner: Arc::new(inner),
            mem_bytes,
        }
    }

    pub fn get_inner(&self) -> Arc<T> {
        self.inner.clone()
    }
}

impl From<CompactSegmentInfo> for CacheValue<CompactSegmentInfo> {
    fn from(value: CompactSegmentInfo) -> Self {
        CacheValue {
            mem_bytes: std::mem::size_of::<CompactSegmentInfo>()
                + value.raw_block_metas.bytes.len(),
            inner: Arc::new(value),
        }
    }
}

impl From<ColumnOrientedSegment> for CacheValue<ColumnOrientedSegment> {
    fn from(value: ColumnOrientedSegment) -> Self {
        CacheValue {
            mem_bytes: value.block_metas.memory_size()
                + std::mem::size_of::<ColumnOrientedSegment>(),
            inner: Arc::new(value),
        }
    }
}
impl From<Vec<Arc<BlockMeta>>> for CacheValue<Vec<Arc<BlockMeta>>> {
    fn from(value: Vec<Arc<BlockMeta>>) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}

impl From<BlockMeta> for CacheValue<BlockMeta> {
    fn from(value: BlockMeta) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}

impl From<IcebergTableCacheValue> for CacheValue<IcebergTableCacheValue> {
    fn from(value: IcebergTableCacheValue) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}

impl From<TableSnapshot> for CacheValue<TableSnapshot> {
    fn from(value: TableSnapshot) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}

impl From<TableSnapshotStatistics> for CacheValue<TableSnapshotStatistics> {
    fn from(value: TableSnapshotStatistics) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}

impl From<SegmentStatistics> for CacheValue<SegmentStatistics> {
    fn from(value: SegmentStatistics) -> Self {
        CacheValue {
            mem_bytes: value.memory_size(),
            inner: Arc::new(value),
        }
    }
}

impl From<FilterImpl> for CacheValue<FilterImpl> {
    fn from(value: FilterImpl) -> Self {
        CacheValue {
            mem_bytes: value.mem_bytes(),
            inner: Arc::new(value),
        }
    }
}

impl From<BloomIndexMeta> for CacheValue<BloomIndexMeta> {
    fn from(value: BloomIndexMeta) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}

impl From<ColumnData> for CacheValue<ColumnData> {
    fn from(value: ColumnData) -> Self {
        CacheValue {
            mem_bytes: value.size(),
            inner: Arc::new(value),
        }
    }
}

impl From<InvertedIndexMeta> for CacheValue<InvertedIndexMeta> {
    fn from(value: InvertedIndexMeta) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}

impl From<InvertedIndexFile> for CacheValue<InvertedIndexFile> {
    fn from(value: InvertedIndexFile) -> Self {
        CacheValue {
            mem_bytes: std::mem::size_of::<InvertedIndexFile>() + value.data.len(),
            inner: Arc::new(value),
        }
    }
}

impl From<IndexMeta> for CacheValue<IndexMeta> {
    fn from(value: IndexMeta) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}

impl From<IndexFile> for CacheValue<IndexFile> {
    fn from(value: IndexFile) -> Self {
        CacheValue {
            mem_bytes: std::mem::size_of::<IndexFile>() + value.data.len(),
            inner: Arc::new(value),
        }
    }
}

impl From<VirtualColumnFileMeta> for CacheValue<VirtualColumnFileMeta> {
    fn from(value: VirtualColumnFileMeta) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}

impl From<ParquetMetaData> for CacheValue<ParquetMetaData> {
    fn from(value: ParquetMetaData) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}

impl From<(PartStatistics, Partitions)> for CacheValue<(PartStatistics, Partitions)> {
    fn from(value: (PartStatistics, Partitions)) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}

impl From<SizedColumnArray> for CacheValue<SizedColumnArray> {
    fn from(value: SizedColumnArray) -> Self {
        CacheValue {
            mem_bytes: value.1,
            inner: Arc::new(value),
        }
    }
}

pub struct FileSize(pub u64);

impl From<FileSize> for CacheValue<FileSize> {
    fn from(value: FileSize) -> Self {
        CacheValue {
            mem_bytes: value.0 as usize,
            inner: Arc::new(value),
        }
    }
}

impl<T> MemSized for CacheValue<T> {
    fn mem_bytes(&self) -> usize {
        self.mem_bytes
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::CacheLockStats;
    use super::GranuleIndexFileCache;

    #[test]
    fn test_granule_index_file_cache_evicts_by_bytes() {
        let cache = GranuleIndexFileCache::new(5);
        cache.insert("a".to_string(), Bytes::from_static(b"123"));
        cache.insert("b".to_string(), Bytes::from_static(b"456"));

        assert!(cache.get("a").is_none());
        assert_eq!(cache.get("b").as_deref(), Some(b"456".as_slice()));

        let stats = CacheLockStats::default();
        cache.insert_with_stats("c".to_string(), Bytes::from_static(b"xy"), Some(&stats));
        assert_eq!(
            cache.get_with_stats("c", Some(&stats)).as_deref(),
            Some(b"xy".as_slice())
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.memory_acquires, 2);

        cache.set_bytes_capacity(2);
        assert!(cache.get("b").is_none());
    }
}
