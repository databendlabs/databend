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

use databend_common_exception::Result;
use databend_storages_common_cache::MemCache;
use databend_storages_common_cache::Reader;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;

/// Location and format version of a snapshot or snapshot statistics file.
pub type SnapshotKey = (String, u64);

/// Table snapshots and their statistics. Values are handed out as `Arc`, so holders outside
/// the cache are not counted as pinned (see `CachedItem for Arc<T>`).
#[derive(Clone)]
pub struct SnapshotCache {
    snapshots: MemCache<Arc<TableSnapshot>>,
    stats: MemCache<Arc<TableSnapshotStatistics>>,
}

impl SnapshotCache {
    pub fn new(
        snapshots: MemCache<Arc<TableSnapshot>>,
        stats: MemCache<Arc<TableSnapshotStatistics>>,
    ) -> Self {
        SnapshotCache { snapshots, stats }
    }

    pub async fn get(
        &self,
        reader: &impl Reader<Key = SnapshotKey, Item = Arc<TableSnapshot>>,
        location: String,
        ver: u64,
    ) -> Result<Arc<TableSnapshot>> {
        let value = self.snapshots.get(reader, (location, ver)).await?;
        Ok(Arc::clone(&value))
    }

    pub async fn get_stats(
        &self,
        reader: &impl Reader<Key = SnapshotKey, Item = Arc<TableSnapshotStatistics>>,
        location: String,
        ver: u64,
    ) -> Result<Arc<TableSnapshotStatistics>> {
        let value = self.stats.get(reader, (location, ver)).await?;
        Ok(Arc::clone(&value))
    }

    pub fn put(&self, location: &str, snapshot: Arc<TableSnapshot>) {
        self.snapshots.put(location, snapshot);
    }

    pub fn put_stats(&self, location: &str, stats: Arc<TableSnapshotStatistics>) {
        self.stats.put(location, stats);
    }

    pub fn contains(&self, location: &str) -> bool {
        self.snapshots.contains(location)
    }

    pub fn evict(&self, location: &str) -> bool {
        self.snapshots.evict(location)
    }

    pub fn read_only(&self) -> Self {
        SnapshotCache {
            snapshots: self.snapshots.read_only(),
            stats: self.stats.read_only(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use databend_common_exception::ErrorCode;
    use databend_common_expression::TableSchema;
    use databend_storages_common_cache::CacheKind;
    use databend_storages_common_cache::Reader;
    use databend_storages_common_cache::cache_manager::CacheManager;
    use databend_storages_common_cache::store::LegacyCapacity;
    use databend_storages_common_cache::store::LegacyStore;
    use databend_storages_common_table_meta::meta::Statistics;
    use databend_storages_common_table_meta::meta::TableMetaTimestamps;

    use super::*;
    use crate::io::FuseCaches;

    fn snapshot() -> Arc<TableSnapshot> {
        let snapshot = TableSnapshot::try_new(
            None,
            None,
            TableSchema::empty(),
            Statistics::default(),
            vec![],
            None,
            None,
            TableMetaTimestamps::new(None, chrono::Duration::hours(1)),
        )
        .unwrap();
        Arc::new(snapshot)
    }

    /// Serves snapshots from memory; locations starting with `missing` are not found.
    #[derive(Default)]
    struct SnapshotFiles {
        reads: AtomicUsize,
    }

    impl SnapshotFiles {
        fn reads(&self) -> usize {
            self.reads.load(Ordering::Relaxed)
        }
    }

    impl Reader for SnapshotFiles {
        type Key = SnapshotKey;
        type Item = Arc<TableSnapshot>;

        fn cache_key<'a>(&self, key: &'a SnapshotKey) -> Cow<'a, str> {
            Cow::Borrowed(&key.0)
        }

        async fn read(&self, (location, _): &SnapshotKey) -> Result<Arc<TableSnapshot>> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            if location.starts_with("missing") {
                return Err(ErrorCode::StorageNotFound(location.clone()));
            }
            Ok(snapshot())
        }
    }

    struct StatsFiles;

    impl Reader for StatsFiles {
        type Key = SnapshotKey;
        type Item = Arc<TableSnapshotStatistics>;

        fn cache_key<'a>(&self, key: &'a SnapshotKey) -> Cow<'a, str> {
            Cow::Borrowed(&key.0)
        }

        async fn read(&self, _key: &SnapshotKey) -> Result<Arc<TableSnapshotStatistics>> {
            Ok(Arc::new(TableSnapshotStatistics::new(
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                Default::default(),
                0,
            )))
        }
    }

    fn caches(snapshot_items: usize) -> (CacheManager, Arc<FuseCaches>) {
        let store = Arc::new(LegacyStore::new([
            (
                CacheKind::TableSnapshot,
                LegacyCapacity::Items(snapshot_items),
            ),
            (CacheKind::TableSnapshotStats, LegacyCapacity::Items(10)),
        ]));
        let manager = CacheManager::new(store, HashMap::new());
        let caches = FuseCaches::new(&manager);
        (manager, caches)
    }

    fn items(manager: &CacheManager, kind: CacheKind) -> usize {
        manager
            .kind_stats()
            .into_iter()
            .find(|s| s.kind == kind)
            .unwrap()
            .items
    }

    #[tokio::test]
    async fn test_snapshot_get_reads_once_and_shares_the_arc() -> Result<()> {
        let (_, caches) = caches(10);
        let files = SnapshotFiles::default();
        let first = caches.snapshots.get(&files, "s1".into(), 4).await?;
        let second = caches.snapshots.get(&files, "s1".into(), 4).await?;
        assert_eq!(files.reads(), 1);
        assert!(Arc::ptr_eq(&first, &second));
        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_read_only_does_not_fill() -> Result<()> {
        let (manager, caches) = caches(10);
        let read_only = caches.read_only();
        let files = SnapshotFiles::default();

        read_only.snapshots.get(&files, "s1".into(), 4).await?;
        assert_eq!(items(&manager, CacheKind::TableSnapshot), 0);

        // Read-only views still see entries filled elsewhere, and writes still go in.
        caches.snapshots.get(&files, "s1".into(), 4).await?;
        read_only.snapshots.get(&files, "s1".into(), 4).await?;
        assert_eq!(files.reads(), 2);
        read_only.snapshots.put("s2", snapshot());
        assert!(caches.snapshots.contains("s2"));
        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_not_found_is_not_cached() -> Result<()> {
        let (manager, caches) = caches(10);
        let files = SnapshotFiles::default();
        for _ in 0..2 {
            let err = caches.snapshots.get(&files, "missing".into(), 4).await;
            assert_eq!(err.unwrap_err().code(), ErrorCode::STORAGE_NOT_FOUND);
        }
        assert_eq!(files.reads(), 2);
        assert_eq!(items(&manager, CacheKind::TableSnapshot), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_evict_and_capacity() -> Result<()> {
        let (manager, caches) = caches(2);
        let files = SnapshotFiles::default();
        for loc in ["a", "b", "c"] {
            caches.snapshots.get(&files, loc.into(), 4).await?;
        }
        assert_eq!(items(&manager, CacheKind::TableSnapshot), 2);
        assert!(!caches.snapshots.contains("a"));

        assert!(caches.snapshots.evict("c"));
        assert!(!caches.snapshots.contains("c"));
        caches.snapshots.get(&files, "c".into(), 4).await?;
        assert_eq!(files.reads(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_stats_are_a_separate_kind() -> Result<()> {
        let (manager, caches) = caches(10);
        caches.snapshots.put("loc", snapshot());
        let stats = caches
            .snapshots
            .get_stats(&StatsFiles, "loc".into(), 0)
            .await?;
        assert_eq!(stats.row_count, 0);
        assert_eq!(items(&manager, CacheKind::TableSnapshot), 1);
        assert_eq!(items(&manager, CacheKind::TableSnapshotStats), 1);

        // Evicting a snapshot leaves the statistics at the same location alone.
        caches.snapshots.evict("loc");
        assert_eq!(items(&manager, CacheKind::TableSnapshotStats), 1);
        Ok(())
    }
}
