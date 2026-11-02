// Copyright 2026 Datafuse Labs
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

use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use databend_common_exception::Result;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::CachedItem;
use databend_storages_common_cache::DiskFill;
use databend_storages_common_cache::KindLimit;
use databend_storages_common_cache::MemCache;
use databend_storages_common_cache::Pool;
use databend_storages_common_cache::ReadCtx;
use databend_storages_common_cache::Reader;
use databend_storages_common_cache::backend::InsertOutcome;
use databend_storages_common_cache::backend::KindStats;
use databend_storages_common_cache::backend::KindStore;
use databend_storages_common_cache::backend::PoolStats;
use databend_storages_common_cache::backend::ShrinkReport;
use databend_storages_common_cache::kinds::segment;
use databend_storages_common_cache::kinds::table;
use futures::future::join_all;
use tokio::sync::Notify;

use crate::fakes::FakeReader;
use crate::fakes::Item;
use crate::fakes::Live;
use crate::fakes::cache;
use crate::fakes::data_kind;
use crate::fakes::meta_kind;
use crate::fakes::shared_store;
use crate::kinds::samples::Sample;

/// The first read has already obtained the old data, but cannot complete until released.
struct UpdatingReader<T> {
    old: T,
    new: T,
    reads: AtomicUsize,
    started: Notify,
    release: Notify,
}

impl<T: CachedItem + Clone> Reader for UpdatingReader<T> {
    type Key = String;
    type Item = T;

    fn cache_key<'a>(&self, key: &'a String) -> Cow<'a, str> {
        Cow::Borrowed(key)
    }

    async fn read(&self, _: &String, _: &ReadCtx) -> Result<T> {
        if self.reads.fetch_add(1, Ordering::Relaxed) == 0 {
            let old = self.old.clone();
            self.started.notify_one();
            self.release.notified().await;
            Ok(old)
        } else {
            Ok(self.new.clone())
        }
    }
}

async fn assert_update_does_not_join_old_load<K: Sample>()
where K::Value: Clone {
    let manager = CacheManager::new(shared_store(1 << 30, 0, 0), None);
    let cache = manager.cache::<K>();
    let reader = UpdatingReader {
        old: K::sample(1),
        new: K::sample(2),
        reads: AtomicUsize::new(0),
        started: Notify::new(),
        release: Notify::new(),
    };
    let (old, fresh) = tokio::join!(cache.get(&reader, "k".to_string()), async {
        reader.started.notified().await;
        cache.put("k", reader.new.clone());
        // The new value can leave through capacity eviction before the old load completes.
        assert!(cache.evict("k"));
        let fresh =
            tokio::time::timeout(Duration::from_secs(10), cache.get(&reader, "k".to_string()))
                .await;
        reader.release.notify_one();
        fresh
    });
    old.unwrap();
    fresh
        .expect("a post-update miss joined the old load")
        .unwrap();
    assert_eq!(reader.reads.load(Ordering::Relaxed), 2);
    // The old load may still fill late: this test does not require a new invalidation protocol.
}

#[tokio::test]
async fn test_segment_update_does_not_share_old_load() {
    assert_update_does_not_join_old_load::<segment::Info>().await;
}

#[tokio::test]
async fn test_segment_stats_update_does_not_share_old_load() {
    assert_update_does_not_join_old_load::<segment::Stats>().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_reads_once_for_concurrent_misses() -> Result<()> {
    let cache = cache(&shared_store(1 << 20, 1 << 20, 1 << 20), meta_kind());
    let reader = FakeReader::slow(10, Duration::from_millis(50));
    let results = join_all((0..100).map(|_| cache.get(&reader, "k".to_string()))).await;
    assert!(results.iter().all(|r| r.is_ok()));
    assert_eq!(reader.reads(), 1);
    assert!(cache.contains("k"));
    Ok(())
}

/// Models a publication immediately after the first lookup observed a miss. The next
/// lookup sees the real store, without relying on sleeps between lookup and registration.
struct MissThenHit {
    inner: Arc<dyn KindStore>,
    gets: AtomicUsize,
}

type Erased = Arc<dyn std::any::Any + Send + Sync>;

impl KindStore for MissThenHit {
    fn is_enabled(&self, kind: CacheKind) -> bool {
        self.inner.is_enabled(kind)
    }

    fn get(&self, kind: CacheKind, key: &str) -> Option<Erased> {
        if self.gets.fetch_add(1, Ordering::Relaxed) == 0 {
            None
        } else {
            self.inner.get(kind, key)
        }
    }

    fn contains(&self, kind: CacheKind, key: &str) -> bool {
        self.inner.contains(kind, key)
    }

    fn insert(&self, kind: CacheKind, key: &str, value: Erased, bytes: usize) -> InsertOutcome {
        self.inner.insert(kind, key, value, bytes)
    }

    fn evict(&self, kind: CacheKind, key: &str) -> bool {
        self.inner.evict(kind, key)
    }

    fn set_kind_limit(&self, kind: CacheKind, limit: KindLimit) {
        self.inner.set_kind_limit(kind, limit);
    }

    fn set_pool_cap(&self, pool: Pool, bytes: usize) -> Result<()> {
        self.inner.set_pool_cap(pool, bytes)
    }

    fn kind_stats(&self, kind: CacheKind) -> KindStats {
        self.inner.kind_stats(kind)
    }

    fn pool_stats(&self) -> Vec<PoolStats> {
        self.inner.pool_stats()
    }

    fn shrink_for_pressure(&self, relieved: &dyn Fn() -> bool) -> ShrinkReport {
        self.inner.shrink_for_pressure(relieved)
    }
}

#[tokio::test]
async fn test_flight_rechecks_a_miss_before_loading() -> Result<()> {
    let inner = shared_store(1 << 20, 0, 0);
    let live = Live::default();
    cache(&inner, meta_kind()).put("k", live.item(10));
    let store = Arc::new(MissThenHit {
        inner,
        gets: AtomicUsize::new(0),
    });
    let cache = MemCache::new(store.clone(), meta_kind());
    let reader = FakeReader::new(20);
    let value = cache.get(&reader, "k".to_string()).await?;
    assert_eq!(value.bytes, 10);
    assert_eq!(reader.reads(), 0);
    assert_eq!(store.gets.load(Ordering::Relaxed), 2);
    Ok(())
}

#[tokio::test]
async fn test_get_hits_after_first_read() -> Result<()> {
    let cache = cache(&shared_store(1 << 20, 0, 0), meta_kind());
    let reader = FakeReader::new(10);
    let first = cache.get(&reader, "k".to_string()).await?;
    let second = cache.get(&reader, "k".to_string()).await?;
    assert!(first.is_charged());
    assert_eq!(second.bytes, 10);
    assert_eq!(reader.reads(), 1);
    Ok(())
}

#[tokio::test]
async fn test_read_only_does_not_fill() -> Result<()> {
    let cache = cache(&shared_store(1 << 20, 0, 0), meta_kind());
    let reader = FakeReader::new(10);
    let value = cache.read_only().get(&reader, "k".to_string()).await?;
    assert!(!value.is_charged());
    assert!(!cache.contains("k"));

    let live = Live::default();
    cache.read_only().put("p", live.item(10));
    assert!(cache.contains("p"));
    Ok(())
}

/// A full-cache caller joining a load started by a read-only view gets the value but no
/// fill; the next lookup misses and fills.
#[tokio::test(flavor = "multi_thread")]
async fn test_read_only_load_shared_with_full_cache_does_not_fill() -> Result<()> {
    let cache = cache(&shared_store(1 << 20, 0, 0), meta_kind());
    let reader = FakeReader::slow(10, Duration::from_millis(50));
    let read_only = cache.read_only();
    let (a, b) = tokio::join!(read_only.get(&reader, "k".to_string()), async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        cache.get(&reader, "k".to_string()).await
    });
    assert!(!a?.is_charged() && !b?.is_charged());
    assert_eq!(reader.reads(), 1);
    assert!(!cache.contains("k"));

    assert!(cache.get(&reader, "k".to_string()).await?.is_charged());
    assert_eq!(reader.reads(), 2);
    Ok(())
}

/// Data is cached on its first read, as every other pool.
#[test]
fn test_data_pool_admits_on_first_access() {
    let cache = cache(&shared_store(0, 0, 1 << 20), data_kind());
    let live = Live::default();
    assert!(cache.fill("k", live.item(10)).is_charged());
    assert!(cache.contains("k"));
    assert!(cache.put("p", live.item(10)).is_charged());
}

#[test]
fn test_evicted_value_stays_charged_until_dropped() {
    let store = shared_store(1 << 20, 0, 0);
    let cache = cache(&store, meta_kind());
    let live = Live::default();
    let held = cache.put("k", live.item(100));
    assert!(cache.evict("k"));
    let stats = store.kind_stats(CacheKind::of::<table::Snapshot>());
    assert_eq!((stats.resident, stats.in_cache, stats.items), (100, 0, 0));
    drop(held);
    assert_eq!(
        store
            .kind_stats(CacheKind::of::<table::Snapshot>())
            .resident,
        0
    );
    assert_eq!(live.count(), 0);
}

#[test]
fn test_get_sync_computes_once() -> Result<()> {
    let cache = cache(&shared_store(1 << 20, 0, 0), meta_kind());
    let live = Live::default();
    let mut computed = 0;
    for _ in 0..3 {
        cache.get_sync("k", || {
            computed += 1;
            Ok(live.item(10))
        })?;
    }
    assert_eq!(computed, 1);
    Ok(())
}

/// Records what the handle let it do with the disk cache.
struct CtxReader {
    live: Live,
    seen: parking_lot::Mutex<Vec<DiskFill>>,
}

impl Reader for CtxReader {
    type Key = String;
    type Item = Item;

    fn cache_key<'a>(&self, key: &'a String) -> Cow<'a, str> {
        Cow::Borrowed(key)
    }

    async fn read(&self, _: &String, ctx: &ReadCtx) -> Result<Item> {
        self.seen.lock().push(ctx.disk);
        Ok(self.live.item(10))
    }
}

/// A miss through a handle that fills reads and fills the disk cache; through a read-only view
/// it reads the disk cache but does not fill it, as for background tasks.
#[tokio::test]
async fn test_handle_passes_its_disk_fill_to_the_reader() -> Result<()> {
    let cache = cache(&shared_store(1 << 20, 1 << 20, 1 << 20), meta_kind());
    let reader = CtxReader {
        live: Live::default(),
        seen: Default::default(),
    };
    assert_eq!(cache.disk_fill(), DiskFill::ReadWrite);
    cache.get(&reader, "a".to_string()).await?;
    let view = cache.read_only();
    assert_eq!(view.disk_fill(), DiskFill::ReadOnly);
    view.get(&reader, "b".to_string()).await?;
    assert_eq!(*reader.seen.lock(), [
        DiskFill::ReadWrite,
        DiskFill::ReadOnly
    ]);
    Ok(())
}

/// A view without disk fill, for maintenance reads: the reader reads the disk cache without
/// filling it, and the memory tier still fills; a read-only view of it fills neither.
#[tokio::test]
async fn test_view_without_disk_fill_still_fills_memory() -> Result<()> {
    let cache = cache(&shared_store(1 << 20, 1 << 20, 1 << 20), meta_kind());
    let reader = CtxReader {
        live: Live::default(),
        seen: Default::default(),
    };
    let view = cache.without_disk_fill();
    assert_eq!(view.disk_fill(), DiskFill::ReadOnly);
    assert!(!view.is_read_only());
    view.get(&reader, "a".to_string()).await?;
    assert!(cache.contains("a"));
    let read_only = view.read_only();
    read_only.get(&reader, "b".to_string()).await?;
    assert!(!cache.contains("b"));
    assert_eq!(*reader.seen.lock(), [
        DiskFill::ReadOnly,
        DiskFill::ReadOnly
    ]);
    Ok(())
}
