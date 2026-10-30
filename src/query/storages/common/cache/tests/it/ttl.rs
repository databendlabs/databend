// Copyright 2022 Datafuse Labs.
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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CachedItem;
use databend_storages_common_cache::KindLimit;
use databend_storages_common_cache::MemCache;
use databend_storages_common_cache::PoolCaps;
use databend_storages_common_cache::Reader;
use databend_storages_common_cache::TTLReader;
use databend_storages_common_cache::TTLValue;
use databend_storages_common_cache::backend::KindStore;
use databend_storages_common_cache::legacy::LegacyCapacity;
use databend_storages_common_cache::legacy::LegacyStore;
use databend_storages_common_cache::shared::SharedStore;

const KIND: CacheKind = CacheKind::IcebergTable;

struct Value {
    generation: usize,
    loaded_at: Instant,
    sync_refresh: bool,
    refreshing: AtomicBool,
}

impl CachedItem for Value {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>()
    }
}

impl TTLValue for Value {
    fn is_expired(&self, ttl: &Duration) -> bool {
        self.loaded_at.elapsed() > *ttl
    }

    fn requires_sync_refresh(&self) -> bool {
        self.sync_refresh
    }

    fn is_refreshing(&self) -> bool {
        self.refreshing.load(Ordering::Relaxed)
    }

    fn set_refreshing(&self) {
        self.refreshing.store(true, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct CountingReader {
    loads: Arc<AtomicUsize>,
    fail: Arc<AtomicBool>,
    sync_refresh: bool,
}

impl Reader for CountingReader {
    type Key = String;
    type Item = Value;

    fn cache_key<'a>(&self, key: &'a String) -> Cow<'a, str> {
        Cow::Borrowed(key)
    }

    async fn read(&self, _key: &String) -> Result<Value> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(ErrorCode::StorageOther("test load failure"));
        }
        let generation = self.loads.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(Value {
            generation,
            loaded_at: Instant::now(),
            sync_refresh: self.sync_refresh,
            refreshing: AtomicBool::new(false),
        })
    }
}

struct Fixture {
    reader: TTLReader<CountingReader>,
    cache: MemCache<Value>,
    loads: Arc<AtomicUsize>,
    fail: Arc<AtomicBool>,
}

fn fixture(sync_refresh: bool, ttl: Duration) -> Fixture {
    let store = Arc::new(LegacyStore::new([(KIND, LegacyCapacity::Items(10))]));
    fixture_on(store, sync_refresh, ttl)
}

fn fixture_on(store: Arc<dyn KindStore>, sync_refresh: bool, ttl: Duration) -> Fixture {
    let cache = MemCache::new(store, KIND);
    let loads = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(AtomicBool::new(false));
    let reader = CountingReader {
        loads: loads.clone(),
        fail: fail.clone(),
        sync_refresh,
    };
    Fixture {
        reader: TTLReader::new(cache.clone(), reader, ttl),
        cache,
        loads,
        fail,
    }
}

fn reader(sync_refresh: bool, ttl: Duration) -> (TTLReader<CountingReader>, Arc<AtomicUsize>) {
    let f = fixture(sync_refresh, ttl);
    (f.reader, f.loads)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fresh_value_is_served_from_cache() -> Result<()> {
    let (reader, loads) = reader(false, Duration::from_secs(600));
    assert_eq!(reader.read("t").await?.generation, 1);
    assert_eq!(reader.read("t").await?.generation, 1);
    assert_eq!(loads.load(Ordering::SeqCst), 1);

    reader.refresh("t").await?;
    assert_eq!(reader.read("t").await?.generation, 2);
    assert!(reader.remove("t"));
    assert_eq!(reader.read("t").await?.generation, 3);
    Ok(())
}

/// An expired value is served once more while a single background load replaces it.
#[tokio::test(flavor = "multi_thread")]
async fn test_expired_value_refreshes_in_background() -> Result<()> {
    let (reader, loads) = reader(false, Duration::ZERO);
    assert_eq!(reader.read("t").await?.generation, 1);
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(reader.read("t").await?.generation, 1);
    assert_eq!(reader.read("t").await?.generation, 1);

    let deadline = Instant::now() + Duration::from_secs(10);
    while loads.load(Ordering::SeqCst) < 2 || reader.read("t").await?.generation == 1 {
        assert!(Instant::now() < deadline, "background refresh did not land");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(loads.load(Ordering::SeqCst), 2);
    Ok(())
}

/// A failed background refresh drops the stale value, so the next read reloads it.
#[tokio::test(flavor = "multi_thread")]
async fn test_failed_refresh_drops_stale_value() -> Result<()> {
    let f = fixture(false, Duration::ZERO);
    assert_eq!(f.reader.read("t").await?.generation, 1);
    tokio::time::sleep(Duration::from_millis(1)).await;
    f.fail.store(true, Ordering::SeqCst);
    // Served stale while the refresh fails in the background.
    assert_eq!(f.reader.read("t").await?.generation, 1);
    let deadline = Instant::now() + Duration::from_secs(10);
    while f.cache.contains("t") {
        assert!(Instant::now() < deadline, "stale value was kept");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(f.reader.read("t").await.is_err());
    f.fail.store(false, Ordering::SeqCst);
    assert_eq!(f.reader.read("t").await?.generation, 2);
    Ok(())
}

/// An explicit refresh the cache cannot admit next to the old value drops the old value, so
/// the next read loads the new one.
#[tokio::test(flavor = "multi_thread")]
async fn test_rejected_refresh_drops_old_value() -> Result<()> {
    // The kind's limit fits one value but not the old one and its replacement together.
    let bytes = size_of::<Value>();
    let store = Arc::new(SharedStore::new(
        PoolCaps {
            meta: 1 << 20,
            index: 0,
            data: 0,
        },
        [(KIND, KindLimit::bytes(bytes * 3 / 2))],
        1,
    ));
    let f = fixture_on(store, false, Duration::from_secs(600));
    let old = f.reader.read("t").await?;
    assert_eq!(old.generation, 1);
    // The old value is still held, so the refreshed one (generation 2) does not fit.
    f.reader.refresh("t").await?;
    assert!(!f.cache.contains("t"));
    drop(old);
    // Not served stale: the next read loads again.
    assert_eq!(f.reader.read("t").await?.generation, 3);
    assert!(f.cache.contains("t"));
    Ok(())
}

/// A value that must not be served stale is reloaded before returning.
#[tokio::test(flavor = "multi_thread")]
async fn test_sync_refresh_reloads_before_returning() -> Result<()> {
    let (reader, loads) = reader(true, Duration::ZERO);
    assert_eq!(reader.read("t").await?.generation, 1);
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(reader.read("t").await?.generation, 2);
    assert_eq!(loads.load(Ordering::SeqCst), 2);
    Ok(())
}
