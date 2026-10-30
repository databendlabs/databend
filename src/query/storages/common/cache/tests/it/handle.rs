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

use std::time::Duration;

use databend_common_exception::Result;
use databend_storages_common_cache::CacheKind;
use futures::future::join_all;

use crate::fakes::DATA;
use crate::fakes::FakeReader;
use crate::fakes::Live;
use crate::fakes::META;
use crate::fakes::cache;
use crate::fakes::shared_store;

#[tokio::test(flavor = "multi_thread")]
async fn test_get_reads_once_for_concurrent_misses() -> Result<()> {
    let cache = cache(&shared_store(1 << 20, 1 << 20, 1 << 20), META);
    let reader = FakeReader::slow(10, Duration::from_millis(50));
    let results = join_all((0..100).map(|_| cache.get(&reader, "k".to_string()))).await;
    assert!(results.iter().all(|r| r.is_ok()));
    assert_eq!(reader.reads(), 1);
    assert!(cache.contains("k"));
    Ok(())
}

#[tokio::test]
async fn test_get_hits_after_first_read() -> Result<()> {
    let cache = cache(&shared_store(1 << 20, 0, 0), META);
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
    let cache = cache(&shared_store(1 << 20, 0, 0), META);
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
    let cache = cache(&shared_store(1 << 20, 0, 0), META);
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

#[test]
fn test_data_pool_admits_on_second_access() {
    let cache = cache(&shared_store(0, 0, 1 << 20), DATA);
    let live = Live::default();
    assert!(!cache.fill("k", live.item(10)).is_charged());
    assert!(!cache.contains("k"));
    assert!(cache.fill("k", live.item(10)).is_charged());
    assert!(cache.contains("k"));
    assert!(cache.put("p", live.item(10)).is_charged());
}

#[test]
fn test_column_kinds_are_disabled_by_default() {
    use databend_storages_common_cache::PoolCaps;
    use databend_storages_common_cache::shared::SharedStore;

    let store = SharedStore::new(
        PoolCaps {
            meta: 0,
            index: 0,
            data: 1 << 20,
        },
        [],
        1,
    );
    let store: std::sync::Arc<dyn databend_storages_common_cache::backend::KindStore> =
        std::sync::Arc::new(store);
    let cache = cache(&store, DATA);
    let live = Live::default();
    assert!(!cache.put("k", live.item(10)).is_charged());
    assert!(!cache.contains("k"));
    assert_eq!(CacheKind::ColumnData.default_limit().max_bytes, Some(0));
}

#[test]
fn test_evicted_value_stays_charged_until_dropped() {
    let store = shared_store(1 << 20, 0, 0);
    let cache = cache(&store, META);
    let live = Live::default();
    let held = cache.put("k", live.item(100));
    assert!(cache.evict("k"));
    let stats = store.kind_stats(CacheKind::TableSnapshot);
    assert_eq!((stats.resident, stats.in_cache, stats.items), (100, 0, 0));
    drop(held);
    assert_eq!(store.kind_stats(CacheKind::TableSnapshot).resident, 0);
    assert_eq!(live.count(), 0);
}

#[test]
fn test_get_sync_computes_once() -> Result<()> {
    let cache = cache(&shared_store(1 << 20, 0, 0), META);
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
