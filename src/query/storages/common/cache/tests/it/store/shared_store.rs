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
use std::sync::Barrier;

use databend_common_base::runtime::Thread;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::KindLimit;
use databend_storages_common_cache::Pool;
use databend_storages_common_cache::cache_manager::CacheManager;
use databend_storages_common_cache::store::KindStore;

use crate::fakes::INDEX;
use crate::fakes::Live;
use crate::fakes::META;
use crate::fakes::META2;
use crate::fakes::cache;
use crate::fakes::shared_store;

fn pool_resident(store: &dyn KindStore, pool: Pool) -> usize {
    store
        .pool_stats()
        .into_iter()
        .find(|s| s.pool == pool)
        .unwrap()
        .resident
}

#[test]
fn test_pools_do_not_evict_each_other() {
    let store = shared_store(1000, 1000, 0);
    let meta = cache(&store, META);
    let index = cache(&store, INDEX);
    let live = Live::default();
    for i in 0..10 {
        meta.put(&i.to_string(), live.item(100));
    }
    for i in 0..100 {
        index.put(&i.to_string(), live.item(100));
    }
    assert!((0..10).all(|i| meta.contains(&i.to_string())));
    assert_eq!(pool_resident(&*store, Pool::Meta), 1000);
    assert_eq!(pool_resident(&*store, Pool::Index), 1000);
}

#[test]
fn test_kinds_share_their_pool() {
    let store = shared_store(1000, 0, 0);
    let a = cache(&store, META);
    let b = cache(&store, META2);
    let live = Live::default();
    for i in 0..10 {
        a.put(&i.to_string(), live.item(100));
    }
    for i in 0..5 {
        b.put(&i.to_string(), live.item(100));
    }
    assert_eq!(pool_resident(&*store, Pool::Meta), 1000);
    let items = |kind| store.kind_stats(kind).items;
    assert_eq!(
        items(CacheKind::SegmentInfo) + items(CacheKind::TableSnapshot),
        10
    );
    assert!(items(CacheKind::SegmentInfo) > 0);
}

#[test]
fn test_oversized_and_pinned_are_rejected() {
    let store = shared_store(1000, 0, 0);
    let cache = cache(&store, META);
    let live = Live::default();
    assert!(!cache.put("big", live.item(251)).is_charged());

    let held: Vec<_> = (0..4)
        .map(|i| cache.put(&i.to_string(), live.item(250)))
        .collect();
    assert!(!cache.put("more", live.item(10)).is_charged());
    drop(held);
    assert!(cache.put("more", live.item(10)).is_charged());
}

#[test]
fn test_kind_limit() {
    let store = shared_store(1000, 0, 0);
    let cache = cache(&store, META);
    let live = Live::default();
    store.set_kind_limit(CacheKind::TableSnapshot, KindLimit::bytes(200));
    cache.put("a", live.item(100));
    cache.put("b", live.item(100));
    assert!(!cache.put("c", live.item(100)).is_charged());

    store.set_kind_limit(CacheKind::TableSnapshot, KindLimit::bytes(100));
    assert_eq!(store.kind_stats(CacheKind::TableSnapshot).in_cache, 100);
}

#[test]
fn test_disable_clears_and_rejects() {
    let store = shared_store(1000, 0, 0);
    let cache = cache(&store, META);
    let live = Live::default();
    cache.put("a", live.item(100));
    let manager = CacheManager::new(store.clone(), Default::default());
    manager.set_capacity_by_name("table_snapshot", 0).unwrap();
    assert!(!cache.contains("a"));
    assert!(!cache.put("b", live.item(100)).is_charged());
    assert_eq!(live.count(), 0);

    manager
        .set_capacity_by_name("memory_cache_table_snapshot", 1000)
        .unwrap();
    assert!(cache.put("b", live.item(100)).is_charged());
}

#[test]
fn test_pool_cap_by_name() {
    let store = shared_store(1000, 0, 0);
    let cache = cache(&store, META);
    let live = Live::default();
    for i in 0..10 {
        cache.put(&i.to_string(), live.item(100));
    }
    let manager = CacheManager::new(store.clone(), Default::default());
    manager.set_capacity_by_name("pool:meta", 500).unwrap();
    assert_eq!(pool_resident(&*store, Pool::Meta), 500);
    manager.set_capacity_by_name("all", 0).unwrap();
    assert_eq!(pool_resident(&*store, Pool::Meta), 0);
    assert!(manager.set_capacity_by_name("all", 1).is_err());
    assert!(manager.set_capacity_by_name("pool:unknown", 1).is_err());
    assert!(manager.set_capacity_by_name("unknown", 1).is_err());
}

/// Concurrent inserts reserve the pool and kind budgets atomically: residency never exceeds
/// either, however the inserts interleave.
#[test]
fn test_concurrent_inserts_stay_within_budgets() {
    const THREADS: usize = 16;
    let store = shared_store(10_000, 0, 0);
    store.set_kind_limit(META, KindLimit::bytes(6_000));
    let barrier = Arc::new(Barrier::new(THREADS));
    let live = Live::default();
    let threads: Vec<_> = (0..THREADS)
        .map(|t| {
            let (store, live, barrier) = (store.clone(), live.clone(), barrier.clone());
            Thread::spawn(move || {
                let cache = cache(&store, META);
                // Every value is held, so eviction frees nothing.
                let mut held = Vec::new();
                barrier.wait();
                for i in 0..200 {
                    held.push(cache.put(&format!("{t}-{i}"), live.item(100)));
                    let kind = store.kind_stats(META);
                    assert!(kind.resident <= 6_000, "{kind:?}");
                    let pool = store.pool_stats()[0];
                    assert!(pool.resident <= 10_000, "{pool:?}");
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(store.kind_stats(META).resident, 0);
}
