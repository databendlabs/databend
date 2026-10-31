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

//! A kind at its byte limit turns over like a pool at its cap: its own coldest entries make room,
//! instead of the kind refusing every new value and keeping what it first cached.

use std::sync::Arc;
use std::sync::Barrier;

use databend_common_base::runtime::Thread;
use databend_storages_common_cache::KindLimit;
use databend_storages_common_cache::PoolCaps;
use databend_storages_common_cache::backend::KindStore;
use databend_storages_common_cache::shared::SharedStore;
use futures::executor::block_on;

use crate::fakes::FakeReader;
use crate::fakes::Live;
use crate::fakes::cache;
use crate::fakes::data_kind;
use crate::fakes::meta_kind;
use crate::fakes::meta2_kind;
use crate::fakes::shared_store;

/// One shard, so that eviction order is the order of the whole pool.
fn one_shard_store(meta: usize) -> Arc<dyn KindStore> {
    Arc::new(SharedStore::new(
        PoolCaps {
            meta,
            index: 0,
            data: 0,
        },
        [],
        1,
    ))
}

fn keys(range: std::ops::Range<u32>) -> Vec<String> {
    range.map(|i| i.to_string()).collect()
}

/// Twenty values through a kind that holds ten: all are cached in turn, the oldest leave.
#[test]
fn test_full_kind_turns_over() {
    let store = one_shard_store(10_000);
    store.set_kind_limit(meta_kind(), KindLimit::bytes(1000));
    let cache = cache(&store, meta_kind());
    let live = Live::default();
    for key in keys(0..20) {
        assert!(cache.put(&key, live.item(100)).is_charged(), "{key}");
    }
    let cached: Vec<_> = keys(0..20)
        .into_iter()
        .filter(|k| cache.contains(k))
        .collect();
    assert_eq!(cached, keys(10..20));
    let stats = store.kind_stats(meta_kind());
    assert_eq!((stats.items, stats.in_cache), (10, 1000));
    let events = stats.events.unwrap();
    assert_eq!(events.evicted_for_capacity, 10);
    assert_eq!(events.rejected_kind_limit, 0);
}

/// Turning over keeps S3-FIFO order: an entry read again survives newer one-time entries.
#[test]
fn test_hot_entry_survives_turnover() {
    let store = one_shard_store(10_000);
    store.set_kind_limit(meta_kind(), KindLimit::bytes(1000));
    let cache = cache(&store, meta_kind());
    let live = Live::default();
    for key in keys(0..10) {
        cache.put(&key, live.item(100));
    }
    assert!(cache.lookup("0").is_some());
    for key in keys(10..15) {
        cache.put(&key, live.item(100));
    }
    assert!(cache.contains("0"));
    assert!(keys(1..6).iter().all(|k| !cache.contains(k)));
}

/// Only the full kind gives up entries; the other kind of its pool keeps all of its own.
#[test]
fn test_turnover_spares_other_kinds() {
    let store = shared_store(10_000, 0, 0);
    store.set_kind_limit(meta_kind(), KindLimit::bytes(500));
    let (full, other) = (cache(&store, meta_kind()), cache(&store, meta2_kind()));
    let live = Live::default();
    for key in keys(0..10) {
        other.put(&key, live.item(100));
    }
    for key in keys(0..20) {
        full.put(&key, live.item(100));
    }
    assert!(keys(0..10).iter().all(|k| other.contains(k)));
    let events = store.kind_stats(meta2_kind()).events.unwrap();
    assert_eq!(events.evicted_for_capacity, 0);
    assert_eq!(store.kind_stats(meta_kind()).in_cache, 500);
}

/// A kind full of values held by queries cannot make room: new values are refused, and taken
/// again once the held ones are released.
#[test]
fn test_full_kind_of_held_values_refuses() {
    let store = shared_store(10_000, 0, 0);
    store.set_kind_limit(meta_kind(), KindLimit::bytes(1000));
    let cache = cache(&store, meta_kind());
    let live = Live::default();
    let held: Vec<_> = keys(0..10)
        .iter()
        .map(|k| cache.put(k, live.item(100)))
        .collect();
    assert!(!cache.put("new", live.item(100)).is_charged());
    let events = store.kind_stats(meta_kind()).events.unwrap();
    assert_eq!(events.rejected_kind_limit, 1);
    drop(held);
    assert!(cache.put("new", live.item(100)).is_charged());
}

/// A value larger than the whole limit is refused without emptying the kind for it.
#[test]
fn test_value_larger_than_the_limit_evicts_nothing() {
    let store = shared_store(10_000, 0, 0);
    store.set_kind_limit(meta_kind(), KindLimit::bytes(1000));
    let cache = cache(&store, meta_kind());
    let live = Live::default();
    for key in keys(0..5) {
        cache.put(&key, live.item(100));
    }
    assert!(!cache.put("huge", live.item(1001)).is_charged());
    assert!(keys(0..5).iter().all(|k| cache.contains(k)));
}

/// Lowering the limit evicts the coldest entries first, as turning over does.
#[test]
fn test_lowering_the_limit_keeps_hot_entries() {
    let store = one_shard_store(10_000);
    let cache = cache(&store, meta_kind());
    let live = Live::default();
    for key in keys(0..10) {
        cache.put(&key, live.item(100));
    }
    assert!(cache.lookup("8").is_some());
    assert!(cache.lookup("9").is_some());
    store.set_kind_limit(meta_kind(), KindLimit::bytes(500));
    assert_eq!(store.kind_stats(meta_kind()).in_cache, 500);
    assert!(cache.contains("8") && cache.contains("9"));
    assert!(keys(0..5).iter().all(|k| !cache.contains(k)));
}

/// Values loaded through a reader, with the data pool's admission, also turn over.
#[test]
fn test_loaded_values_turn_over() {
    let store = shared_store(0, 0, 100_000);
    store.set_kind_limit(data_kind(), KindLimit::bytes(1000));
    let cache = cache(&store, data_kind());
    let reader = FakeReader::new(100);
    for key in keys(0..20) {
        // The second load of a key is admitted.
        block_on(cache.get(&reader, key.clone())).unwrap();
        block_on(cache.get(&reader, key.clone())).unwrap();
        assert!(cache.contains(&key), "{key}");
    }
    let events = store.kind_stats(data_kind()).events.unwrap();
    assert_eq!(events.rejected_kind_limit, 0);
    assert_eq!(store.kind_stats(data_kind()).in_cache, 1000);
}

/// Many threads filling a limited kind: it turns over without passing its limit, nothing is
/// refused, and the counts agree once they are done.
#[test]
fn test_concurrent_turnover_stays_within_the_limit() {
    const THREADS: usize = 16;
    let store = shared_store(100_000, 0, 0);
    store.set_kind_limit(meta_kind(), KindLimit::bytes(5_000));
    let barrier = Arc::new(Barrier::new(THREADS));
    let live = Live::default();
    let threads: Vec<_> = (0..THREADS)
        .map(|t| {
            let (store, live, barrier) = (store.clone(), live.clone(), barrier.clone());
            Thread::spawn(move || {
                let cache = cache(&store, meta_kind());
                barrier.wait();
                for i in 0..500 {
                    assert!(cache.put(&format!("{t}-{i}"), live.item(100)).is_charged());
                    let kind = store.kind_stats(meta_kind());
                    assert!(kind.resident <= 5_000, "{kind:?}");
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let kind = store.kind_stats(meta_kind());
    assert_eq!((kind.resident, kind.in_cache), (5_000, 5_000));
    assert_eq!(live.count(), kind.items);
    let events = kind.events.unwrap();
    assert_eq!(events.rejected_kind_limit, 0);
    assert_eq!(events.evicted_for_capacity, (THREADS * 500 - 50) as u64);
}
