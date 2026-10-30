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

use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::KindLimit;
use databend_storages_common_cache::Pool;
use databend_storages_common_cache::Unit;
use databend_storages_common_cache::store::KindStore;
use databend_storages_common_cache::store::LegacyCapacity;
use databend_storages_common_cache::store::LegacyStore;

use crate::fakes::Live;
use crate::fakes::META;
use crate::fakes::cache;

fn legacy(capacities: &[(CacheKind, LegacyCapacity)]) -> Arc<dyn KindStore> {
    Arc::new(LegacyStore::new(capacities.iter().copied()))
}

#[test]
fn test_items_capacity_evicts_lru() {
    let store = legacy(&[(CacheKind::TableSnapshot, LegacyCapacity::Items(2))]);
    let cache = cache(&store, META);
    let live = Live::default();
    cache.put("a", live.item(1000));
    cache.put("b", live.item(1000));
    cache.lookup("a");
    cache.put("c", live.item(1000));
    assert!(cache.contains("a"));
    assert!(!cache.contains("b"));
    assert!(cache.contains("c"));
    let stats = store.kind_stats(CacheKind::TableSnapshot);
    assert_eq!(
        (stats.items, stats.capacity, stats.unit),
        (2, Some(2), Unit::Count)
    );
}

#[test]
fn test_bytes_capacity_evicts_even_held_values() {
    let store = legacy(&[(CacheKind::BloomIndexMeta, LegacyCapacity::Bytes(200))]);
    let cache = cache(&store, CacheKind::BloomIndexMeta);
    let live = Live::default();
    let held = cache.put("a", live.item(100));
    cache.put("b", live.item(100));
    cache.put("c", live.item(100));
    assert!(!cache.contains("a"));
    let stats = store.kind_stats(CacheKind::BloomIndexMeta);
    assert_eq!((stats.in_cache, stats.resident), (200, 300));
    drop(held);
    assert_eq!(store.kind_stats(CacheKind::BloomIndexMeta).resident, 200);
}

#[test]
fn test_vector_index_keeps_held_values() {
    let store = legacy(&[(CacheKind::VectorIndex, LegacyCapacity::Bytes(200))]);
    let cache = cache(&store, CacheKind::VectorIndex);
    let live = Live::default();
    let held = cache.put("a", live.item(100));
    cache.put("b", live.item(100));
    cache.put("c", live.item(100));
    assert!(cache.contains("a"));
    assert!(!cache.contains("b"));
    let _held_b = cache.put("b", live.item(100));
    assert!(!cache.put("d", live.item(100)).is_charged());
    drop(held);
    assert!(cache.put("d", live.item(100)).is_charged());
}

/// Ported from the removed `VectorIndexCache`: an oversized index is returned uncached without
/// evicting hot ones, and an evicted index stays charged until its last reader drops it.
#[test]
fn test_vector_index_oversized_and_evicted_while_held() {
    let store = legacy(&[(CacheKind::VectorIndex, LegacyCapacity::Bytes(200))]);
    let cache = cache(&store, CacheKind::VectorIndex);
    let live = Live::default();
    drop(cache.put("hot", live.item(100)));
    let large = cache.put("large", live.item(300));
    assert!(!large.is_charged());
    assert!(cache.contains("hot"));
    assert!(!cache.contains("large"));
    drop(large);
    assert_eq!(store.kind_stats(CacheKind::VectorIndex).resident, 100);

    let reader = cache.lookup("hot").unwrap();
    let another_reader = reader.clone();
    assert!(cache.evict("hot"));
    let stats = store.kind_stats(CacheKind::VectorIndex);
    assert_eq!((stats.items, stats.resident), (0, 100));
    drop(reader);
    assert_eq!(store.kind_stats(CacheKind::VectorIndex).resident, 100);
    drop(another_reader);
    assert_eq!(store.kind_stats(CacheKind::VectorIndex).resident, 0);
}

#[test]
fn test_unconfigured_kind_is_disabled() {
    let store = legacy(&[]);
    let cache = cache(&store, META);
    let live = Live::default();
    assert!(!cache.put("a", live.item(1)).is_charged());
    assert_eq!(store.kind_stats(CacheKind::TableSnapshot).capacity, Some(0));
}

#[test]
fn test_set_kind_limit_keeps_unit() {
    let store = legacy(&[(CacheKind::TableSnapshot, LegacyCapacity::Items(10))]);
    let cache = cache(&store, META);
    let live = Live::default();
    for i in 0..10 {
        cache.put(&i.to_string(), live.item(1));
    }
    store.set_kind_limit(CacheKind::TableSnapshot, KindLimit::bytes(3));
    let stats = store.kind_stats(CacheKind::TableSnapshot);
    assert_eq!((stats.items, stats.unit), (3, Unit::Count));
    store.set_kind_limit(CacheKind::TableSnapshot, KindLimit::DISABLED);
    let stats = store.kind_stats(CacheKind::TableSnapshot);
    assert_eq!((stats.items, stats.unit), (0, Unit::Count));
    assert!(store.set_pool_cap(Pool::Meta, 1).is_err());

    // Re-enabling a kind that started disabled keeps its legacy unit.
    store.set_kind_limit(CacheKind::TableSnapshot, KindLimit::bytes(2));
    for i in 0..5 {
        cache.put(&i.to_string(), live.item(100));
    }
    let stats = store.kind_stats(CacheKind::TableSnapshot);
    assert_eq!((stats.items, stats.capacity), (2, Some(2)));
}

#[test]
fn test_shrink_only_reclaimable_kinds() {
    let store = legacy(&[
        (CacheKind::TableSnapshot, LegacyCapacity::Items(10)),
        (CacheKind::VectorIndex, LegacyCapacity::Bytes(1000)),
    ]);
    let meta = cache(&store, META);
    let vector = cache(&store, CacheKind::VectorIndex);
    let live = Live::default();
    meta.put("m", live.item(100));
    vector.put("v", live.item(100));
    let report = store.shrink_for_pressure(&|| false);
    assert_eq!(report.freed, 100);
    assert!(meta.contains("m"));
    assert!(!vector.contains("v"));
}
