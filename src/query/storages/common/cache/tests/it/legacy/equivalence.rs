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

//! `LegacyStore` must behave exactly like the per-kind `InMemoryLruCache` it replaced. Random
//! operation sequences run against both, and every step compares what each one returns and
//! which keys it holds.

use std::sync::Arc;

use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CacheRef;
use databend_storages_common_cache::CacheValue;
use databend_storages_common_cache::InMemoryLruCache;
use databend_storages_common_cache::KindLimit;
use databend_storages_common_cache::backend::KindStore;
use databend_storages_common_cache::kinds::bloom;
use databend_storages_common_cache::kinds::segment;
use databend_storages_common_cache::kinds::table;
use databend_storages_common_cache::legacy::LegacyCapacity;
use databend_storages_common_cache::legacy::LegacyStore;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::SmallRng;

use crate::fakes::Item;
use crate::fakes::Live;
use crate::fakes::cache;

const KEYS: usize = 64;

/// Value cached by the old cache; `id` identifies which insertion it came from.
struct Old {
    id: usize,
    bytes: usize,
}

impl From<Old> for CacheValue<Old> {
    fn from(value: Old) -> Self {
        let bytes = value.bytes;
        CacheValue::new(value, bytes)
    }
}

#[derive(Clone, Copy, Debug)]
enum Unit {
    Items,
    Bytes,
}

fn old_cache(unit: Unit, capacity: usize) -> InMemoryLruCache<Old> {
    match unit {
        Unit::Items => InMemoryLruCache::with_items_capacity("old".to_string(), capacity),
        Unit::Bytes => InMemoryLruCache::with_bytes_capacity("old".to_string(), capacity),
    }
}

fn new_store(kind: CacheKind, unit: Unit, capacity: usize) -> Arc<dyn KindStore> {
    let capacity = match unit {
        Unit::Items => LegacyCapacity::Items(capacity),
        Unit::Bytes => LegacyCapacity::Bytes(capacity),
    };
    Arc::new(LegacyStore::new([(kind, capacity)]))
}

fn run(kind: CacheKind, unit: Unit, seed: u64) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut capacity = rng.gen_range(
        1..=match unit {
            Unit::Items => KEYS,
            Unit::Bytes => 4_000,
        },
    );
    let old = old_cache(unit, capacity);
    let store = new_store(kind, unit, capacity);
    let new = cache(&store, kind);
    let live = Live::default();
    // `ids[k]` is the step that last put key `k`: the new cache's value for `k` is that one,
    // since its values are only told apart by size.
    let mut ids = vec![None; KEYS];

    for step in 0..3_000 {
        let key = rng.gen_range(0..KEYS);
        let name = key.to_string();
        let ctx = format!("{kind:?} {unit:?} seed {seed} step {step} key {key} cap {capacity}");
        match rng.gen_range(0..10) {
            0..=3 => {
                // Mostly within the capacity, sometimes larger than all of it.
                let bytes = if rng.gen_bool(0.05) {
                    rng.gen_range(1..=capacity * 2)
                } else {
                    rng.gen_range(1..=capacity.min(400))
                };
                old.insert(name.clone(), Old { id: step, bytes });
                let charged = new.put(&name, live.item(bytes)).is_charged();
                ids[key] = Some(step);
                assert_eq!(charged, old.contains_key(&name), "{ctx}: put cached");
            }
            4..=6 => {
                let old_value = old.get(&name).map(|v| (v.bytes, Some(v.id)));
                let new_value = new
                    .lookup(&name)
                    .map(|v: CacheRef<Item>| (v.bytes, ids[key]));
                assert_eq!(new_value, old_value, "{ctx}: lookup");
            }
            7 => {
                assert_eq!(new.evict(&name), old.evict(&name), "{ctx}: evict");
            }
            8 => {
                assert_eq!(
                    new.contains(&name),
                    old.contains_key(&name),
                    "{ctx}: contains"
                );
            }
            _ => {
                capacity = rng.gen_range(1..=capacity * 2);
                match unit {
                    Unit::Items => old.set_items_capacity(capacity),
                    Unit::Bytes => old.set_bytes_capacity(capacity),
                }
                store.set_kind_limit(kind, KindLimit::bytes(capacity));
            }
        }

        let stats = store.kind_stats(kind);
        assert_eq!(stats.items, old.len(), "{ctx}: len");
        if let Unit::Bytes = unit {
            assert_eq!(stats.in_cache as u64, old.bytes_size(), "{ctx}: bytes");
        }
        // Peeking every key compares the full contents without touching recency.
        for k in 0..KEYS {
            let name = k.to_string();
            assert_eq!(
                new.contains(&name),
                old.contains_key(&name),
                "{ctx}: holds key {k}"
            );
        }
    }
    // Values not cached are released.
    drop(new);
    assert_eq!(live.count(), store.kind_stats(kind).items);
}

#[test]
fn test_items_capacity_matches_lru_cache() {
    for seed in 0..20 {
        run(CacheKind::of::<table::Snapshot>(), Unit::Items, seed);
        run(CacheKind::of::<bloom::IndexMeta>(), Unit::Items, seed);
    }
}

#[test]
fn test_bytes_capacity_matches_lru_cache() {
    for seed in 0..20 {
        run(CacheKind::of::<segment::Info>(), Unit::Bytes, seed);
        run(CacheKind::of::<bloom::Filter>(), Unit::Bytes, seed);
    }
}
