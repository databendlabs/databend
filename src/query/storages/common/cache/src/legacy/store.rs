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

use databend_common_cache::Cache;
use databend_common_cache::LruCache;
use databend_common_config::CacheConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::warn;
use parking_lot::Mutex;

use crate::CacheKind;
use crate::KindLimit;
use crate::Pool;
use crate::Unit;
use crate::backend::InsertMode;
use crate::backend::InsertOutcome;
use crate::backend::KindStats;
use crate::backend::KindStore;
use crate::backend::PoolStats;
use crate::backend::RejectReason;
use crate::backend::ShrinkReport;
use crate::backend::entry::Entry;
use crate::handle::cache_ref::ErasedValue;
use crate::handle::cache_ref::Reservation;
use crate::handle::cache_ref::Usage;
use crate::legacy::LegacyKind;
use crate::legacy::LegacyKinds;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LegacyCapacity {
    Bytes(usize),
    Items(usize),
    Disabled,
}

impl LegacyCapacity {
    fn exceeded(
        &self,
        lru: &LruCache<String, Entry>,
        extra_items: usize,
        extra_bytes: usize,
    ) -> bool {
        match *self {
            LegacyCapacity::Bytes(max) => lru.bytes_size() as usize + extra_bytes > max,
            LegacyCapacity::Items(max) => lru.len() + extra_items > max,
            LegacyCapacity::Disabled => true,
        }
    }
}

struct KindState {
    lru: Mutex<LruCache<String, Entry>>,
    capacity: Mutex<LegacyCapacity>,
    usage: Arc<Usage>,
    /// `None` for kinds the legacy backend does not support; they stay disabled.
    legacy: Option<&'static LegacyKind>,
}

impl KindState {
    fn unit(&self) -> Unit {
        self.legacy.map_or(Unit::Bytes, |legacy| legacy.unit)
    }

    fn counts_held_values(&self) -> bool {
        self.legacy.is_some_and(|legacy| legacy.counts_held_values)
    }
}

/// One LRU per kind with the pre-refactor capacity rules of `LegacyKinds`.
pub struct LegacyStore {
    kinds: Box<[KindState]>,
    pools: [Arc<Usage>; 3],
}

impl LegacyStore {
    /// Sized by `LegacyKinds`, warning when the byte capacities exceed the memory limit.
    pub fn from_config(config: &CacheConfig, max_server_memory_usage: u64) -> Self {
        let capacities = LegacyKinds::global().capacities(config, max_server_memory_usage);
        let total: u128 = capacities
            .iter()
            .map(|(_, capacity)| match capacity {
                LegacyCapacity::Bytes(n) => *n as u128,
                _ => 0,
            })
            .sum();
        if max_server_memory_usage > 0 && total > max_server_memory_usage as u128 {
            warn!(
                "[CacheManager] Cache capacities sum to {total} bytes, more than max_server_memory_usage {max_server_memory_usage}"
            );
        }
        LegacyStore::new(capacities)
    }

    pub fn new(capacities: impl IntoIterator<Item = (CacheKind, LegacyCapacity)>) -> Self {
        let kinds: Box<[KindState]> = CacheKind::all()
            .map(|kind| KindState {
                lru: Mutex::new(LruCache::with_bytes_capacity(usize::MAX)),
                capacity: Mutex::new(LegacyCapacity::Disabled),
                usage: Arc::new(Usage::default()),
                legacy: LegacyKinds::global().get(kind),
            })
            .collect();
        for (kind, capacity) in capacities {
            *kinds[kind.index()].capacity.lock() = capacity;
        }
        LegacyStore {
            kinds,
            pools: Pool::ALL.map(|_| Arc::new(Usage::default())),
        }
    }

    fn kind(&self, kind: CacheKind) -> &KindState {
        &self.kinds[kind.index()]
    }

    fn forget(&self, kind: CacheKind, removed: Vec<Entry>) -> usize {
        let count = removed.len();
        for entry in &removed {
            self.pools[kind.pool().index()].leave(entry.bytes);
            self.kind(kind).usage.leave(entry.bytes);
        }
        drop(removed);
        count
    }

    fn set_limit(&self, kind: CacheKind, limit: KindLimit) {
        let state = self.kind(kind);
        let capacity = {
            let mut capacity = state.capacity.lock();
            *capacity = match (limit.max_bytes, state.unit()) {
                (Some(0), _) => LegacyCapacity::Disabled,
                (Some(n), Unit::Count) => LegacyCapacity::Items(n),
                (Some(n), Unit::Bytes) => LegacyCapacity::Bytes(n),
                (None, _) => LegacyCapacity::Bytes(usize::MAX),
            };
            *capacity
        };
        let removed: Vec<_> = {
            let mut lru = state.lru.lock();
            let mut removed = Vec::new();
            while capacity.exceeded(&lru, 0, 0) {
                match lru.pop_by_policy() {
                    Some((_, entry)) => removed.push(entry),
                    None => break,
                }
            }
            removed
        };
        self.forget(kind, removed);
    }

    fn make_room(
        &self,
        kind: CacheKind,
        lru: &mut LruCache<String, Entry>,
        capacity: LegacyCapacity,
        key: &str,
        bytes: usize,
    ) -> (Vec<Entry>, std::result::Result<(), RejectReason>) {
        let mut victims = Vec::new();
        if self.kind(kind).counts_held_values() {
            let LegacyCapacity::Bytes(max) = capacity else {
                return (victims, Err(RejectReason::Disabled));
            };
            if bytes > max {
                return (victims, Err(RejectReason::Oversized));
            }
            let mut freed = 0;
            while self.kind(kind).usage.resident() - freed + bytes > max {
                let victim = lru
                    .iter()
                    .find(|(_, e)| !e.is_shared())
                    .map(|(k, _)| k.clone());
                // Victims already removed are returned with the rejection, so the caller
                // still accounts for them.
                let Some(entry) = victim.and_then(|k| lru.pop(&k)) else {
                    return (victims, Err(RejectReason::PoolFullPinned));
                };
                freed += entry.bytes;
                victims.push(entry);
            }
            return (victims, Ok(()));
        }

        if matches!(capacity, LegacyCapacity::Bytes(max) if bytes > max) {
            // As the LRU cache always did: the value is inserted and then evicted together
            // with every other entry, so the kind ends up empty and the value is not cached.
            while let Some((_, entry)) = lru.pop_by_policy() {
                victims.push(entry);
            }
            return (victims, Err(RejectReason::Oversized));
        }
        // The value being replaced makes room like any other; it is replaced either way.
        if let Some(old) = lru.pop(key) {
            victims.push(old);
        }
        // Fits once the LRU is empty: bytes are within the capacity, and an item capacity
        // is at least 1 (0 is `Disabled`).
        while capacity.exceeded(lru, 1, bytes) {
            match lru.pop_by_policy() {
                Some((_, entry)) => victims.push(entry),
                None => break,
            }
        }
        (victims, Ok(()))
    }
}

impl KindStore for LegacyStore {
    fn is_enabled(&self, kind: CacheKind) -> bool {
        *self.kind(kind).capacity.lock() != LegacyCapacity::Disabled
    }

    fn get(&self, kind: CacheKind, key: &str) -> Option<ErasedValue> {
        self.kind(kind).lru.lock().get(key).map(|e| e.value.clone())
    }

    fn contains(&self, kind: CacheKind, key: &str) -> bool {
        self.kind(kind).lru.lock().contains(key)
    }

    fn insert(
        &self,
        kind: CacheKind,
        key: &str,
        value: ErasedValue,
        bytes: usize,
        _mode: InsertMode,
    ) -> InsertOutcome {
        let state = self.kind(kind);
        let capacity = *state.capacity.lock();
        if capacity == LegacyCapacity::Disabled {
            return InsertOutcome::Rejected(RejectReason::Disabled);
        }
        let pool = &self.pools[kind.pool().index()];
        let (victims, outcome) = {
            let mut lru = state.lru.lock();
            let (mut victims, room) = self.make_room(kind, &mut lru, capacity, key, bytes);
            match room {
                Err(reason) => (victims, Err(reason)),
                Ok(()) => {
                    // Charged under the lock: kinds counting held values check `resident` here, so
                    // concurrent inserts cannot both pass the check before either charges.
                    // Its victims are unreferenced, so they are released first: `resident`
                    // then never exceeds the capacity, not even briefly.
                    if state.counts_held_values() {
                        self.forget(kind, std::mem::take(&mut victims));
                    }
                    let reservation = Reservation::charge(pool.clone(), state.usage.clone(), bytes);
                    let replaced = lru.insert(key.to_string(), Entry { value, bytes });
                    pool.enter(bytes);
                    state.usage.enter(bytes);
                    (victims, Ok((reservation, replaced)))
                }
            }
        };
        self.forget(kind, victims);
        match outcome {
            Err(reason) => InsertOutcome::Rejected(reason),
            Ok((reservation, replaced)) => {
                self.forget(kind, replaced.into_iter().collect());
                InsertOutcome::Inserted(reservation)
            }
        }
    }

    fn evict(&self, kind: CacheKind, key: &str) -> bool {
        let removed = self.kind(kind).lru.lock().pop(key);
        self.forget(kind, removed.into_iter().collect()) > 0
    }

    /// The bound is interpreted in the kind's legacy unit (bytes or items). A kind sharing its
    /// budget with another resizes both.
    fn set_kind_limit(&self, kind: CacheKind, limit: KindLimit) {
        let partner = self.kind(kind).legacy.and_then(|l| l.shares_budget_with);
        match (partner, limit.max_bytes) {
            (Some(partner), Some(total)) => {
                let (own, other) = LegacyKind::split_budget(total);
                self.set_limit(kind, KindLimit::bytes(own));
                self.set_limit(partner, KindLimit::bytes(other));
            }
            _ => self.set_limit(kind, limit),
        }
    }

    fn set_pool_cap(&self, _pool: Pool, _bytes: usize) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "pool capacity requires the shared cache backend",
        ))
    }

    fn kind_stats(&self, kind: CacheKind) -> KindStats {
        let state = self.kind(kind);
        let (capacity, unit) = match *state.capacity.lock() {
            LegacyCapacity::Bytes(n) => (Some(n), Unit::Bytes),
            LegacyCapacity::Items(n) => (Some(n), Unit::Count),
            LegacyCapacity::Disabled => (Some(0), state.unit()),
        };
        KindStats {
            kind,
            items: state.usage.items(),
            resident: state.usage.resident(),
            in_cache: state.usage.in_cache(),
            capacity,
            unit,
            events: None,
        }
    }

    fn pool_stats(&self) -> Vec<PoolStats> {
        Vec::new()
    }

    /// Never reclaims: the pre-refactor caches were not shrunk under memory pressure, and the
    /// memory manager gets a reclaim handle only with the shared backend.
    fn shrink_for_pressure(&self, _relieved: &dyn Fn() -> bool) -> ShrinkReport {
        ShrinkReport::default()
    }
}
