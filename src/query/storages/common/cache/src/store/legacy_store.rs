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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use parking_lot::Mutex;

use crate::Unit;
use crate::cache_kind::CacheKind;
use crate::cache_kind::Pool;
use crate::cache_ref::ErasedValue;
use crate::cache_ref::Reservation;
use crate::cache_ref::Usage;
use crate::pool_caps::KindLimit;
use crate::store::entry::Entry;
use crate::store::kind_store::InsertMode;
use crate::store::kind_store::InsertOutcome;
use crate::store::kind_store::KindStats;
use crate::store::kind_store::KindStore;
use crate::store::kind_store::PoolStats;
use crate::store::kind_store::RejectReason;
use crate::store::kind_store::ShrinkReport;
use crate::store::legacy_capacities::legacy_unit;

/// Kinds released under memory pressure: the ones the pre-refactor `release_cache_memory(Basic)` cleared.
const RECLAIMABLE: [CacheKind; 7] = [
    CacheKind::ColumnArray,
    CacheKind::SegmentBlockMetas,
    CacheKind::ColumnData,
    CacheKind::InvertedIndexPayload,
    CacheKind::InvertedIndexFullPayload,
    CacheKind::VectorIndex,
    CacheKind::VectorRefine,
];

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

struct LegacyKind {
    lru: Mutex<LruCache<String, Entry>>,
    capacity: Mutex<LegacyCapacity>,
    usage: Arc<Usage>,
}

/// One LRU per kind with the pre-refactor capacity rules. Only `VectorIndex` counts values
/// still held by readers against its capacity, as before.
pub struct LegacyStore {
    kinds: Box<[LegacyKind]>,
    pools: [Arc<Usage>; 3],
}

impl LegacyStore {
    pub fn new(capacities: impl IntoIterator<Item = (CacheKind, LegacyCapacity)>) -> Self {
        let kinds: Box<[LegacyKind]> = CacheKind::ALL
            .iter()
            .map(|_| LegacyKind {
                lru: Mutex::new(LruCache::with_bytes_capacity(usize::MAX)),
                capacity: Mutex::new(LegacyCapacity::Disabled),
                usage: Arc::new(Usage::default()),
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

    fn kind(&self, kind: CacheKind) -> &LegacyKind {
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

    fn make_room(
        &self,
        kind: CacheKind,
        lru: &mut LruCache<String, Entry>,
        capacity: LegacyCapacity,
        key: &str,
        bytes: usize,
    ) -> (Vec<Entry>, std::result::Result<(), RejectReason>) {
        let mut victims = Vec::new();
        if kind == CacheKind::VectorIndex {
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
            let (victims, room) = self.make_room(kind, &mut lru, capacity, key, bytes);
            match room {
                Err(reason) => (victims, Err(reason)),
                Ok(()) => {
                    // Charged under the lock: `VectorIndex` checks `resident` here, so
                    // concurrent inserts cannot both pass the check before either charges.
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

    /// The bound is interpreted in the kind's legacy unit (bytes or items).
    fn set_kind_limit(&self, kind: CacheKind, limit: KindLimit) {
        let state = self.kind(kind);
        let capacity = {
            let mut capacity = state.capacity.lock();
            *capacity = match (limit.max_bytes, legacy_unit(kind)) {
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
            LegacyCapacity::Disabled => (Some(0), legacy_unit(kind)),
        };
        KindStats {
            kind,
            items: state.usage.items(),
            resident: state.usage.resident(),
            in_cache: state.usage.in_cache(),
            capacity,
            unit,
        }
    }

    fn pool_stats(&self) -> Vec<PoolStats> {
        Vec::new()
    }

    fn shrink_for_pressure(&self, relieved: &dyn Fn() -> bool) -> ShrinkReport {
        let mut report = ShrinkReport::default();
        for kind in RECLAIMABLE {
            let before = self.kind(kind).usage.resident();
            let removed = {
                let Some(mut lru) = self.kind(kind).lru.try_lock() else {
                    continue;
                };
                let keys: Vec<String> = lru
                    .iter()
                    .filter(|(_, e)| !e.is_shared())
                    .map(|(k, _)| k.clone())
                    .collect();
                keys.iter().filter_map(|k| lru.pop(k)).collect::<Vec<_>>()
            };
            let evicted = self.forget(kind, removed);
            if evicted == 0 {
                continue;
            }
            report.evicted += evicted;
            report.batches += 1;
            report.freed += before.saturating_sub(self.kind(kind).usage.resident());
            if relieved() {
                break;
            }
        }
        report
    }
}
