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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Instant;

use databend_common_exception::Result;

use super::pool::PoolStore;
use super::pool::PoolStructure;
use super::pool::Removed;
use crate::CacheKind;
use crate::KindLimit;
use crate::Pool;
use crate::PoolCaps;
use crate::Unit;
use crate::backend::InsertMode;
use crate::backend::InsertOutcome;
use crate::backend::KindEvents;
use crate::backend::KindStats;
use crate::backend::KindStore;
use crate::backend::PoolStats;
use crate::backend::ReclaimStats;
use crate::backend::RejectReason;
use crate::backend::ShrinkReport;
use crate::backend::entry::Entry;
use crate::backend::entry::KeyRef;
use crate::handle::cache_ref::ErasedValue;
use crate::handle::cache_ref::Reservation;
use crate::handle::cache_ref::Usage;

const UNLIMITED: usize = usize::MAX;

/// Why entries left the cache.
#[derive(Clone, Copy)]
enum Evicted {
    /// For room in the pool, or because a pool cap or kind limit was lowered.
    Capacity,
    /// Reclaimed for queries under memory pressure.
    MemoryPressure,
}

/// The counts behind `KindEvents`. Plain atomics: reclaim runs inside allocations, possibly
/// while the allocating thread holds a lock, so counting must neither lock nor allocate.
#[derive(Default)]
struct EventCounters {
    evicted: [AtomicU64; 2],
    evicted_bytes: [AtomicU64; 2],
    admission_deferred: AtomicU64,
    rejected_oversized: AtomicU64,
    rejected_kind_limit: AtomicU64,
    rejected_pool_full_pinned: AtomicU64,
}

impl EventCounters {
    fn evicted(&self, reason: Evicted, bytes: usize) {
        let i = reason as usize;
        self.evicted[i].fetch_add(1, Ordering::Relaxed);
        self.evicted_bytes[i].fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn rejected(&self, reason: RejectReason) {
        let counter = match reason {
            // Every fill of a kind that is off; not worth counting.
            RejectReason::Disabled => return,
            RejectReason::Oversized => &self.rejected_oversized,
            RejectReason::KindLimit => &self.rejected_kind_limit,
            RejectReason::PoolFullPinned => &self.rejected_pool_full_pinned,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> KindEvents {
        let get = |c: &AtomicU64| c.load(Ordering::Relaxed);
        let capacity = Evicted::Capacity as usize;
        let pressure = Evicted::MemoryPressure as usize;
        KindEvents {
            evicted_for_capacity: get(&self.evicted[capacity]),
            evicted_for_capacity_bytes: get(&self.evicted_bytes[capacity]),
            evicted_for_pressure: get(&self.evicted[pressure]),
            evicted_for_pressure_bytes: get(&self.evicted_bytes[pressure]),
            admission_deferred: get(&self.admission_deferred),
            rejected_oversized: get(&self.rejected_oversized),
            rejected_kind_limit: get(&self.rejected_kind_limit),
            rejected_pool_full_pinned: get(&self.rejected_pool_full_pinned),
        }
    }
}

#[derive(Default)]
struct ReclaimCounters {
    count: AtomicU64,
    freed_bytes: AtomicU64,
    millis: AtomicU64,
}

struct KindState {
    usage: Arc<Usage>,
    limit: AtomicUsize,
    events: EventCounters,
}

impl KindState {
    fn limit(&self) -> KindLimit {
        match self.limit.load(Ordering::Relaxed) {
            UNLIMITED => KindLimit::UNLIMITED,
            bytes => KindLimit::bytes(bytes),
        }
    }

    fn set_limit(&self, limit: KindLimit) {
        self.limit
            .store(limit.max_bytes.unwrap_or(UNLIMITED), Ordering::Relaxed);
    }
}

/// Three pools with static byte caps; kinds share their pool's eviction order.
pub struct SharedStore {
    pools: [PoolStore; 3],
    kinds: Box<[KindState]>,
    reclaims: ReclaimCounters,
}

impl SharedStore {
    pub fn new(
        caps: PoolCaps,
        kind_limits: impl IntoIterator<Item = (CacheKind, KindLimit)>,
        shards: usize,
    ) -> Self {
        let kinds: Box<[KindState]> = CacheKind::all()
            .map(|kind| {
                let state = KindState {
                    usage: Arc::new(Usage::default()),
                    limit: AtomicUsize::new(UNLIMITED),
                    events: EventCounters::default(),
                };
                state.set_limit(kind.info().shared_limit);
                state
            })
            .collect();
        for (kind, limit) in kind_limits {
            kinds[kind.index()].set_limit(limit);
        }
        SharedStore {
            pools: Pool::ALL.map(|pool| PoolStore::new(pool, caps.get(pool), shards)),
            kinds,
            reclaims: ReclaimCounters::default(),
        }
    }

    fn pool(&self, pool: Pool) -> &PoolStore {
        &self.pools[pool.index()]
    }

    /// Sizes of the pool's internal structures; for tests that check they stay bounded.
    pub fn pool_structure(&self, pool: Pool) -> PoolStructure {
        self.pool(pool).structure()
    }

    fn kind(&self, kind: CacheKind) -> &KindState {
        &self.kinds[kind.index()]
    }

    fn forget(&self, pool: &PoolStore, removed: Removed) -> usize {
        let count = removed.len();
        for (key, entry) in &removed {
            pool.usage().leave(entry.bytes);
            self.kind(key.kind).usage.leave(entry.bytes);
        }
        drop(removed);
        count
    }

    /// `forget`, counting the entries as evicted for `reason`.
    fn forget_evicted(&self, pool: &PoolStore, removed: Removed, reason: Evicted) -> usize {
        for (key, entry) in &removed {
            self.kind(key.kind).events.evicted(reason, entry.bytes);
        }
        self.forget(pool, removed)
    }

    fn reject(&self, kind: CacheKind, reason: RejectReason) -> InsertOutcome {
        self.kind(kind).events.rejected(reason);
        InsertOutcome::Rejected(reason)
    }

    fn evict_until(&self, pool: &PoolStore, cap: usize) {
        // Read once per round: values dropped by readers lower it between two reads.
        while let Some(excess) = pool.usage().resident().checked_sub(cap).filter(|e| *e > 0) {
            let victims = pool.evict_batch(pool.next_cursor(), excess, true);
            if self.forget_evicted(pool, victims, Evicted::Capacity) == 0 {
                break;
            }
        }
    }

    fn check_admissible(
        &self,
        kind: CacheKind,
        bytes: usize,
    ) -> std::result::Result<(), RejectReason> {
        let limit = self.kind(kind).limit();
        let cap = self.pool(kind.pool()).cap();
        if limit.is_disabled() || cap == 0 {
            return Err(RejectReason::Disabled);
        }
        if bytes > cap / 4 {
            return Err(RejectReason::Oversized);
        }
        Ok(())
    }

    /// Reserves `bytes` of the kind's limit; concurrent inserts cannot overshoot it.
    fn reserve_kind(&self, kind: CacheKind, bytes: usize) -> bool {
        let usage = &self.kind(kind).usage;
        match self.kind(kind).limit().max_bytes {
            Some(max) => usage.try_reserve(bytes, max),
            None => usage.try_reserve(bytes, usize::MAX),
        }
    }

    /// Reserves `bytes` of the pool, evicting from `shard` on as long as that frees room.
    fn reserve_pool(&self, pool: &PoolStore, shard: usize, bytes: usize) -> bool {
        loop {
            if pool.usage().try_reserve(bytes, pool.cap()) {
                return true;
            }
            let excess = (pool.usage().resident() + bytes).saturating_sub(pool.cap());
            let victims = pool.evict_batch(shard, excess.max(1), true);
            if self.forget_evicted(pool, victims, Evicted::Capacity) == 0 {
                return false;
            }
        }
    }

    /// Evicts pool by pool, batch by batch, until `relieved` or nothing is left to evict.
    fn reclaim(&self, relieved: &dyn Fn() -> bool) -> ShrinkReport {
        let mut report = ShrinkReport::default();
        for pool in Pool::RECLAIM_ORDER {
            let store = self.pool(pool);
            loop {
                let before = store.usage().resident();
                let victims = store.evict_batch(store.next_cursor(), usize::MAX, false);
                let evicted = self.forget_evicted(store, victims, Evicted::MemoryPressure);
                if evicted == 0 {
                    break;
                }
                report.evicted += evicted;
                report.batches += 1;
                report.freed += before.saturating_sub(store.usage().resident());
                if relieved() {
                    return report;
                }
            }
        }
        report
    }
}

impl KindStore for SharedStore {
    fn is_enabled(&self, kind: CacheKind) -> bool {
        !self.kind(kind).limit().is_disabled() && self.pool(kind.pool()).cap() > 0
    }

    fn get(&self, kind: CacheKind, key: &str) -> Option<ErasedValue> {
        self.pool(kind.pool()).get(KeyRef::new(kind, key))
    }

    fn contains(&self, kind: CacheKind, key: &str) -> bool {
        self.pool(kind.pool()).contains(KeyRef::new(kind, key))
    }

    fn insert(
        &self,
        kind: CacheKind,
        key: &str,
        value: ErasedValue,
        bytes: usize,
        mode: InsertMode,
    ) -> InsertOutcome {
        if let Err(reason) = self.check_admissible(kind, bytes) {
            return self.reject(kind, reason);
        }
        let pool = self.pool(kind.pool());
        let key = KeyRef::new(kind, key);
        let placement = match mode {
            InsertMode::Put => None,
            InsertMode::Fill => match pool.admission(key) {
                Some(placement) => Some(placement),
                None => {
                    let deferred = &self.kind(kind).events.admission_deferred;
                    deferred.fetch_add(1, Ordering::Relaxed);
                    return InsertOutcome::Deferred;
                }
            },
        };

        // Both budgets are reserved before the value goes in, so concurrent inserts cannot
        // together exceed them; a rejected insert gives its reservation back.
        let state = self.kind(kind);
        if !self.reserve_kind(kind, bytes) {
            return self.reject(kind, RejectReason::KindLimit);
        }
        if !self.reserve_pool(pool, pool.shard_of(key), bytes) {
            state.usage.release(bytes);
            return self.reject(kind, RejectReason::PoolFullPinned);
        }
        let reservation = Reservation::reserved(pool.usage().clone(), state.usage.clone(), bytes);
        pool.usage().enter(bytes);
        state.usage.enter(bytes);
        let replaced = pool.insert(key, Entry { value, bytes }, placement);
        self.forget(pool, replaced);
        InsertOutcome::Inserted(reservation)
    }

    fn evict(&self, kind: CacheKind, key: &str) -> bool {
        let pool = self.pool(kind.pool());
        let removed = pool.remove(KeyRef::new(kind, key));
        self.forget(pool, removed) > 0
    }

    fn set_kind_limit(&self, kind: CacheKind, limit: KindLimit) {
        let state = self.kind(kind);
        state.set_limit(limit);
        let pool = self.pool(kind.pool());
        // Read once: concurrent inserts and evictions move it between two reads.
        let in_cache = state.usage.in_cache();
        let excess = match limit.max_bytes {
            Some(0) => None,
            Some(max) if in_cache > max => Some(in_cache - max),
            _ => return,
        };
        let removed = pool.remove_kind(kind, excess);
        self.forget_evicted(pool, removed, Evicted::Capacity);
    }

    fn set_pool_cap(&self, pool: Pool, bytes: usize) -> Result<()> {
        let store = self.pool(pool);
        store.set_cap(bytes);
        if bytes == 0 {
            let removed = store.remove_all();
            self.forget_evicted(store, removed, Evicted::Capacity);
        } else {
            self.evict_until(store, bytes);
        }
        Ok(())
    }

    fn kind_stats(&self, kind: CacheKind) -> KindStats {
        let state = self.kind(kind);
        KindStats {
            kind,
            items: state.usage.items(),
            resident: state.usage.resident(),
            in_cache: state.usage.in_cache(),
            capacity: state.limit().max_bytes,
            unit: Unit::Bytes,
            events: Some(state.events.snapshot()),
        }
    }

    fn pool_stats(&self) -> Vec<PoolStats> {
        self.pools
            .iter()
            .map(|p| PoolStats {
                pool: p.pool(),
                cap: p.cap(),
                items: p.usage().items(),
                resident: p.usage().resident(),
                in_cache: p.usage().in_cache(),
            })
            .collect()
    }

    fn shrink_for_pressure(&self, relieved: &dyn Fn() -> bool) -> ShrinkReport {
        let started = Instant::now();
        let report = self.reclaim(relieved);
        let reclaims = &self.reclaims;
        reclaims.count.fetch_add(1, Ordering::Relaxed);
        reclaims
            .freed_bytes
            .fetch_add(report.freed as u64, Ordering::Relaxed);
        reclaims
            .millis
            .fetch_add(started.elapsed().as_millis() as u64, Ordering::Relaxed);
        report
    }

    fn reclaim_stats(&self) -> Option<ReclaimStats> {
        let get = |c: &AtomicU64| c.load(Ordering::Relaxed);
        Some(ReclaimStats {
            count: get(&self.reclaims.count),
            freed_bytes: get(&self.reclaims.freed_bytes),
            millis: get(&self.reclaims.millis),
        })
    }
}
