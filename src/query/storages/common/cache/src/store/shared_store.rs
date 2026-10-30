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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_common_exception::Result;

use crate::Unit;
use crate::cache_kind::CacheKind;
use crate::cache_kind::Pool;
use crate::cache_ref::ErasedValue;
use crate::cache_ref::Reservation;
use crate::cache_ref::Usage;
use crate::pool_caps::KindLimit;
use crate::pool_caps::PoolCaps;
use crate::store::entry::Entry;
use crate::store::entry::KeyRef;
use crate::store::kind_store::InsertMode;
use crate::store::kind_store::InsertOutcome;
use crate::store::kind_store::KindStats;
use crate::store::kind_store::KindStore;
use crate::store::kind_store::PoolStats;
use crate::store::kind_store::RejectReason;
use crate::store::kind_store::ShrinkReport;
use crate::store::pool_store::PoolStore;
use crate::store::pool_store::Removed;

const UNLIMITED: usize = usize::MAX;

struct KindState {
    usage: Arc<Usage>,
    limit: AtomicUsize,
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
}

impl SharedStore {
    pub fn new(
        caps: PoolCaps,
        kind_limits: impl IntoIterator<Item = (CacheKind, KindLimit)>,
        shards: usize,
    ) -> Self {
        let kinds: Box<[KindState]> = CacheKind::ALL
            .iter()
            .map(|kind| {
                let state = KindState {
                    usage: Arc::new(Usage::default()),
                    limit: AtomicUsize::new(UNLIMITED),
                };
                state.set_limit(kind.default_limit());
                state
            })
            .collect();
        for (kind, limit) in kind_limits {
            kinds[kind.index()].set_limit(limit);
        }
        SharedStore {
            pools: Pool::ALL.map(|pool| PoolStore::new(pool, caps.get(pool), shards)),
            kinds,
        }
    }

    fn pool(&self, pool: Pool) -> &PoolStore {
        &self.pools[pool.index()]
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

    fn evict_until(&self, pool: &PoolStore, cap: usize) {
        // Read once per round: values dropped by readers lower it between two reads.
        while let Some(excess) = pool.usage().resident().checked_sub(cap).filter(|e| *e > 0) {
            let victims = pool.evict_batch(pool.next_cursor(), excess, true);
            if self.forget(pool, victims) == 0 {
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
            if self.forget(pool, victims) == 0 {
                return false;
            }
        }
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
            return InsertOutcome::Rejected(reason);
        }
        let pool = self.pool(kind.pool());
        let key = KeyRef::new(kind, key);
        let placement = match mode {
            InsertMode::Put => None,
            InsertMode::Fill => match pool.admission(key) {
                Some(placement) => Some(placement),
                None => return InsertOutcome::Deferred,
            },
        };

        // Both budgets are reserved before the value goes in, so concurrent inserts cannot
        // together exceed them; a rejected insert gives its reservation back.
        let state = self.kind(kind);
        if !self.reserve_kind(kind, bytes) {
            return InsertOutcome::Rejected(RejectReason::KindLimit);
        }
        if !self.reserve_pool(pool, pool.shard_of(key), bytes) {
            state.usage.release(bytes);
            return InsertOutcome::Rejected(RejectReason::PoolFullPinned);
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
        self.forget(pool, removed);
    }

    fn set_pool_cap(&self, pool: Pool, bytes: usize) -> Result<()> {
        let store = self.pool(pool);
        store.set_cap(bytes);
        if bytes == 0 {
            let removed = store.remove_all();
            self.forget(store, removed);
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
        let mut report = ShrinkReport::default();
        for pool in Pool::RECLAIM_ORDER {
            let store = self.pool(pool);
            loop {
                let before = store.usage().resident();
                let victims = store.evict_batch(store.next_cursor(), usize::MAX, false);
                let evicted = self.forget(store, victims);
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
