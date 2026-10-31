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

use std::fmt::Display;
use std::fmt::Formatter;

use databend_common_exception::Result;

use crate::CacheKind;
use crate::Pool;
use crate::handle::cache_ref::ErasedValue;
use crate::handle::cache_ref::Reservation;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Unit {
    Bytes,
    Count,
}

impl Display for Unit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Unit::Bytes => f.write_str("bytes"),
            Unit::Count => f.write_str("count"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KindLimit {
    /// `None`: bounded only by the pool. `Some(0)`: disabled.
    pub max_bytes: Option<usize>,
}

impl KindLimit {
    pub const UNLIMITED: KindLimit = KindLimit { max_bytes: None };
    pub const DISABLED: KindLimit = KindLimit { max_bytes: Some(0) };

    pub const fn bytes(max_bytes: usize) -> KindLimit {
        KindLimit {
            max_bytes: Some(max_bytes),
        }
    }

    pub const fn is_disabled(&self) -> bool {
        matches!(self.max_bytes, Some(0))
    }

    pub fn allows(&self, used: usize, bytes: usize) -> bool {
        self.max_bytes.is_none_or(|max| used + bytes <= max)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InsertMode {
    /// Read-path refill, subject to admission.
    Fill,
    /// Write-path insertion, bypassing admission.
    Put,
}

pub enum InsertOutcome {
    Inserted(Reservation),
    Deferred,
    Rejected(RejectReason),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectReason {
    Disabled,
    /// Larger than the kind's limit, or the kind is at its limit with every entry held.
    KindLimit,
    Oversized,
    PoolFullPinned,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ShrinkReport {
    pub freed: usize,
    pub evicted: usize,
    pub batches: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KindStats {
    pub kind: CacheKind,
    pub items: usize,
    pub resident: usize,
    pub in_cache: usize,
    /// `None`: bounded only by the pool. `Some(0)`: disabled.
    pub capacity: Option<usize>,
    pub unit: Unit,
    /// Why entries left or were refused; `None` for a backend that does not count them.
    pub events: Option<KindEvents>,
}

/// Counts, since the store was created, of entries of one kind leaving or not entering it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KindEvents {
    /// Evicted for room in the pool, or because a pool cap or kind limit was lowered.
    pub evicted_for_capacity: u64,
    pub evicted_for_capacity_bytes: u64,
    /// Reclaimed for queries under memory pressure.
    pub evicted_for_pressure: u64,
    pub evicted_for_pressure_bytes: u64,
    /// Loaded values not cached yet: the key had not been read often enough.
    pub admission_deferred: u64,
    pub rejected_oversized: u64,
    pub rejected_kind_limit: u64,
    pub rejected_pool_full_pinned: u64,
}

/// Totals of the reclaims under memory pressure, since the store was created.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReclaimStats {
    pub count: u64,
    pub freed_bytes: u64,
    pub millis: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolStats {
    pub pool: Pool,
    pub cap: usize,
    pub items: usize,
    pub resident: usize,
    pub in_cache: usize,
}

/// Type-erased storage for all kinds. Implementations decide capacity and eviction.
pub trait KindStore: Send + Sync {
    /// False when the kind or its pool is turned off; callers then skip every tier.
    fn is_enabled(&self, kind: CacheKind) -> bool;

    fn get(&self, kind: CacheKind, key: &str) -> Option<ErasedValue>;

    fn contains(&self, kind: CacheKind, key: &str) -> bool;

    fn insert(
        &self,
        kind: CacheKind,
        key: &str,
        value: ErasedValue,
        bytes: usize,
        mode: InsertMode,
    ) -> InsertOutcome;

    fn evict(&self, kind: CacheKind, key: &str) -> bool;

    /// `KindLimit::DISABLED` clears the kind; a lower limit evicts down to it.
    fn set_kind_limit(&self, kind: CacheKind, limit: KindLimit);

    /// A cap of 0 clears and disables every kind of the pool.
    fn set_pool_cap(&self, pool: Pool, bytes: usize) -> Result<()>;

    fn kind_stats(&self, kind: CacheKind) -> KindStats;

    fn pool_stats(&self) -> Vec<PoolStats>;

    /// Evicts unreferenced entries under memory pressure until `relieved` returns true or nothing
    /// is left to evict; the shared backend goes Data → Index → Meta, the legacy one frees
    /// nothing. Must not block on locks held by the calling thread.
    fn shrink_for_pressure(&self, relieved: &dyn Fn() -> bool) -> ShrinkReport;

    /// `None` for a backend that never reclaims.
    fn reclaim_stats(&self) -> Option<ReclaimStats> {
        None
    }

    /// Whether values are charged their pre-refactor estimate, `CachedItem::legacy_bytes`,
    /// rather than `mem_bytes`.
    fn charges_legacy_bytes(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kind_limit() {
        assert!(KindLimit::UNLIMITED.allows(usize::MAX / 2, 1));
        assert!(KindLimit::DISABLED.is_disabled());
        assert!(KindLimit::bytes(10).allows(4, 6));
        assert!(!KindLimit::bytes(10).allows(5, 6));
    }
}
