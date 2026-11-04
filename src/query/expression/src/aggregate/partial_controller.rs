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

//! Adaptive control of a thread-local partial aggregate.
//!
//! The partial aggregate is a cache in front of the final aggregate: its output may repeat group
//! keys, and the final aggregate merges them. Each instance decides on its own:
//!
//! * `Learning` (start): a small, cache-resident index that restarts in place when full (a
//!   "window") while the payload keeps the groups. This absorbs locality and hot keys at a low,
//!   bounded cost. At geometric row checkpoints, with `R` rows, `M` groups materialized, a
//!   sampled estimate `D` of the distinct groups and `G` the groups the best table it can build
//!   would emit: `D` if an index holding them fits (see `Grown`), else `M`, the window's:
//!   - `M / R` and `G / R` both close to 1: neither absorbs much, switch to `Bypass`.
//!   - `M > 2 * G`: keys recur beyond a window and a grown table holds them, switch to `Grown`.
//! * `Grown`: the index is grown at once to hold the `D` groups, if they fit in
//!   [`MAX_GROW_CAPACITY`] slots and, with the bytes a group takes, in the memory of the table.
//!   When it fills up it restarts in place like a window.
//! * `Shrunk`: a grown table met memory pressure, emitted its groups and went back to the small
//!   index for the rest of the input, without further decisions.
//! * `Bypass`: forward every row to the final aggregate as raw input, for the rest of the input.
//!
//! The partial aggregate never spills: it bounds itself and leaves spilling to the final
//! aggregate. [`PartialState::step`] takes every decision of an instance, and tells the
//! caller to emit the table to the final aggregate, with an [`EmitReason`]: on each change of
//! state, once the table holds its memory (see [`partial_table_bytes`]), and once the index
//! restarted and the payload, no longer bounded by it, reached the size of a block.

use std::fmt;
use std::ops::AddAssign;

use simple_hll::HyperLogLog;

use crate::AggregateHashTable;

/// First learning checkpoint, in rows since learning started.
const FIRST_CHECK_ROWS: usize = 256 * 1024;
/// Each following checkpoint is this many times the previous one.
const CHECK_GROWTH: usize = 4;
/// Bypass when the window and the best table an instance can build both emit this share of the
/// rows.
const BYPASS_DISTINCT_RATIO: f64 = 0.9;
/// Grow when materialized groups exceed the estimated distinct groups by this factor.
const GROW_DEDUP_RATIO: f64 = 2.0;
/// The largest grown index: 4M slots, which the index rounds to 3M slots (whole chunks, a power
/// of two of them), 16 MiB of index for up to 2.3M groups per instance. Every instance holds the
/// keys it sees, so a larger table multiplies their memory by the number of threads, while a
/// table that cannot hold the distinct groups absorbs little of them. Keys beyond it are left to
/// the final aggregate.
pub const MAX_GROW_CAPACITY: usize = 1 << 22;
/// Bounds of the memory of a partial table, see [`partial_table_bytes`].
pub const MIN_PARTIAL_TABLE_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_PARTIAL_TABLE_BYTES: usize = 256 * 1024 * 1024;
/// Grow to hold this many times the estimated distinct groups. An index short of a few keys
/// absorbs nothing when the keys come in a cyclic order, as each restarts the index before it
/// repeats, so the margin covers the estimate error.
const GROW_MARGIN: f64 = 1.1;
/// 4096 registers (4KB), about 1.6% standard error: far below the factors the decisions use.
const HLL_PRECISION: usize = 12;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PartialAddStats {
    pub rows: usize,
    /// Rows that created a new group.
    pub new_groups: usize,
    /// Times the index restarted in place.
    pub windows: usize,
}

impl AddAssign for PartialAddStats {
    fn add_assign(&mut self, other: Self) {
        self.rows += other.rows;
        self.new_groups += other.new_groups;
        self.windows += other.windows;
    }
}

/// Estimates the distinct groups since learning started from their group hashes, with a
/// HyperLogLog.
///
/// Sampling starts at the first window of a learning period, with the groups of that window:
/// before it, the index was never restarted and its groups are exactly the distinct groups, also
/// recorded as a lower bound.
#[derive(Default)]
pub struct DistinctSampler {
    hll: HyperLogLog<HLL_PRECISION>,
    first_window_groups: Option<usize>,
}

impl DistinctSampler {
    /// Start sampling at the first window, which held `groups` distinct groups.
    pub fn start(&mut self, groups: usize) {
        if self.first_window_groups.is_none() {
            self.first_window_groups = Some(groups);
        }
    }

    pub fn is_started(&self) -> bool {
        self.first_window_groups.is_some()
    }

    #[inline]
    pub fn observe(&mut self, hashes: &[u64]) {
        for &hash in hashes {
            self.observe_hash(hash);
        }
    }

    #[inline]
    pub fn observe_hash(&mut self, hash: u64) {
        self.hll.add_hash(mix_hash(hash));
    }

    /// Estimated distinct groups since learning started, if sampling has started.
    pub fn distinct(&self) -> Option<usize> {
        self.first_window_groups
            .map(|first| self.hll.count().max(first))
    }
}

/// The MurmurHash3 finalizer. The group hashes of some types, short strings among them, are poorly
/// mixed in the low bits that select the HyperLogLog register, which biases the estimate low by
/// up to about 20%; the finalizer mixes every bit into every other.
#[inline]
fn mix_hash(mut hash: u64) -> u64 {
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    hash ^ (hash >> 33)
}

/// Requested behavior of the partial aggregate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartialAggregateMode {
    /// Adapt at runtime.
    Auto,
    /// Never aggregate.
    Bypass,
}

/// Share of `max_memory_usage`, in percent, that the partial tables of one partial aggregate hold
/// together; each partial aggregate of a query has its own share.
const PARTIAL_TABLE_MEMORY_PERCENT: usize = 2;

/// The most memory a partial table holds, index included: [`PARTIAL_TABLE_MEMORY_PERCENT`] of
/// `max_memory_usage` shared by the `threads` instances of a partial aggregate, within
/// [`MIN_PARTIAL_TABLE_BYTES`] and [`MAX_PARTIAL_TABLE_BYTES`], 128 MiB with 8 GB per core. A
/// grown table must fit in it, and the table is emitted once it holds that much, which mostly
/// bounds the memory aggregate states own outside the arena (the values of `array_agg`, a
/// distinct set), as the index and the payload are bounded otherwise. The bounds keep a table
/// that can grow on small machines, and one that is not larger than the 4M-slot index needs
/// when there are few threads.
pub fn partial_table_bytes(max_memory_usage: usize, threads: usize) -> usize {
    let share = max_memory_usage / 100 * PARTIAL_TABLE_MEMORY_PERCENT / threads.max(1);
    share.clamp(MIN_PARTIAL_TABLE_BYTES, MAX_PARTIAL_TABLE_BYTES)
}

/// What the caller measured after one `add_groups_partial` call.
#[derive(Clone, Copy, Debug, Default)]
pub struct PartialObservation {
    pub stats: PartialAddStats,
    /// Memory the table holds, index included, since it was last emitted.
    pub held_bytes: usize,
    /// Memory a group took so far (its row, its states and what they own), or 0 if unknown. The
    /// index is not included while learning, the only state that uses it: it only grows, inside
    /// the measured calls, when the table leaves learning.
    pub group_bytes: usize,
    /// The index restarted since the table was last emitted, and the payload reached the size
    /// of a block.
    pub payload_full: bool,
}

/// Why [`PartialState::step`] asks the caller to emit the groups of the table to the final
/// aggregate and restart the index at the capacity of the new state. A grown index is rebuilt
/// from the payload, which must hold one row per key, so every change of state emits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmitReason {
    Grow,
    Bypass,
    Shrink,
    MemoryFull,
    PayloadFull,
}

/// The state of one partial aggregate, see the module documentation.
#[derive(Debug)]
pub enum PartialState {
    Learning(Learning),
    Grown {
        /// Index capacity the table grows to at once when full.
        capacity: usize,
    },
    Shrunk,
    Bypass,
}

/// Counters of the learning state.
pub struct Learning {
    /// Stats since learning started; groups are counted across all windows.
    seen: PartialAddStats,
    next_check_rows: usize,
    sampler: DistinctSampler,
}

impl Default for Learning {
    fn default() -> Self {
        Self {
            seen: PartialAddStats::default(),
            next_check_rows: FIRST_CHECK_ROWS,
            sampler: DistinctSampler::default(),
        }
    }
}

impl fmt::Debug for Learning {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Learning")
            .field("rows", &self.seen.rows)
            .field("new_groups", &self.seen.new_groups)
            .finish_non_exhaustive()
    }
}

impl PartialState {
    pub fn new(mode: PartialAggregateMode) -> Self {
        match mode {
            PartialAggregateMode::Auto => PartialState::Learning(Learning::default()),
            PartialAggregateMode::Bypass => PartialState::Bypass,
        }
    }

    /// Index capacity of the table, which restarts in place when full.
    pub fn capacity(&self) -> usize {
        match self {
            PartialState::Grown { capacity } => *capacity,
            _ => AggregateHashTable::capacity_for_groups(0),
        }
    }

    /// The distinct sampler for the next call while learning. The table starts it at the first
    /// window and feeds it from then on.
    pub fn sampler(&mut self) -> Option<&mut DistinctSampler> {
        match self {
            PartialState::Learning(learning) => Some(&mut learning.sampler),
            _ => None,
        }
    }

    /// Take the decisions after one `add_groups_partial` call: move to the next state, and say
    /// whether to emit the table. `max_table_bytes` comes from [`partial_table_bytes`].
    /// `memory_pressure` is only checked for a grown table, which cannot release its index
    /// otherwise.
    pub fn step(
        &mut self,
        observation: PartialObservation,
        max_table_bytes: usize,
        memory_pressure: impl FnOnce() -> bool,
    ) -> Option<EmitReason> {
        let next = match self {
            PartialState::Learning(learning) => {
                learning.observe(observation.stats, observation.group_bytes, max_table_bytes)
            }
            PartialState::Grown { .. } if memory_pressure() => Some(PartialState::Shrunk),
            PartialState::Bypass => return None,
            _ => None,
        };
        if let Some(next) = next {
            *self = next;
            return Some(match self {
                PartialState::Grown { .. } => EmitReason::Grow,
                PartialState::Shrunk => EmitReason::Shrink,
                _ => EmitReason::Bypass,
            });
        }
        if observation.held_bytes >= max_table_bytes {
            return Some(EmitReason::MemoryFull);
        }
        observation.payload_full.then_some(EmitReason::PayloadFull)
    }
}

impl Learning {
    /// The next state at a checkpoint, if learning ends there.
    fn observe(
        &mut self,
        stats: PartialAddStats,
        group_bytes: usize,
        max_table_bytes: usize,
    ) -> Option<PartialState> {
        self.seen += stats;
        if self.seen.rows < self.next_check_rows {
            return None;
        }
        while self.next_check_rows <= self.seen.rows {
            self.next_check_rows *= CHECK_GROWTH;
        }

        let rows = self.seen.rows as f64;
        let materialized = self.seen.new_groups as f64;
        // Materialized groups are an upper bound of the distinct groups.
        let distinct = match self.sampler.distinct() {
            None => materialized,
            Some(distinct) => (distinct as f64).min(materialized),
        };

        // A table that holds the distinct groups emits them once each. When it does not fit, the
        // window is the best this instance can do, and as the distinct groups only grow, the
        // table will not fit later either.
        let groups = (distinct * GROW_MARGIN) as usize;
        let capacity = AggregateHashTable::capacity_for_groups(groups);
        let bytes = AggregateHashTable::index_bytes_for(capacity)
            .saturating_add(groups.saturating_mul(group_bytes));
        let fits = capacity <= MAX_GROW_CAPACITY && bytes <= max_table_bytes;
        let reachable = if fits { distinct } else { materialized };

        if materialized / rows >= BYPASS_DISTINCT_RATIO && reachable / rows >= BYPASS_DISTINCT_RATIO
        {
            return Some(PartialState::Bypass);
        }
        if materialized > reachable * GROW_DEDUP_RATIO
            && capacity > AggregateHashTable::capacity_for_groups(0)
        {
            return Some(PartialState::Grown { capacity });
        }
        None
    }
}
