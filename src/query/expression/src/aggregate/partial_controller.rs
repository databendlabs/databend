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
//!   bounded cost. At geometric row checkpoints, with `R` rows, `M` groups materialized and a
//!   sampled estimate `D` of the distinct groups:
//!   - `M / R` and `D / R` both close to 1: nothing to absorb, switch to `Bypass`.
//!   - `M > 2 * D`: keys recur beyond a window, so grow the index at once to hold the `D` groups,
//!     or as many as the memory headroom allows. A grown index that fills up restarts in place
//!     like a window.
//! * `Bypass`: forward every row to the final aggregate as raw input, for the rest of the input.
//!
//! Memory pressure is handled by the caller spilling the table; learning restarts afterwards.

use simple_hll::HyperLogLog;

use crate::AggregateHashTable;

/// First learning checkpoint, in rows since learning started.
const FIRST_CHECK_ROWS: usize = 256 * 1024;
/// Each following checkpoint is this many times the previous one.
const CHECK_GROWTH: usize = 4;
/// Bypass when both the materialized and the estimated distinct ratio reach this.
const BYPASS_DISTINCT_RATIO: f64 = 0.9;
/// Grow when materialized groups exceed the estimated distinct groups by this factor.
const GROW_DEDUP_RATIO: f64 = 2.0;
/// Leave this many times the growth of all instances as headroom before memory pressure.
const HEADROOM_FACTOR: usize = 2;
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

/// Estimates the distinct groups since learning started from their group hashes, which are
/// already well mixed, with a HyperLogLog.
///
/// Sampling starts at the first window of a learning period: before it, the index was never
/// restarted and its groups are exactly the distinct groups, recorded as a lower bound.
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
            self.hll.add_hash(hash);
        }
    }

    /// Estimated distinct groups since learning started, if sampling has started.
    pub fn distinct(&self) -> Option<usize> {
        self.first_window_groups
            .map(|first| self.hll.count().max(first))
    }

    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Requested behavior of the partial aggregate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartialAggregateMode {
    /// Adapt at runtime.
    Auto,
    /// Never aggregate.
    Bypass,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartialState {
    Learning,
    Bypass,
}

/// Runtime facts the controller needs besides the stats of the last call.
#[derive(Clone, Copy, Debug, Default)]
pub struct PartialEnvironment {
    /// Memory left before the pressure threshold, if memory is limited.
    pub headroom_bytes: Option<usize>,
    /// Bytes per group of the table, including its index entry.
    pub bytes_per_group: usize,
    /// Partial instances running concurrently.
    pub concurrency: usize,
}

/// What the caller must do after [`PartialAggregateController::observe`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PartialDecision {
    /// Emit the groups of the table and restart the index before the next block.
    pub flush_hot: bool,
}

pub struct PartialAggregateController {
    state: PartialState,
    /// Target index capacity; the initial capacity until the controller decides to grow.
    capacity: usize,
    grown: bool,

    // Counters since learning started.
    learn_rows: usize,
    /// Groups created since learning started, across all windows.
    learn_groups: usize,
    next_check_rows: usize,
    sampler: DistinctSampler,

    total: PartialAddStats,
    grows: usize,
}

impl PartialAggregateController {
    pub fn new(mode: PartialAggregateMode) -> Self {
        Self {
            state: match mode {
                PartialAggregateMode::Auto => PartialState::Learning,
                PartialAggregateMode::Bypass => PartialState::Bypass,
            },
            capacity: AggregateHashTable::capacity_for_groups(0),
            grown: false,
            learn_rows: 0,
            learn_groups: 0,
            next_check_rows: FIRST_CHECK_ROWS,
            sampler: DistinctSampler::default(),
            total: PartialAddStats::default(),
            grows: 0,
        }
    }

    pub fn state(&self) -> PartialState {
        self.state
    }

    /// Index capacity the table grows to at once when full; beyond it the index restarts.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The distinct sampler for the next call while deciding whether to grow. The table starts
    /// it at the first window and feeds it from then on.
    pub fn sampler(&mut self) -> Option<&mut DistinctSampler> {
        (self.state == PartialState::Learning && !self.grown).then_some(&mut self.sampler)
    }

    pub fn total(&self) -> &PartialAddStats {
        &self.total
    }

    pub fn grows(&self) -> usize {
        self.grows
    }

    /// Feed the stats of one `add_groups_partial` call and get the resulting action.
    pub fn observe(
        &mut self,
        stats: &PartialAddStats,
        env: &PartialEnvironment,
    ) -> PartialDecision {
        self.total.rows += stats.rows;
        self.total.new_groups += stats.new_groups;
        self.total.windows += stats.windows;
        if self.state != PartialState::Learning || self.grown {
            return PartialDecision::default();
        }

        self.learn_rows += stats.rows;
        self.learn_groups += stats.new_groups;
        if self.learn_rows < self.next_check_rows {
            return PartialDecision::default();
        }
        while self.next_check_rows <= self.learn_rows {
            self.next_check_rows *= CHECK_GROWTH;
        }

        let rows = self.learn_rows as f64;
        let materialized = self.learn_groups as f64;
        // Materialized groups are an upper bound of the distinct groups.
        let distinct = match self.sampler.distinct() {
            None => materialized,
            Some(distinct) => (distinct as f64).min(materialized),
        };

        if materialized / rows >= BYPASS_DISTINCT_RATIO && distinct / rows >= BYPASS_DISTINCT_RATIO
        {
            self.state = PartialState::Bypass;
            return PartialDecision { flush_hot: true };
        }
        if materialized > distinct * GROW_DEDUP_RATIO {
            let fit_groups = match env.headroom_bytes {
                None => distinct as usize,
                Some(headroom) => {
                    let per_group =
                        env.bytes_per_group.max(1) * env.concurrency.max(1) * HEADROOM_FACTOR;
                    (distinct as usize).min(headroom / per_group)
                }
            };
            let capacity = AggregateHashTable::capacity_for_groups(fit_groups);
            if capacity > self.capacity {
                self.grown = true;
                self.grows += 1;
                self.capacity = capacity;
                // The grown index is rebuilt from the payload, which must hold one row per
                // key: start from an empty table.
                return PartialDecision { flush_hot: true };
            }
        }
        PartialDecision::default()
    }

    /// The caller spilled the table under memory pressure: learn again from windows.
    pub fn on_hot_spilled(&mut self) {
        if self.state == PartialState::Learning {
            self.capacity = AggregateHashTable::capacity_for_groups(0);
            self.grown = false;
            self.learn_rows = 0;
            self.learn_groups = 0;
            self.next_check_rows = FIRST_CHECK_ROWS;
            self.sampler.reset();
        }
    }
}
