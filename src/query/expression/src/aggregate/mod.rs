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

#![allow(clippy::needless_range_loop)]
#![allow(clippy::too_many_arguments)]

mod aggregate_function;
mod aggregate_function_state;
mod aggregate_hashtable;
mod group_hash;
mod legacy_hash_index;
mod new_hash_index;
mod partitioned_payload;
mod payload;
mod payload_flush;
mod payload_row;
mod probe_state;
mod row_ptr;

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

pub use aggregate_function::*;
pub use aggregate_function_state::*;
pub use aggregate_hashtable::*;
pub use group_hash::*;
pub use partitioned_payload::*;
pub use payload::*;
pub use payload_flush::*;
pub use probe_state::ProbeState;
use probe_state::*;
use row_ptr::*;

use crate::aggregate::legacy_hash_index::LegacyHashIndex;
use crate::aggregate::legacy_hash_index::TableAdapter;
use crate::aggregate::new_hash_index::ExperimentalHashIndex;

// A batch size to probe, flush, repartition, etc.
pub(crate) const BATCH_SIZE: usize = 2048;

const LOAD_FACTOR: f64 = 1.5;

// 75% of the capacity
// new index can probe multiple ctrl byte by SIMD
// we can make the hash index more
const NEW_INDEX_LOAD_FACTOR: f64 = 1.35;

pub(crate) const MAX_PAGE_SIZE: usize = 256 * 1024;

// Assume (1 << 15) = 32KB L1 cache per core, divided by two because hyperthreading
pub(crate) const L1_CACHE_SIZE: usize = 32768 / 2;
// Assume (1 << 20) = 1MB L2 cache per core, divided by two because hyperthreading
pub(crate) const L2_CACHE_SIZE: usize = 1048576 / 2;
// Assume (1 << 20) + (1 << 19) = 1.5MB L3 cache per core (shared), divided by two because hyperthreading
pub(crate) const L3_CACHE_SIZE: usize = 1572864 / 2;

pub(crate) const MAX_RADIX_BITS: u64 = 7;
pub const MAX_AGGREGATE_HASHTABLE_BUCKETS_NUM: u64 = 1 << MAX_RADIX_BITS;

enum HashIndex {
    Legacy(LegacyHashIndex),
    Experimental(ExperimentalHashIndex),
}

impl HashIndex {
    /// Create a HashIndex with the given capacity
    fn new(config: &HashTableConfig, capacity: usize) -> HashIndex {
        if config.enable_experiment_hash_index {
            HashIndex::Experimental(ExperimentalHashIndex::with_capacity(capacity))
        } else {
            HashIndex::Legacy(LegacyHashIndex::with_capacity(capacity))
        }
    }

    /// Create a dummy LegacyHashIndex with zero capacity
    /// Any operation on this LegacyHashIndex is not allowed.
    fn new_dummy(config: &HashTableConfig) -> HashIndex {
        if config.enable_experiment_hash_index {
            HashIndex::Experimental(ExperimentalHashIndex::dummy())
        } else {
            HashIndex::Legacy(LegacyHashIndex::dummy())
        }
    }

    fn capacity(&self) -> usize {
        match self {
            HashIndex::Legacy(index) => index.capacity(),
            HashIndex::Experimental(index) => index.capacity(),
        }
    }

    fn count(&self) -> usize {
        match self {
            HashIndex::Legacy(index) => index.count(),
            HashIndex::Experimental(index) => index.count(),
        }
    }

    fn resize_threshold(&self) -> usize {
        match self {
            HashIndex::Legacy(index) => index.resize_threshold(),
            HashIndex::Experimental(index) => index.resize_threshold(),
        }
    }

    fn allocated_bytes(&self) -> usize {
        match self {
            HashIndex::Legacy(index) => index.allocated_bytes(),
            HashIndex::Experimental(index) => index.allocated_bytes(),
        }
    }

    fn reset(&mut self) {
        match self {
            HashIndex::Legacy(index) => index.reset(),
            HashIndex::Experimental(index) => index.reset(),
        }
    }

    fn probe_and_create(
        &mut self,
        state: &mut ProbeState,
        row_count: usize,
        adapter: &mut dyn TableAdapter,
    ) -> usize {
        match self {
            HashIndex::Legacy(index) => index.probe_and_create(state, row_count, adapter),
            HashIndex::Experimental(index) => index.probe_and_create(state, row_count, adapter),
        }
    }

    fn probe_slot_and_set(&mut self, hash: u64, row_ptr: RowPtr) {
        match self {
            HashIndex::Legacy(index) => index.probe_slot_and_set(hash, row_ptr),
            HashIndex::Experimental(index) => index.probe_slot_and_set(hash, row_ptr),
        }
    }
}

#[derive(Clone, Debug)]
pub struct HashTableConfig {
    // Max radix bits across all threads, this is a hint to repartition
    pub current_max_radix_bits: Arc<AtomicU64>,
    pub initial_radix_bits: u64,
    pub max_radix_bits: u64,
    pub repartition_radix_bits_incr: u64,
    pub block_fill_factor: f64,
    pub partial_agg: bool,
    pub max_partial_capacity: usize,
    pub enable_experiment_hash_index: bool,
}

impl Default for HashTableConfig {
    fn default() -> Self {
        Self {
            current_max_radix_bits: Arc::new(AtomicU64::new(3)),
            initial_radix_bits: 3,
            max_radix_bits: MAX_RADIX_BITS,
            repartition_radix_bits_incr: 2,
            block_fill_factor: 1.8,
            partial_agg: false,
            max_partial_capacity: 131072,
            enable_experiment_hash_index: false,
        }
    }
}

impl HashTableConfig {
    pub fn new_experiment_partial(
        radix_bits: u64,
        node_nums: usize,
        active_threads: usize,
    ) -> Self {
        let capacity = if node_nums != 1 {
            131072 * (2 << node_nums)
        } else {
            let total_shared_cache_size = active_threads * L3_CACHE_SIZE;
            let cache_per_active_thread =
                L1_CACHE_SIZE + L2_CACHE_SIZE + total_shared_cache_size / active_threads;
            let size_per_entry = (8_f64 * LOAD_FACTOR) as usize;
            (cache_per_active_thread / size_per_entry).next_power_of_two()
        };

        // not support payload growth when `enable_experiment_aggregate` = 1
        HashTableConfig {
            current_max_radix_bits: Arc::new(AtomicU64::new(radix_bits)),
            initial_radix_bits: radix_bits,
            max_radix_bits: radix_bits,
            repartition_radix_bits_incr: 0,
            partial_agg: true,
            max_partial_capacity: capacity,
            ..Default::default()
        }
    }

    pub fn with_initial_radix_bits(mut self, initial_radix_bits: u64) -> Self {
        self.initial_radix_bits = initial_radix_bits;
        self.current_max_radix_bits = Arc::new(AtomicU64::new(initial_radix_bits));
        self
    }

    pub fn with_experiment_hash_index(mut self, enable: bool) -> Self {
        self.enable_experiment_hash_index = enable;
        self
    }

    pub fn with_partial(mut self, partial_agg: bool, active_threads: usize) -> Self {
        self.partial_agg = partial_agg;

        // init max_partial_capacity
        let total_shared_cache_size = active_threads * L3_CACHE_SIZE;
        let cache_per_active_thread =
            L1_CACHE_SIZE + L2_CACHE_SIZE + total_shared_cache_size / active_threads;
        let size_per_entry = (8_f64 * LOAD_FACTOR) as usize;
        let capacity = (cache_per_active_thread / size_per_entry).next_power_of_two();
        self.max_partial_capacity = capacity;

        self
    }

    pub fn cluster_with_partial(mut self, partial_agg: bool, node_nums: usize) -> Self {
        self.partial_agg = partial_agg;
        self.repartition_radix_bits_incr = 4;
        self.max_partial_capacity = 131072 * (2 << node_nums);

        self
    }

    pub fn update_current_max_radix_bits(&self) {
        loop {
            let current_max_radix_bits = self.current_max_radix_bits.load(Ordering::SeqCst);
            if current_max_radix_bits < self.max_radix_bits
                && self
                    .current_max_radix_bits
                    .compare_exchange(
                        current_max_radix_bits,
                        self.max_radix_bits,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_err()
            {
                continue;
            }
            break;
        }
    }
}
