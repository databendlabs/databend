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

mod aggregate_function;
mod aggregate_function_state;
mod aggregate_hashtable;
mod aggregate_meta;
mod group_hash;
mod hash_index;
mod hash_index_adapter;
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
pub use aggregate_meta::*;
pub use group_hash::*;
pub use partitioned_payload::*;
pub use payload::*;
pub use payload_flush::*;
pub use probe_state::ProbeState;
use probe_state::*;
use row_ptr::*;

use crate::aggregate::hash_index::HashIndex;

// A batch size to probe, flush, repartition, etc.
pub(crate) const BATCH_SIZE: usize = 2048;

const LOAD_FACTOR: f64 = 1.5;

// 75% of the capacity
// The hash index can probe multiple ctrl bytes by SIMD, so it can use a denser
// load factor than the payload capacity estimate.
const HASH_INDEX_LOAD_FACTOR: f64 = 1.35;

pub(crate) const MAX_PAGE_SIZE: usize = 256 * 1024;

// Assume (1 << 15) = 32KB L1 cache per core, divided by two because hyperthreading
pub(crate) const L1_CACHE_SIZE: usize = 32768 / 2;
// Assume (1 << 20) = 1MB L2 cache per core, divided by two because hyperthreading
pub(crate) const L2_CACHE_SIZE: usize = 1048576 / 2;
// Assume (1 << 20) + (1 << 19) = 1.5MB L3 cache per core (shared), divided by two because hyperthreading
pub(crate) const L3_CACHE_SIZE: usize = 1572864 / 2;

pub(crate) const MAX_RADIX_BITS: u64 = 7;
pub const MAX_AGGREGATE_HASHTABLE_BUCKETS_NUM: u64 = 1 << MAX_RADIX_BITS;

#[derive(Clone, Debug)]
pub struct HashTableConfig {
    // Max radix bits across all threads, this is a hint to repartition
    pub current_max_radix_bits: Arc<AtomicU64>,
    pub initial_radix_bits: u64,
    pub partition_start_bit: u64,
    pub max_radix_bits: u64,
    pub repartition_radix_bits_incr: u64,
    pub block_fill_factor: f64,
    pub partial_agg: bool,
    pub max_partial_capacity: usize,
}

impl Default for HashTableConfig {
    fn default() -> Self {
        Self {
            current_max_radix_bits: Arc::new(AtomicU64::new(3)),
            initial_radix_bits: 3,
            partition_start_bit: 0,
            max_radix_bits: MAX_RADIX_BITS,
            repartition_radix_bits_incr: 2,
            block_fill_factor: 1.8,
            partial_agg: false,
            max_partial_capacity: 131072,
        }
    }
}

impl HashTableConfig {
    pub fn partial_aggregate(radix_bits: u64, node_nums: usize, active_threads: usize) -> Self {
        let capacity = if node_nums != 1 {
            131072 * (2 << node_nums)
        } else {
            let total_shared_cache_size = active_threads * L3_CACHE_SIZE;
            let cache_per_active_thread =
                L1_CACHE_SIZE + L2_CACHE_SIZE + total_shared_cache_size / active_threads;
            let size_per_entry = (8_f64 * LOAD_FACTOR) as usize;
            (cache_per_active_thread / size_per_entry).next_power_of_two()
        };

        // Partial aggregate does not support payload growth after the target radix is fixed.
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

    pub fn with_partition_start_bit(mut self, partition_start_bit: u64) -> Self {
        self.partition_start_bit = partition_start_bit;
        self
    }
}
