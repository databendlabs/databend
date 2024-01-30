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
mod partitioned_payload;
mod payload;
mod payload_flush;
mod payload_row;
mod probe_state;

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

pub use aggregate_function::*;
pub use aggregate_function_state::*;
pub use aggregate_hashtable::*;
pub use group_hash::*;
pub use partitioned_payload::*;
pub use payload::*;
pub use payload_flush::*;
pub use probe_state::*;

pub type SelectVector = [usize; BATCH_SIZE];

pub fn new_sel() -> SelectVector {
    [0; BATCH_SIZE]
}

// A batch size to probe, flush, repartition, etc.
pub(crate) const BATCH_SIZE: usize = 2048;
pub(crate) const LOAD_FACTOR: f64 = 1.5;
pub(crate) const MAX_PAGE_SIZE: usize = 256 * 1024;

pub(crate) const L1_CACHE_SIZE: usize = 32 * 1024 / 2;
pub(crate) const L2_CACHE_SIZE: usize = 1024 * 1024 / 2;
pub(crate) const L3_CACHE_SIZE: usize =  (512 + 1024) * 1024 / 2;

// pub(crate) const L2_MAX_ROWS_IN_HT: usize = 1024 * 1024 / 8 / 2;
// pub(crate) const L3_MAX_ROWS_IN_HT: usize = 16 * 1024 * 1024 / 8 / 2;

#[derive(Clone, Debug)]
pub struct HashTableConfig {
    // Max radix bits across all threads, this is a hint to repartition
    pub current_max_radix_bits: Arc<AtomicU64>,
    pub initial_radix_bits: u64,
    pub max_radix_bits: u64,
    pub repartition_radix_bits_incr: u64,
    pub block_fill_factor: f64,
    pub partial_agg: bool,
    // min reduction ratio to control whether to expand the ht
    // {1024 * 1024, 1.1} / {16 * 1024 * 1024, 2.0},
    pub min_reductions: [f64; 2],
    pub capacity: usize,
}

impl Default for HashTableConfig {
    fn default() -> Self {
        Self {
            current_max_radix_bits: Arc::new(AtomicU64::new(3)),
            initial_radix_bits: 3,
            max_radix_bits: 7,
            repartition_radix_bits_incr: 2,
            block_fill_factor: 1.8,
            partial_agg: false,
            min_reductions: [1.1, 2.0],
            capacity: 0,
        }
    }
}

impl HashTableConfig {
    pub fn with_initial_radix_bits(mut self, initial_radix_bits: u64) -> Self {
        self.initial_radix_bits = initial_radix_bits;
        self.current_max_radix_bits = Arc::new(AtomicU64::new(initial_radix_bits));
        self
    }

    pub fn with_partial(mut self, partial_agg: bool) -> Self {
        self.partial_agg = partial_agg;
        self
    }

    pub fn with_initial_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }
}
