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

pub mod aggregate_function;
mod aggregate_function_state;
mod aggregate_hashtable;
mod aggregate_meta;
mod group_hash;
mod hash_index;
mod hash_index_adapter;
mod partial_controller;
mod partitioned_payload;
mod payload;
mod payload_flush;
mod payload_row;
mod probe_state;
mod row_ptr;

pub use aggregate_function::AggregateCall;
pub use aggregate_function_state::*;
pub use aggregate_hashtable::*;
pub use aggregate_meta::*;
pub use group_hash::*;
pub use partial_controller::*;
pub use partitioned_payload::*;
pub use payload::*;
pub use payload_flush::*;
pub use probe_state::ProbeState;
use probe_state::*;
use row_ptr::*;

use crate::aggregate::hash_index::HashIndex;

// A batch size to probe, flush, repartition, etc.
pub(crate) const BATCH_SIZE: usize = 2048;

// A hash index holds up to 1 / 1.35 (74%) of its slots: a probe matches the tags of a whole
// chunk at once, so the index can be dense.
const HASH_INDEX_LOAD_FACTOR: f64 = 1.35;

pub(crate) const MAX_PAGE_SIZE: usize = 256 * 1024;

#[derive(Clone, Debug)]
pub struct HashTableConfig {
    pub initial_radix_bits: u64,
    pub partition_start_bit: u64,
    // Partial aggregation: the caller drives how rows are handled and how the index grows.
    pub partial_agg: bool,
}

impl Default for HashTableConfig {
    fn default() -> Self {
        Self {
            initial_radix_bits: 3,
            partition_start_bit: 0,
            partial_agg: false,
        }
    }
}

impl HashTableConfig {
    /// A partial aggregate table has a single partition: its rows are routed to the final
    /// aggregates by group hash when they leave it, see `Payload::scatter_into_buckets`.
    pub fn partial_aggregate() -> Self {
        HashTableConfig {
            initial_radix_bits: 0,
            partial_agg: true,
            ..Default::default()
        }
    }

    pub fn with_initial_radix_bits(mut self, initial_radix_bits: u64) -> Self {
        self.initial_radix_bits = initial_radix_bits;
        self
    }

    pub fn with_partition_start_bit(mut self, partition_start_bit: u64) -> Self {
        self.partition_start_bit = partition_start_bit;
        self
    }
}
