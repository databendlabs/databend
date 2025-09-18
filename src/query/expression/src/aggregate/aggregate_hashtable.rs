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

// A new AggregateHashtable which inspired by duckdb's https://duckdb.org/2022/03/07/aggregate-hashtable.html

use std::sync::atomic::Ordering;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::group_hash_columns;
use super::hash_index::AdapterImpl;
use super::hash_index::HashIndex;
use super::partitioned_payload::PartitionedPayload;
use super::payload_flush::PayloadFlushState;
use super::probe_state::ProbeState;
use super::Entry;
use super::HashTableConfig;
use super::Payload;
use super::LOAD_FACTOR;
use super::MAX_PAGE_SIZE;
use crate::types::DataType;
use crate::AggregateFunctionRef;
use crate::BlockEntry;
use crate::ColumnBuilder;
use crate::ProjectedBlock;

const BATCH_ADD_SIZE: usize = 2048;

pub struct AggregateHashTable<const PARTIAL: bool> {
    pub payload: PartitionedPayload,
    // use for append rows directly during deserialize
    pub direct_append: bool,
    pub config: HashTableConfig,

    current_radix_bits: u64,
    hash_index: HashIndex,
}

unsafe impl<const PARTIAL: bool> Send for AggregateHashTable<PARTIAL> {}
unsafe impl<const PARTIAL: bool> Sync for AggregateHashTable<PARTIAL> {}

impl<const PARTIAL: bool> AggregateHashTable<PARTIAL> {
    pub fn new(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
        config: HashTableConfig,
        arena: Arc<Bump>,
    ) -> Self {
        assert_eq!(config.partial_agg, PARTIAL);
        let capacity = Self::initial_capacity();
        Self::new_with_capacity(group_types, aggrs, config, capacity, arena)
    }

    pub fn new_with_capacity(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
        config: HashTableConfig,
        capacity: usize,
        arena: Arc<Bump>,
    ) -> Self {
        Self {
            direct_append: false,
            current_radix_bits: config.initial_radix_bits,
            payload: PartitionedPayload::new(
                group_types,
                aggrs,
                1 << config.initial_radix_bits,
                vec![arena],
            ),
            hash_index: HashIndex::with_capacity(capacity),
            config,
        }
    }

    pub fn new_directly(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
        config: HashTableConfig,
        capacity: usize,
        arena: Arc<Bump>,
        need_init_entry: bool,
    ) -> Self {
        let entries = if need_init_entry {
            vec![Entry::default(); capacity]
        } else {
            vec![]
        };
        Self {
            direct_append: !need_init_entry,
            current_radix_bits: config.initial_radix_bits,
            payload: PartitionedPayload::new(
                group_types,
                aggrs,
                1 << config.initial_radix_bits,
                vec![arena],
            ),
            hash_index: HashIndex {
                entries,
                count: 0,
                capacity,
            },
            config,
        }
    }

    pub fn len(&self) -> usize {
        self.payload.len()
    }

    pub fn add_groups(
        &mut self,
        state: &mut ProbeState,
        group_columns: ProjectedBlock,
        params: &[ProjectedBlock],
        agg_states: ProjectedBlock,
        row_count: usize,
    ) -> Result<usize> {
        if row_count <= BATCH_ADD_SIZE {
            self.add_groups_inner(state, group_columns, params, agg_states, row_count)
        } else {
            let mut new_count = 0;
            for start in (0..row_count).step_by(BATCH_ADD_SIZE) {
                let end = (start + BATCH_ADD_SIZE).min(row_count);
                let step_group_columns = group_columns
                    .iter()
                    .map(|entry| entry.slice(start..end))
                    .collect::<Vec<_>>();

                let step_params: Vec<Vec<BlockEntry>> = params
                    .iter()
                    .map(|c| c.iter().map(|x| x.slice(start..end)).collect())
                    .collect();
                let step_params = step_params.iter().map(|v| v.into()).collect::<Vec<_>>();
                let agg_states = agg_states
                    .iter()
                    .map(|c| c.slice(start..end))
                    .collect::<Vec<_>>();

                new_count += self.add_groups_inner(
                    state,
                    (&step_group_columns).into(),
                    &step_params,
                    (&agg_states).into(),
                    end - start,
                )?;
            }
            Ok(new_count)
        }
    }

    // Add new groups and combine the states
    fn add_groups_inner(
        &mut self,
        state: &mut ProbeState,
        group_columns: ProjectedBlock,
        params: &[ProjectedBlock],
        agg_states: ProjectedBlock,
        row_count: usize,
    ) -> Result<usize> {
        #[cfg(debug_assertions)]
        {
            for (i, group_column) in group_columns.iter().enumerate() {
                if group_column.data_type() != self.payload.group_types[i] {
                    return Err(ErrorCode::UnknownException(format!(
                        "group_column type not match in index {}, expect: {:?}, actual: {:?}",
                        i,
                        self.payload.group_types[i],
                        group_column.data_type()
                    )));
                }
            }
        }

        state.row_count = row_count;
        group_hash_columns(group_columns, &mut state.group_hashes);

        let new_group_count = if self.direct_append {
            for idx in 0..row_count {
                state.empty_vector[idx] = idx;
            }
            self.payload.append_rows(state, row_count, group_columns);
            row_count
        } else {
            self.probe_and_create(state, group_columns, row_count)
        };

        if !self.payload.aggrs.is_empty() {
            for i in 0..row_count {
                state.state_places[i] = state.addresses[i].state_addr(&self.payload.row_layout);
            }

            let state_places = &state.state_places.as_slice()[0..row_count];
            let states_layout = self.payload.row_layout.states_layout.as_ref().unwrap();
            if agg_states.is_empty() {
                for ((func, params), loc) in self
                    .payload
                    .aggrs
                    .iter()
                    .zip(params.iter())
                    .zip(states_layout.states_loc.iter())
                {
                    func.accumulate_keys(state_places, loc, *params, row_count)?;
                }
            } else {
                for ((func, state), loc) in self
                    .payload
                    .aggrs
                    .iter()
                    .zip(agg_states.iter())
                    .zip(states_layout.states_loc.iter())
                {
                    func.batch_merge(state_places, loc, state, None)?;
                }
            }
        }

        if PARTIAL {
            // check size
            if self.hash_index.count + BATCH_ADD_SIZE > self.hash_index.resize_threshold()
                && self.hash_index.capacity >= self.config.max_partial_capacity
            {
                self.clear_ht();
            }

            // check maybe_repartition
            if self.maybe_repartition() {
                self.clear_ht();
            }
        }

        Ok(new_group_count)
    }

    fn probe_and_create(
        &mut self,
        state: &mut ProbeState,
        group_columns: ProjectedBlock,
        row_count: usize,
    ) -> usize {
        // exceed capacity or should resize
        if row_count + self.hash_index.count > self.hash_index.resize_threshold() {
            self.resize(self.hash_index.capacity * 2);
        }

        self.hash_index
            .probe_and_create(state, row_count, AdapterImpl {
                payload: &mut self.payload,
                group_columns,
            })
    }

    pub fn combine(&mut self, other: Self, flush_state: &mut PayloadFlushState) -> Result<()> {
        self.combine_payloads(&other.payload, flush_state)
    }

    pub fn combine_payloads(
        &mut self,
        payloads: &PartitionedPayload,
        flush_state: &mut PayloadFlushState,
    ) -> Result<()> {
        for payload in payloads.payloads.iter() {
            self.combine_payload(payload, flush_state)?;
        }
        Ok(())
    }

    pub fn combine_payload(
        &mut self,
        payload: &Payload,
        flush_state: &mut PayloadFlushState,
    ) -> Result<()> {
        flush_state.clear();

        while payload.flush(flush_state) {
            let row_count = flush_state.row_count;

            let _ = self.probe_and_create(
                &mut flush_state.probe_state,
                (&flush_state.group_columns).into(),
                row_count,
            );

            // set state places
            if !self.payload.aggrs.is_empty() {
                for i in 0..row_count {
                    flush_state.probe_state.state_places[i] =
                        flush_state.probe_state.addresses[i].state_addr(&self.payload.row_layout);
                }
            }

            let state = &mut flush_state.probe_state;
            let places = &state.state_places.as_slice()[0..row_count];
            let rhses = &flush_state.state_places.as_slice()[0..row_count];
            if let Some(layout) = self.payload.row_layout.states_layout.as_ref() {
                for (aggr, loc) in self.payload.aggrs.iter().zip(layout.states_loc.iter()) {
                    aggr.batch_merge_states(places, rhses, loc)?;
                }
            }
        }

        Ok(())
    }

    pub fn merge_result(&mut self, flush_state: &mut PayloadFlushState) -> Result<bool> {
        if !self.payload.flush(flush_state) {
            return Ok(false);
        }

        let row_count = flush_state.row_count;
        flush_state.aggregate_results.clear();
        if let Some(states_layout) = self.payload.row_layout.states_layout.as_ref() {
            for (aggr, loc) in self
                .payload
                .aggrs
                .iter()
                .zip(states_layout.states_loc.iter().cloned())
            {
                let return_type = aggr.return_type()?;
                let mut builder = ColumnBuilder::with_capacity(&return_type, row_count * 4);

                aggr.batch_merge_result(
                    &flush_state.state_places.as_slice()[0..row_count],
                    loc,
                    &mut builder,
                )?;
                flush_state.aggregate_results.push(builder.build().into());
            }
        }
        Ok(true)
    }

    fn maybe_repartition(&mut self) -> bool {
        // already final stage or the max radix bits
        if !PARTIAL || (self.current_radix_bits == self.config.max_radix_bits) {
            return false;
        }

        let bytes_per_partition = self.payload.memory_size() / self.payload.partition_count();

        let mut new_radix_bits = self.current_radix_bits;

        if bytes_per_partition > MAX_PAGE_SIZE * self.config.block_fill_factor as usize {
            new_radix_bits += self.config.repartition_radix_bits_incr;
        }

        loop {
            let current_max_radix_bits = self.config.current_max_radix_bits.load(Ordering::SeqCst);
            if current_max_radix_bits < new_radix_bits
                && self
                    .config
                    .current_max_radix_bits
                    .compare_exchange(
                        current_max_radix_bits,
                        new_radix_bits,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_err()
            {
                continue;
            }
            break;
        }

        let current_max_radix_bits = self.config.current_max_radix_bits.load(Ordering::SeqCst);

        if current_max_radix_bits > self.current_radix_bits {
            let temp_payload = PartitionedPayload::new(
                self.payload.group_types.clone(),
                self.payload.aggrs.clone(),
                1,
                vec![Arc::new(Bump::new())],
            );
            let payload = std::mem::replace(&mut self.payload, temp_payload);
            let mut state = PayloadFlushState::default();

            self.current_radix_bits = current_max_radix_bits;
            self.payload = payload.repartition(1 << current_max_radix_bits, &mut state);
            return true;
        }
        false
    }

    // scan payload to reconstruct PointArray
    fn resize(&mut self, new_capacity: usize) {
        if PARTIAL {
            if self.hash_index.capacity == self.config.max_partial_capacity {
                return;
            }
            self.hash_index = HashIndex::with_capacity(new_capacity);
            return;
        }

        let mut hash_index = HashIndex::with_capacity(new_capacity);

        // iterate over payloads and copy to new entries
        for payload in self.payload.payloads.iter() {
            for page in payload.pages.iter() {
                for idx in 0..page.rows {
                    let row_ptr = page.data_ptr(idx, payload.tuple_size);
                    let hash = row_ptr.hash(&payload.row_layout);

                    let slot = hash_index.probe_slot(hash);

                    // set value
                    let entry = hash_index.mut_entry(slot);
                    debug_assert!(!entry.is_occupied());
                    entry.set_hash(hash);
                    entry.set_pointer(row_ptr);

                    debug_assert!(entry.is_occupied());
                    debug_assert_eq!(entry.get_pointer(), row_ptr);
                    debug_assert_eq!(entry.get_salt(), Entry::hash_to_salt(hash));

                    hash_index.count += 1;
                }
            }
        }

        self.hash_index = hash_index
    }

    fn initial_capacity() -> usize {
        8192 * 4
    }

    pub fn get_capacity_for_count(count: usize) -> usize {
        ((count.max(Self::initial_capacity()) as f64 * LOAD_FACTOR) as usize).next_power_of_two()
    }

    fn clear_ht(&mut self) {
        self.payload.mark_min_cardinality();
        self.hash_index.reset();
    }

    pub fn allocated_bytes(&self) -> usize {
        self.payload.memory_size()
            + self
                .payload
                .arenas
                .iter()
                .map(|arena| arena.allocated_bytes())
                .sum::<usize>()
            + self.hash_index.allocated_bytes()
    }
}
