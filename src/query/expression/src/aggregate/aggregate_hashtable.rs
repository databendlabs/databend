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

use std::sync::Arc;

use databend_common_exception::Result;

use super::payload::Payload;
use super::payload_flush::PayloadFlushState;
use super::probe_state::ProbeState;
use crate::aggregate::payload_row::row_match_columns;
use crate::group_hash_columns;
use crate::load;
use crate::select_vector::SelectVector;
use crate::types::DataType;
use crate::AggregateFunctionRef;
use crate::Column;
use crate::ColumnBuilder;
use crate::StateAddr;

const LOAD_FACTOR: f64 = 1.5;
// hashes layout:
// [SALT][PAGE_NR][PAGE_OFFSET]
// [SALT] are the high bits of the hash value, e.g. 16 for 64 bit hashes
// [PAGE_NR] is the buffer managed payload page index
// [PAGE_OFFSET] is the logical entry offset into said payload page

#[repr(packed)]
#[derive(Default, Debug, Clone, Copy)]
pub struct Entry {
    pub salt: u16,
    pub page_offset: u16,
    pub page_nr: u32,
}

pub struct AggregateHashTable {
    payload: Payload,
    entries: Vec<Entry>,
    capacity: usize,
}

impl AggregateHashTable {
    pub fn new(
        arena: Arc<bumpalo::Bump>,
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
    ) -> Self {
        let capacity = 128;
        Self {
            entries: Self::new_entries(capacity),
            payload: Payload::new(arena, group_types, aggrs),
            capacity,
        }
    }

    // Faster way to create entries
    // We don't need to extend N zero elements using u64 after we allocate zero spaces
    // due to IsZero Trait(https://stdrs.dev/nightly/x86_64-unknown-linux-gnu/src/alloc/vec/spec_from_elem.rs.html#24)
    fn new_entries(capacity: usize) -> Vec<Entry> {
        let entries = vec![0u64; capacity];
        let (ptr, len, cap) = entries.into_raw_parts();
        unsafe { Vec::from_raw_parts(ptr as *mut Entry, len, cap) }
    }

    fn len(&self) -> usize {
        self.payload.len()
    }

    // Add new groups and combine the states
    pub fn add_groups(
        &mut self,
        state: &mut ProbeState,
        group_columns: &[Column],
        params: &[Vec<Column>],
        row_count: usize,
    ) -> Result<usize> {
        let group_hashes = group_hash_columns(group_columns);
        let new_group_count = self.probe_and_create(state, group_columns, row_count, &group_hashes);

        for i in 0..row_count {
            state.state_places[i] = unsafe {
                StateAddr::new(
                    load::<u64>(state.addresses[i].add(self.payload.state_offset)) as usize,
                )
            };
        }

        for ((aggr, params), addr_offset) in self
            .payload
            .aggrs
            .iter()
            .zip(params.iter())
            .zip(self.payload.state_addr_offsets.iter())
        {
            aggr.accumulate_keys(
                &state.state_places.as_slice()[0..row_count],
                *addr_offset,
                params,
                row_count,
            )?;
        }
        Ok(new_group_count)
    }

    fn probe_and_create(
        &mut self,
        state: &mut ProbeState,
        group_columns: &[Column],
        row_count: usize,
        hashes: &[u64],
    ) -> usize {
        if self.capacity - self.len() <= row_count || self.len() > self.resize_threshold() {
            let mut new_capacity = self.capacity * 2;

            while new_capacity - self.len() <= row_count {
                new_capacity *= 2;
            }
            self.resize(new_capacity);
        }

        state.adjust_group_columns(group_columns, hashes, row_count, self.capacity);

        let mut new_group_count = 0;
        let mut remaining_entries = row_count;

        let mut select_vector = SelectVector::auto_increment();

        let mut payload_page_offset = self.len() % self.payload.row_per_page;
        let mut payload_page_nr = (self.len() / self.payload.row_per_page) + 1;

        while remaining_entries > 0 {
            let mut new_entry_count = 0;
            let mut need_compare_count = 0;
            let mut no_match_count = 0;

            // 1. inject new_group_count, new_entry_count, need_compare_count, no_match_count
            for i in 0..remaining_entries {
                let index = select_vector.get_index(i);
                let entry = &mut self.entries[state.ht_offsets[index]];

                // cell is empty, could be occupied
                if entry.page_nr == 0 {
                    entry.salt = state.hash_salts[index];
                    entry.page_nr = payload_page_nr as u32;
                    entry.page_offset = payload_page_offset as u16;

                    payload_page_offset += 1;
                    if payload_page_offset == self.payload.row_per_page {
                        payload_page_offset = 0;
                        payload_page_nr += 1;
                    }

                    state.empty_vector.set_index(new_entry_count, index);
                    state.new_groups.set_index(new_group_count, index);
                    new_entry_count += 1;
                    new_group_count += 1;
                } else if entry.salt == state.hash_salts[index] {
                    state
                        .group_compare_vector
                        .set_index(need_compare_count, index);
                    need_compare_count += 1;
                } else {
                    state.no_match_vector.set_index(no_match_count, index);
                    no_match_count += 1;
                }
            }

            // 2. append new_group_count to payload
            if new_entry_count != 0 {
                self.payload.append_rows(
                    state,
                    hashes,
                    &select_vector,
                    new_entry_count,
                    group_columns,
                );
            }

            // 3. handle need_compare_count
            for need_compare_idx in 0..need_compare_count {
                let index = state.group_compare_vector.get_index(need_compare_idx);
                let entry = &mut self.entries[state.ht_offsets[index]];

                let page_ptr = self.payload.get_page_ptr((entry.page_nr - 1) as usize);
                let page_offset = entry.page_offset as usize * self.payload.tuple_size;

                state.addresses[index] = unsafe { page_ptr.add(page_offset) };
            }

            // 4. compare
            unsafe {
                row_match_columns(
                    group_columns,
                    &state.addresses,
                    &mut state.group_compare_vector,
                    need_compare_count,
                    &self.payload.validity_offsets,
                    &self.payload.group_offsets,
                    &mut state.no_match_vector,
                    &mut no_match_count,
                );
            }

            // 5. Linear probing
            for i in 0..no_match_count {
                let index = state.no_match_vector.get_index(i);
                state.ht_offsets[index] += 1;

                if state.ht_offsets[index] >= self.capacity {
                    state.ht_offsets[index] = 0;
                }
            }

            std::mem::swap(&mut select_vector, &mut state.no_match_vector);
            state.no_match_vector.resize(no_match_count);

            remaining_entries = no_match_count;
        }

        // set state places
        for i in 0..row_count {
            state.state_places[i] = unsafe {
                StateAddr::new(
                    load::<u64>(state.addresses[i].add(self.payload.state_offset)) as usize,
                )
            };
        }

        new_group_count
    }

    pub fn combine(&mut self, other: Self, flush_state: &mut PayloadFlushState) -> Result<()> {
        while other.payload.flush(flush_state) {
            let row_count = flush_state.row_count;

            let _ = self.probe_and_create(
                &mut flush_state.probe_state,
                &flush_state.group_columns,
                row_count,
                &flush_state.group_hashes,
            );

            let state = &mut flush_state.probe_state;
            for (aggr, addr_offset) in self
                .payload
                .aggrs
                .iter()
                .zip(self.payload.state_addr_offsets.iter())
            {
                aggr.batch_merge_states(
                    &state.state_places.as_slice()[0..row_count],
                    &flush_state.state_places.as_slice()[0..row_count],
                    *addr_offset,
                )?;
            }
        }
        Ok(())
    }

    pub fn merge_result(&mut self, flush_state: &mut PayloadFlushState) -> Result<bool> {
        if self.payload.flush(flush_state) {
            let row_count = flush_state.row_count;

            flush_state.aggregate_results.clear();
            for (aggr, addr_offset) in self
                .payload
                .aggrs
                .iter()
                .zip(self.payload.state_addr_offsets.iter())
            {
                let return_type = aggr.return_type()?;
                let mut builder = ColumnBuilder::with_capacity(&return_type, row_count * 4);

                aggr.batch_merge_result(
                    &flush_state.state_places.as_slice()[0..row_count],
                    *addr_offset,
                    &mut builder,
                )?;
                flush_state.aggregate_results.push(builder.build());
            }
            return Ok(true);
        }
        Ok(false)
    }

    fn resize_threshold(&self) -> usize {
        (self.capacity as f64 / LOAD_FACTOR) as usize
    }

    pub fn resize(&mut self, new_capacity: usize) {
        let mask = (new_capacity - 1) as u64;

        let mut entries = Self::new_entries(new_capacity);
        // iterate over payloads and copy to new entries
        for row in 0..self.len() {
            let row_ptr = self.payload.get_row_ptr(row);
            let hash: u64 = unsafe { load(row_ptr.add(self.payload.hash_offset)) };
            let mut hash_slot = hash & mask;

            while entries[hash_slot as usize].page_nr != 0 {
                hash_slot += 1;
                if hash_slot >= self.capacity as u64 {
                    hash_slot = 0;
                }
            }
            let entry = &mut entries[hash_slot as usize];

            entry.page_nr = (row / self.payload.row_per_page) as u32 + 1;
            entry.page_offset = (row % self.payload.row_per_page) as u16;
            entry.salt = (hash >> (64 - 16)) as u16;
        }

        self.entries = entries;
        self.capacity = new_capacity;
    }
}
