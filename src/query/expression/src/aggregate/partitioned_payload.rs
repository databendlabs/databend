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

use std::alloc::Layout;
use std::sync::Arc;

use bumpalo::Bump;
use itertools::Itertools;

use super::payload::Payload;
use super::probe_state::ProbeState;
use crate::types::DataType;
use crate::AggregateFunctionRef;
use crate::Column;
use crate::PayloadFlushState;
use crate::FLUSH_BATCH_SIZE;

pub struct PartitionedPayload {
    pub payloads: Vec<Payload>,
    pub group_types: Vec<DataType>,
    pub aggrs: Vec<AggregateFunctionRef>,

    pub group_sizes: Vec<usize>,
    pub group_offsets: Vec<usize>,
    pub validity_offsets: Vec<usize>,
    pub hash_offset: usize,
    pub state_offset: usize,
    pub state_addr_offsets: Vec<usize>,
    pub state_layout: Option<Layout>,

    partition_count: u64,
    mask_v: u64,
    shift_v: u64,
}

unsafe impl Send for PartitionedPayload {}
unsafe impl Sync for PartitionedPayload {}

impl PartitionedPayload {
    pub fn new(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
        partition_count: u64,
    ) -> Self {
        let radix_bits = partition_count.trailing_zeros() as u64;
        debug_assert_eq!(1 << radix_bits, partition_count);

        let payloads = (0..partition_count)
            .map(|_| Payload::new(Arc::new(Bump::new()), group_types.clone(), aggrs.clone()))
            .collect_vec();

        let group_sizes = payloads[0].group_sizes.clone();
        let group_offsets = payloads[0].group_offsets.clone();
        let validity_offsets = payloads[0].validity_offsets.clone();
        let hash_offset = payloads[0].hash_offset;
        let state_offset = payloads[0].state_offset;
        let state_addr_offsets = payloads[0].state_addr_offsets.clone();
        let state_layout = payloads[0].state_layout.clone();

        PartitionedPayload {
            payloads,
            group_types,
            aggrs,
            group_sizes,
            group_offsets,
            validity_offsets,
            hash_offset,
            state_offset,
            state_addr_offsets,
            state_layout,
            partition_count,
            mask_v: mask(radix_bits),
            shift_v: shift(radix_bits),
        }
    }

    pub fn append_rows(
        &mut self,
        state: &mut ProbeState,
        new_group_rows: usize,
        group_columns: &[Column],
    ) {
        if self.payloads.len() == 1 {
            self.payloads[0].reserve_append_rows(
                &state.empty_vector,
                &state.group_hashes,
                &mut state.addresses,
                new_group_rows,
                group_columns,
            );
        } else {
            // generate partition selection indices
            state.reset_partitions();
            let select_vector = &state.empty_vector;

            for idx in select_vector.iter().take(new_group_rows).copied() {
                let hash = state.group_hashes[idx];
                let partition_idx = ((hash & self.mask_v) >> self.shift_v) as usize;
                match state.partition_entries.get_mut(&partition_idx) {
                    Some((v, count)) => {
                        v[*count] = idx;
                        *count += 1;
                    }
                    None => {
                        let mut v = vec![0; state.group_hashes.len()];
                        v[0] = idx;
                        state.partition_entries.insert(partition_idx, (v, 1));
                    }
                }
            }

            for partition_index in 0..self.payloads.len() {
                if let Some((select_vector, count)) =
                    state.partition_entries.get_mut(&partition_index)
                {
                    self.payloads[partition_index].reserve_append_rows(
                        select_vector,
                        &state.group_hashes,
                        &mut state.addresses,
                        *count,
                        group_columns,
                    );
                }
            }
        }
    }

    pub fn repartition(self, new_partition_count: usize, state: &mut PayloadFlushState) -> Self {
        if self.partition_count() == new_partition_count {
            return self;
        }

        let mut new_partition_payload = PartitionedPayload::new(
            self.group_types.clone(),
            self.aggrs.clone(),
            new_partition_count as u64,
        );

        new_partition_payload.combine(self, state);
        new_partition_payload
    }

    pub fn combine(&mut self, other: PartitionedPayload, state: &mut PayloadFlushState) {
        if other.partition_count == self.partition_count {
            for (l, r) in self.payloads.iter_mut().zip(other.payloads.into_iter()) {
                l.combine(r);
            }
        } else {
            state.clear();

            for payload in other.payloads.into_iter() {
                self.combine_single(payload, state)
            }
        }
    }

    pub fn combine_single(&mut self, other: Payload, state: &mut PayloadFlushState) {
        if other.len() == 0 {
            return;
        }

        if self.partition_count == 1 {
            self.payloads[0].combine(other);
        } else {
            state.clear();

            while self.gather_flush(&other, state) {
                // copy rows
                for partition in 0..self.partition_count as usize {
                    let payload = &mut self.payloads[partition];
                    if let Some(sel) = &state.probe_state.partition_entries.get_mut(&partition) {
                        payload.copy_rows(&sel.0, sel.1, &state.addresses);

                        payload.external_arena.push(other.arena.clone());
                        payload
                            .external_arena
                            .extend_from_slice(&other.external_arena);
                    }
                }
            }
            other.forget();
        }
    }

    pub fn gather_flush(&self, other: &Payload, state: &mut PayloadFlushState) -> bool {
        let flush_end = (state.flush_offset + FLUSH_BATCH_SIZE).min(other.len());

        if flush_end <= state.flush_offset {
            return false;
        }

        let rows = flush_end - state.flush_offset;
        if state.addresses.len() < rows {
            state.addresses.resize(rows, std::ptr::null::<u8>());
        }

        state.row_count = rows;
        for row in state.flush_offset..flush_end {
            state.addresses[row - state.flush_offset] = other.get_read_ptr(row);
        }

        state.probe_state.reset_partitions();
        for i in 0..rows {
            let hash =
                unsafe { core::ptr::read::<u64>(state.addresses[i].add(self.hash_offset) as _) };

            let partition_idx = ((hash & self.mask_v) >> self.shift_v) as usize;
            match state.probe_state.partition_entries.get_mut(&partition_idx) {
                Some((v, count)) => {
                    v[*count] = i;
                    *count += 1;
                }
                None => {
                    let mut v = vec![0; FLUSH_BATCH_SIZE];
                    v[0] = i;
                    state
                        .probe_state
                        .partition_entries
                        .insert(partition_idx, (v, 1));
                }
            }
        }

        state.flush_offset = flush_end;
        true
    }

    pub fn len(&self) -> usize {
        self.payloads.iter().map(|x| x.len()).sum()
    }

    pub fn partition_count(&self) -> usize {
        self.partition_count as usize
    }

    #[allow(dead_code)]
    pub fn page_count(&self) -> usize {
        self.payloads.iter().map(|x| x.pages.len()).sum()
    }

    #[allow(dead_code)]
    pub fn memory_size(&self) -> usize {
        self.payloads.iter().map(|x| x.memory_size()).sum()
    }
}

#[inline]
fn shift(radix_bits: u64) -> u64 {
    48 - radix_bits
}

#[inline]
fn mask(radix_bits: u64) -> u64 {
    ((1 << radix_bits) - 1) << shift(radix_bits)
}
