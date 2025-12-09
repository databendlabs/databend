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

use std::sync::Arc;

use bumpalo::Bump;
use itertools::Itertools;

use super::payload::Payload;
use super::probe_state::ProbeState;
use super::row_ptr::RowLayout;
use crate::get_states_layout;
use crate::types::DataType;
use crate::AggregateFunctionRef;
use crate::PayloadFlushState;
use crate::ProjectedBlock;
use crate::StatesLayout;
use crate::BATCH_SIZE;

pub struct PartitionedPayload {
    pub payloads: Vec<Payload>,
    pub group_types: Vec<DataType>,
    pub aggrs: Vec<AggregateFunctionRef>,

    pub(super) row_layout: RowLayout,

    pub arenas: Vec<Arc<Bump>>,

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
        arenas: Vec<Arc<Bump>>,
    ) -> Self {
        let radix_bits = partition_count.trailing_zeros() as u64;
        debug_assert_eq!(1 << radix_bits, partition_count);

        let states_layout = if !aggrs.is_empty() {
            Some(get_states_layout(&aggrs).unwrap())
        } else {
            None
        };

        let payloads = (0..partition_count)
            .map(|_| {
                Payload::new(
                    arenas[0].clone(),
                    group_types.clone(),
                    aggrs.clone(),
                    states_layout.clone(),
                )
            })
            .collect_vec();

        let offsets = RowLayout {
            states_layout,
            ..payloads[0].row_layout.clone()
        };

        PartitionedPayload {
            payloads,
            group_types,
            aggrs,
            row_layout: offsets,
            partition_count,

            arenas,
            mask_v: mask(radix_bits),
            shift_v: shift(radix_bits),
        }
    }

    pub fn states_layout(&self) -> Option<&StatesLayout> {
        self.row_layout.states_layout.as_ref()
    }

    pub fn mark_min_cardinality(&mut self) {
        for payload in self.payloads.iter_mut() {
            payload.mark_min_cardinality();
        }
    }

    pub fn append_rows(
        &mut self,
        state: &mut ProbeState,
        new_group_rows: usize,
        group_columns: ProjectedBlock,
    ) {
        if self.payloads.len() == 1 {
            self.payloads[0].reserve_append_rows(
                &state.empty_vector[..new_group_rows],
                &state.group_hashes,
                &mut state.addresses,
                &mut state.page_index,
                group_columns,
            );
        } else {
            // generate partition selection indices
            state.reset_partitions(self.partition_count());
            for &row in &state.empty_vector[..new_group_rows] {
                let hash = state.group_hashes[row];
                let partition_idx = ((hash & self.mask_v) >> self.shift_v) as usize;
                let (count, sel) = &mut state.partition_entries[partition_idx];

                sel[*count] = row;
                *count += 1;
            }

            for (payload, (count, sel)) in self
                .payloads
                .iter_mut()
                .zip(state.partition_entries.iter_mut())
            {
                if *count > 0 {
                    payload.reserve_append_rows(
                        &sel[..*count],
                        &state.group_hashes,
                        &mut state.addresses,
                        &mut state.page_index,
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
            self.arenas.clone(),
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
                self.combine_single(payload, state, None)
            }
        }
    }

    pub fn combine_single(
        &mut self,
        mut other: Payload,
        flush_state: &mut PayloadFlushState,
        only_bucket: Option<usize>,
    ) {
        if other.len() == 0 {
            return;
        }

        if self.partition_count == 1 {
            self.payloads[0].combine(other);
        } else {
            flush_state.clear();

            // flush for other's each page to correct partition
            while self.gather_flush(&other, flush_state) {
                // copy rows
                let state = &*flush_state.probe_state;

                for partition in (0..self.partition_count as usize)
                    .filter(|x| only_bucket.is_none() || only_bucket == Some(*x))
                {
                    let (count, sel) = &state.partition_entries[partition];
                    if *count > 0 {
                        let payload = &mut self.payloads[partition];
                        payload.copy_rows(&sel[..*count], &flush_state.addresses);
                    }
                }
            }
            other.state_move_out = true;
        }
    }

    // for each page's row, compute which partition it belongs to
    pub fn gather_flush(&self, other: &Payload, flush_state: &mut PayloadFlushState) -> bool {
        if flush_state.flush_page >= other.pages.len() {
            return false;
        }

        let page = &other.pages[flush_state.flush_page];

        // ToNext
        if flush_state.flush_page_row >= page.rows {
            flush_state.flush_page += 1;
            flush_state.flush_page_row = 0;
            flush_state.row_count = 0;
            return self.gather_flush(other, flush_state);
        }

        let end = (flush_state.flush_page_row + BATCH_SIZE).min(page.rows);
        let rows = end - flush_state.flush_page_row;
        flush_state.row_count = rows;

        let state = &mut *flush_state.probe_state;
        state.reset_partitions(self.partition_count());

        for idx in 0..rows {
            let row_ptr = other.data_ptr(page, idx + flush_state.flush_page_row);
            flush_state.addresses[idx] = row_ptr;

            let hash = row_ptr.hash(&self.row_layout);
            let partition_idx = ((hash & self.mask_v) >> self.shift_v) as usize;

            let (count, sel) = &mut state.partition_entries[partition_idx];
            sel[*count] = idx;
            *count += 1;
        }
        flush_state.flush_page_row = end;
        true
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.payloads.iter().map(|x| x.len()).sum()
    }

    #[inline]
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
