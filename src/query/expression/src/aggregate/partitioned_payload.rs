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
use crate::AggregateFunctionRef;
use crate::BATCH_SIZE;
use crate::PayloadFlushState;
use crate::ProjectedBlock;
use crate::StatesLayout;
use crate::get_states_layout;
use crate::types::DataType;

#[derive(Debug, Clone, Copy)]
struct PartitionMask {
    mask: u64,
    shift: u64,
}

impl PartitionMask {
    fn new(partition_count: u64) -> Self {
        let radix_bits = partition_count.trailing_zeros() as u64;
        debug_assert_eq!(1 << radix_bits, partition_count);

        let shift = 48 - radix_bits;
        let mask = ((1 << radix_bits) - 1) << shift;

        Self { mask, shift }
    }

    pub fn index(&self, hash: u64) -> usize {
        ((hash & self.mask) >> self.shift) as _
    }
}

pub struct PartitionedPayload {
    pub payloads: Vec<Payload>,
    pub group_types: Vec<DataType>,
    pub aggrs: Vec<AggregateFunctionRef>,

    pub(super) row_layout: RowLayout,

    pub arenas: Vec<Arc<Bump>>,

    partition_mask: PartitionMask,
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

        let row_layout = RowLayout {
            states_layout,
            ..payloads[0].row_layout.clone()
        };

        PartitionedPayload {
            payloads,
            group_types,
            aggrs,
            row_layout,

            arenas,
            partition_mask: PartitionMask::new(partition_count),
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
                let partition_idx = self.partition_mask.index(hash);
                let (count, sel) = &mut state.partition_entries[partition_idx];

                sel[*count as usize] = row;
                *count += 1;
            }

            for (payload, (count, sel)) in self
                .payloads
                .iter_mut()
                .zip(state.partition_entries.iter_mut())
            {
                if *count > 0 {
                    payload.reserve_append_rows(
                        &sel[..*count as _],
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

        let PartitionedPayload {
            payloads,
            group_types,
            aggrs,
            arenas,
            ..
        } = self;

        let mut new_partition_payload =
            PartitionedPayload::new(group_types, aggrs, new_partition_count as u64, arenas);

        state.clear();
        for payload in payloads.into_iter() {
            new_partition_payload.combine_single(payload, state, None)
        }

        new_partition_payload
    }

    pub fn combine(&mut self, other: PartitionedPayload, state: &mut PayloadFlushState) {
        if other.partition_count() == self.partition_count() {
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

        if self.partition_count() == 1 {
            self.payloads[0].combine(other);
        } else {
            flush_state.clear();

            // flush for other's each page to correct partition
            while self.gather_flush(&other, flush_state) {
                // copy rows
                let state = &*flush_state.probe_state;

                match only_bucket {
                    Some(i) => {
                        let (count, sel) = &state.partition_entries[i];
                        self.payloads[i].copy_rows(&sel[..*count as _], &flush_state.addresses);
                    }
                    None => {
                        for ((count, sel), payload) in
                            state.partition_entries.iter().zip(self.payloads.iter_mut())
                        {
                            if *count > 0 {
                                payload.copy_rows(&sel[..*count as _], &flush_state.addresses);
                            }
                        }
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
            let partition_idx = self.partition_mask.index(hash);

            let (count, sel) = &mut state.partition_entries[partition_idx];
            sel[*count as usize] = idx.into();
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
        self.payloads.len()
    }

    pub fn page_count(&self) -> usize {
        self.payloads.iter().map(|x| x.pages.len()).sum()
    }

    pub fn memory_size(&self) -> usize {
        self.payloads.iter().map(|x| x.memory_size()).sum()
    }
}
