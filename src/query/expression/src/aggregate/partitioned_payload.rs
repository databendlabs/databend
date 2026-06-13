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

use super::AggregateFunctionRef;
use super::BATCH_SIZE;
use super::PayloadFlushState;
use super::StatesLayout;
use super::get_states_layout;
use super::payload::Payload;
use super::payload::PayloadTransferBatch;
use super::payload::PayloadTransferStateOffsets;
use super::probe_state::ProbeState;
use super::row_ptr::RowLayout;
use crate::ProjectedBlock;
use crate::types::DataType;

#[derive(Debug, Clone, Copy)]
struct PartitionMask {
    mask: u64,
    shift: u64,
}

impl PartitionMask {
    fn new(partition_count: u64) -> Self {
        Self::with_start_bit(partition_count, 0)
    }

    fn with_start_bit(partition_count: u64, start_bit: u64) -> Self {
        let radix_bits = partition_count.trailing_zeros() as u64;
        debug_assert_eq!(1 << radix_bits, partition_count);
        debug_assert!(start_bit + radix_bits <= 48);

        let shift = 48 - start_bit - radix_bits;
        let mask = ((1 << radix_bits) - 1) << shift;

        Self { mask, shift }
    }

    fn index(&self, hash: u64) -> usize {
        ((hash & self.mask) >> self.shift) as _
    }
}

pub struct PartitionedPayload {
    pub(super) payloads: Vec<Payload>,
    pub(super) group_types: Vec<DataType>,
    pub(super) aggrs: Vec<AggregateFunctionRef>,

    pub(super) row_layout: RowLayout,

    pub(super) arenas: Vec<Arc<Bump>>,

    partition_start_bit: u64,
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
        Self::new_with_start_bit(group_types, aggrs, partition_count, 0, arenas)
    }

    pub(super) fn new_with_start_bit(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
        partition_count: u64,
        partition_start_bit: u64,
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
            partition_start_bit,
            partition_mask: PartitionMask::with_start_bit(partition_count, partition_start_bit),
        }
    }

    pub fn into_bucket_payloads(self) -> impl Iterator<Item = (usize, Payload)> {
        self.payloads.into_iter().enumerate()
    }

    pub fn into_non_empty_bucket_payloads(self) -> impl Iterator<Item = (usize, Payload)> {
        self.into_bucket_payloads()
            .filter(|(_, payload)| payload.len() != 0)
    }

    pub(super) fn into_single_payload(mut self) -> Payload {
        assert_eq!(
            self.partition_count(),
            1,
            "partitioned payload must contain exactly one payload"
        );
        self.payloads.pop().unwrap()
    }

    pub(super) fn mark_min_cardinality(&mut self) {
        for payload in self.payloads.iter_mut() {
            payload.mark_min_cardinality();
        }
    }

    pub(super) fn append_rows(
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
            partition_start_bit,
            ..
        } = self;

        let mut new_partition_payload = PartitionedPayload::new_with_start_bit(
            group_types,
            aggrs,
            new_partition_count as u64,
            partition_start_bit,
            arenas,
        );

        state.clear();
        for payload in payloads {
            new_partition_payload.combine_single(payload, state)
        }

        new_partition_payload
    }

    pub(super) fn combine_single(&mut self, other: Payload, flush_state: &mut PayloadFlushState) {
        if other.len() == 0 {
            return;
        }

        if self.partition_count() == 1 {
            self.payloads[0].combine(other);
        } else {
            flush_state.clear();

            let mut state_offsets = PayloadTransferStateOffsets::default();
            // flush for other's each page to correct partition
            while let Some(batch) = self.scan_partition_transfer(&other, flush_state) {
                batch.copy_all_partitions_to(&mut self.payloads, &mut state_offsets);
            }

            other.commit_transferred_state_offsets(state_offsets, &mut self.payloads);
        }
    }

    fn scan_partition_transfer<'a>(
        &self,
        other: &Payload,
        flush_state: &'a mut PayloadFlushState,
    ) -> Option<PayloadTransferBatch<'a>> {
        loop {
            let page = other.pages.get(flush_state.flush_page)?;

            if flush_state.flush_page_row >= page.rows {
                flush_state.flush_page += 1;
                flush_state.flush_page_row = 0;
                flush_state.row_count = 0;
                continue;
            }

            let end = (flush_state.flush_page_row + BATCH_SIZE).min(page.rows);
            let rows = end - flush_state.flush_page_row;
            flush_state.row_count = rows;

            {
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
            }

            flush_state.flush_page_row = end;
            return Some(PayloadTransferBatch::from_flush_state(flush_state));
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.payloads.iter().map(|x| x.len()).sum()
    }

    #[inline]
    pub fn partition_count(&self) -> usize {
        self.payloads.len()
    }

    pub fn memory_size(&self) -> usize {
        self.payloads.iter().map(|x| x.memory_size()).sum()
    }

    pub fn scatter_into_buckets(self, buckets: usize) -> Vec<PartitionedPayload> {
        let group_types = self.group_types.clone();
        let aggrs = self.aggrs.clone();
        let partition_count = self.partition_count() as u64;
        let arenas = self.arenas.clone();

        let mut bucket_payloads = Vec::with_capacity(buckets);
        for _ in 0..buckets {
            bucket_payloads.push(PartitionedPayload::new(
                group_types.clone(),
                aggrs.clone(),
                partition_count,
                arenas.clone(),
            ));
        }

        let mut state = PayloadFlushState::default();
        let mut payloads = (0..buckets)
            .map(|_| {
                Payload::new(
                    Arc::new(Bump::new()),
                    group_types.clone(),
                    aggrs.clone(),
                    self.row_layout.states_layout.clone(),
                )
            })
            .collect_vec();

        for payload in self.payloads {
            payload.transfer_hash_partitioned_rows(&mut payloads, &mut state);
        }

        for (bucket, payload) in payloads.into_iter().enumerate() {
            bucket_payloads[bucket].combine_single(payload, &mut state);
        }

        bucket_payloads
    }
}

#[cfg(test)]
mod tests {
    use super::PartitionMask;

    #[test]
    fn test_partition_mask_with_start_bit() {
        let top_bit_mask = PartitionMask::new(2);
        assert_eq!(top_bit_mask.index(1_u64 << 47), 1);
        assert_eq!(top_bit_mask.index(1_u64 << 44), 0);

        let shifted_mask = PartitionMask::with_start_bit(2, 3);
        assert_eq!(shifted_mask.index(1_u64 << 47), 0);
        assert_eq!(shifted_mask.index(1_u64 << 44), 1);
    }
}
