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

use super::BATCH_SIZE;
use super::StateAddr;
use super::StatesLayout;
use super::aggregate_function::AggregateCallRef;
use super::aggregate_function::get_states_layout;
use super::payload::GroupRows;
use super::payload::Payload;
use super::probe_state::ProbeState;
use super::row_ptr::RowIds;
use super::row_ptr::RowLayout;
use super::row_ptr::RowPtr;
use crate::types::DataType;

/// Maps a group hash to a partition of a table: `partition_count` partitions by the bits below
/// bit 48 that start `start_bit` bits down. The final aggregate takes the next bits at each spill
/// depth, so that a spilled partition splits further.
#[derive(Debug, Clone, Copy)]
struct PartitionMask {
    mask: u64,
    shift: u64,
}

impl PartitionMask {
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
    pub(super) aggrs: Vec<AggregateCallRef>,

    pub(super) row_layout: RowLayout,

    pub(super) arenas: Vec<Arc<Bump>>,

    partition_mask: PartitionMask,
    /// The ids of the rows in the hash index.
    pub(super) row_ids: RowIds,
}

unsafe impl Send for PartitionedPayload {}
unsafe impl Sync for PartitionedPayload {}

impl PartitionedPayload {
    /// Calls `f` with the group hash of each row.
    pub(super) fn for_each_hash(&self, mut f: impl FnMut(u64)) {
        for payload in &self.payloads {
            payload.for_each_hash(&mut f);
        }
    }

    pub(super) fn new_with_start_bit(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateCallRef>,
        partition_count: u64,
        partition_start_bit: u64,
        arenas: Vec<Arc<Bump>>,
    ) -> Self {
        assert!(
            arenas.len() == 1 || arenas.len() == partition_count as usize,
            "arenas must be shared or match the payload partition count"
        );
        let states_layout = if !aggrs.is_empty() {
            Some(get_states_layout(&aggrs).unwrap())
        } else {
            None
        };

        let payloads = (0..partition_count)
            .map(|partition| {
                Payload::new(
                    arenas[if arenas.len() == 1 {
                        0
                    } else {
                        partition as usize
                    }]
                    .clone(),
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

        let row_ids = RowIds::new(
            partition_count as usize,
            payloads[0].row_per_page,
            payloads[0].tuple_size,
            payloads[0].row_layout.states,
        );
        PartitionedPayload {
            payloads,
            group_types,
            aggrs,
            row_layout,
            row_ids,

            arenas,
            partition_mask: PartitionMask::with_start_bit(partition_count, partition_start_bit),
        }
    }

    /// The row with id `id`.
    #[inline]
    pub(super) fn row_ptr(&self, id: u64) -> RowPtr {
        self.row_ids.row_ptr(id)
    }

    /// Whether the ids of the rows still fit 4 bytes after `new_rows` more rows, however they
    /// spread over the partitions: each partition may open one more page than its rows fill.
    pub(super) fn row_ids_fit_compact(&self, new_rows: usize) -> bool {
        let pages = self.payloads.iter().map(|p| p.pages.len()).sum::<usize>();
        let new_pages = new_rows.div_ceil(self.payloads[0].row_per_page) + self.payloads.len();
        pages + new_pages <= self.row_ids.max_compact_pages()
    }

    /// Number the pages again from 0, for an index rebuilt from the rows.
    pub(super) fn renumber_pages(&mut self) {
        self.row_ids
            .renumber(self.payloads.iter().map(|p| p.pages.as_slice()));
    }

    pub fn into_bucket_payloads(self) -> impl Iterator<Item = (usize, Payload)> {
        self.payloads.into_iter().enumerate()
    }

    pub fn into_non_empty_bucket_payloads(self) -> impl Iterator<Item = (usize, Payload)> {
        self.into_bucket_payloads()
            .filter(|(_, payload)| payload.len() != 0)
    }

    pub(super) fn append_rows(
        &mut self,
        state: &mut ProbeState,
        new_group_rows: usize,
        groups: GroupRows,
        source_states: Option<&[StateAddr; BATCH_SIZE]>,
    ) {
        if self.payloads.len() == 1 {
            self.payloads[0].reserve_append_rows(
                &state.empty_vector[..new_group_rows],
                &state.group_hashes,
                &mut state.addresses,
                &mut state.page_index,
                (&mut state.row_ids, &mut self.row_ids, 0),
                groups,
                source_states,
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

            for (partition, (payload, (count, sel))) in self
                .payloads
                .iter_mut()
                .zip(state.partition_entries.iter_mut())
                .enumerate()
            {
                if *count > 0 {
                    payload.reserve_append_rows(
                        &sel[..*count as _],
                        &state.group_hashes,
                        &mut state.addresses,
                        &mut state.page_index,
                        (&mut state.row_ids, &mut self.row_ids, partition),
                        groups,
                        source_states,
                    );
                }
            }
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
}

#[cfg(test)]
mod tests {
    use super::PartitionMask;

    #[test]
    fn test_partition_mask_with_start_bit() {
        let top_bit_mask = PartitionMask::with_start_bit(2, 0);
        assert_eq!(top_bit_mask.index(1_u64 << 47), 1);
        assert_eq!(top_bit_mask.index(1_u64 << 44), 0);

        let shifted_mask = PartitionMask::with_start_bit(2, 3);
        assert_eq!(shifted_mask.index(1_u64 << 47), 0);
        assert_eq!(shifted_mask.index(1_u64 << 44), 1);
    }
}
