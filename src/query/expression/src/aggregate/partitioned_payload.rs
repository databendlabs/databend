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
use super::MAX_PAGE_SIZE;
use super::StateAddr;
use super::StatesLayout;
use super::aggregate_function::AggregateCallRef;
use super::aggregate_function::get_states_layout;
use super::group_hash_route_index;

/// Smallest pages of a table routed to the final aggregates, see `new_routed`.
const MIN_ROUTED_PAGE_SIZE: usize = 16 * 1024;
use super::payload::GroupRows;
use super::payload::Payload;
use super::payload::RowCursor;
use super::probe_state::ProbeState;
use super::row_ptr::PageNumbering;
use super::row_ptr::RowDecoder;
use super::row_ptr::RowIds;
use super::row_ptr::RowLayout;
use super::row_ptr::RowPtr;
use crate::types::DataType;

/// Maps a group hash to a partition of a table.
#[derive(Debug, Clone, Copy)]
pub(super) enum PartitionMask {
    /// `partition_count` partitions by the bits below bit 48 that start `start_bit` bits down.
    /// The final aggregate takes the next bits at each spill depth, so that a spilled partition
    /// splits further.
    Radix { mask: u64, shift: u64 },
    /// The final aggregate a row is routed to, see `group_hash_route_index`: a partial table
    /// split this way hands each partition to its final aggregate without copying the rows.
    Route(u64),
}

impl PartitionMask {
    fn with_start_bit(partition_count: u64, start_bit: u64) -> Self {
        let radix_bits = partition_count.trailing_zeros() as u64;
        debug_assert_eq!(1 << radix_bits, partition_count);
        debug_assert!(start_bit + radix_bits <= 48);

        let shift = 48 - start_bit - radix_bits;
        let mask = ((1 << radix_bits) - 1) << shift;

        Self::Radix { mask, shift }
    }

    #[inline]
    fn index(&self, hash: u64) -> usize {
        match *self {
            Self::Radix { mask, shift } => ((hash & mask) >> shift) as _,
            Self::Route(partitions) => group_hash_route_index(hash, partitions) as _,
        }
    }
}

/// Reserves the rows of new groups while a probe claims their slots, so that a slot takes the
/// id of its row at once. A partition whose page is full opens the next one, and numbers it
/// in the entries `PartitionedPayload::begin_rows` reserved.
pub(in crate::aggregate) struct RowReserver<'a> {
    payloads: &'a mut [Payload],
    cursors: &'a mut [RowCursor],
    numbering: PageNumbering<'a>,
    partition_mask: PartitionMask,
}

impl<'a> RowReserver<'a> {
    /// A reserver for a table of one partition.
    #[cfg(test)]
    pub(in crate::aggregate) fn single(
        payloads: &'a mut [Payload],
        cursors: &'a mut [RowCursor],
        numbering: PageNumbering<'a>,
    ) -> Self {
        RowReserver {
            payloads,
            cursors,
            numbering,
            partition_mask: PartitionMask::with_start_bit(1, 0),
        }
    }

    /// The id of a new row for a group of hash `hash`, and the index of its page in its
    /// partition.
    #[inline]
    pub(in crate::aggregate) fn reserve(&mut self, hash: u64) -> (u64, usize) {
        let partition = self.partition_mask.index(hash);
        if self.cursors[partition].id == self.cursors[partition].end {
            self.open_page(partition);
        }
        let cursor = &mut self.cursors[partition];
        let id = cursor.id;
        cursor.id += 1;
        (id, cursor.page)
    }

    #[cold]
    #[inline(never)]
    fn open_page(&mut self, partition: usize) {
        let payload = &mut self.payloads[partition];
        payload.commit_cursor(&self.cursors[partition]);
        self.cursors[partition] = payload.open_cursor(&mut self.numbering, partition);
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
    /// The rows a probe reserves in each partition, see `RowReserver`.
    cursors: Vec<RowCursor>,
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
        let mask = PartitionMask::with_start_bit(partition_count, partition_start_bit);
        Self::with_mask(
            group_types,
            aggrs,
            partition_count,
            mask,
            arenas,
            MAX_PAGE_SIZE,
        )
    }

    /// A payload of `partitions` partitions, one per final aggregate its rows are routed to.
    pub(super) fn new_routed(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateCallRef>,
        partitions: u64,
        arenas: Vec<Arc<Bump>>,
    ) -> Self {
        let page_size = (MAX_PAGE_SIZE / partitions as usize).max(MIN_ROUTED_PAGE_SIZE);
        let mask = PartitionMask::Route(partitions);
        Self::with_mask(group_types, aggrs, partitions, mask, arenas, page_size)
    }

    fn with_mask(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateCallRef>,
        partition_count: u64,
        partition_mask: PartitionMask,
        arenas: Vec<Arc<Bump>>,
        page_size: usize,
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
                    page_size,
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
            cursors: vec![RowCursor::EMPTY; partition_count as usize],

            arenas,
            partition_mask,
        }
    }

    /// The row with id `id`.
    #[inline]
    pub(super) fn row_ptr(&self, id: u64) -> RowPtr {
        self.row_ids.row_ptr(id)
    }

    /// The pages after `new_rows` more rows, however they spread over the partitions: each
    /// partition may open one more page than its rows fill.
    fn pages_after(&self, new_rows: usize) -> usize {
        let pages = self.payloads.iter().map(|p| p.pages.len()).sum::<usize>();
        pages + new_rows.div_ceil(self.payloads[0].row_per_page) + self.payloads.len()
    }

    /// The largest id a row may take after `new_rows` more rows.
    pub(super) fn max_row_id(&self, new_rows: usize) -> u64 {
        self.row_ids.max_row_id(self.pages_after(new_rows))
    }

    /// Number the pages again from 0, for an index rebuilt from the rows.
    pub(super) fn renumber_pages(&mut self) {
        self.row_ids
            .renumber(self.payloads.iter().map(|p| p.pages.as_slice()));
    }

    pub fn into_bucket_payloads(self) -> impl Iterator<Item = (usize, Payload)> {
        let routed = match self.partition_mask {
            PartitionMask::Route(partitions) => partitions as usize,
            PartitionMask::Radix { .. } => 0,
        };
        self.payloads
            .into_iter()
            .map(move |mut payload| {
                payload.routed_partitions = routed;
                payload
            })
            .enumerate()
    }

    pub fn into_non_empty_bucket_payloads(self) -> impl Iterator<Item = (usize, Payload)> {
        self.into_bucket_payloads()
            .filter(|(_, payload)| payload.len() != 0)
    }

    /// Start reserving rows for a probe of `row_count` rows: from the free rows of the writable
    /// page of each partition, and room to number the pages the probe may open.
    pub(super) fn begin_rows(&mut self, row_count: usize) {
        self.row_ids.reserve_pages(self.pages_after(row_count));
        let mut numbering = self.row_ids.numbering();
        for (partition, (payload, cursor)) in self
            .payloads
            .iter_mut()
            .zip(self.cursors.iter_mut())
            .enumerate()
        {
            *cursor = payload.current_cursor(&mut numbering, partition);
        }
    }

    /// Decodes the ids of the rows, and reserves new rows.
    #[inline]
    pub(super) fn rows(&mut self) -> (RowDecoder<'_>, RowReserver<'_>) {
        let (rows, numbering) = self.row_ids.split();
        (rows, RowReserver {
            payloads: &mut self.payloads,
            cursors: &mut self.cursors,
            numbering,
            partition_mask: self.partition_mask,
        })
    }

    /// Write the rows the probe reserved for the groups of the first `new_group_rows` rows of
    /// `state.empty_vector`.
    pub(super) fn append_rows(
        &mut self,
        state: &mut ProbeState,
        new_group_rows: usize,
        groups: GroupRows,
        source_states: Option<&[StateAddr; BATCH_SIZE]>,
    ) {
        for (payload, cursor) in self.payloads.iter_mut().zip(&self.cursors) {
            payload.commit_cursor(cursor);
        }
        if self.payloads.len() == 1 {
            self.payloads[0].write_rows(
                &state.empty_vector[..new_group_rows],
                &state.group_hashes,
                &mut state.addresses,
                &mut state.page_index,
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

            for (payload, (count, sel)) in self
                .payloads
                .iter_mut()
                .zip(state.partition_entries.iter_mut())
            {
                if *count > 0 {
                    payload.write_rows(
                        &sel[..*count as _],
                        &state.group_hashes,
                        &mut state.addresses,
                        &mut state.page_index,
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
