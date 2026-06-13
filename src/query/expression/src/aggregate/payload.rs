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

use std::mem::MaybeUninit;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_base::runtime::drop_guard;
use log::info;
use strength_reduce::StrengthReducedU64;

use super::AggrState;
use super::AggregateFunctionRef;
use super::BATCH_SIZE;
use super::MAX_PAGE_SIZE;
use super::PayloadFlushState;
use super::RowID;
use super::StateAddr;
use super::StatesLayout;
use super::payload_row::rowformat_size;
use super::payload_row::serialize_column_to_rowformat;
use super::payload_row::serialize_const_column_to_rowformat;
use super::probe_state::SelectVector;
use super::row_ptr::RowLayout;
use super::row_ptr::RowMut;
use super::row_ptr::RowPtr;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::ProjectedBlock;
use crate::types::DataType;

// payload layout
// [VALIDITY][GROUPS][HASH][STATE_ADDRS]
// [VALIDITY] is the validity bits of the data columns (including the HASH)
// [GROUPS] is the group data, could be multiple values, fixed size, strings are elsewhere
// [HASH] is the hash data of the groups
// [STATE_ADDRS] is the state_addrs of the aggregate functions, 8 bytes each
pub struct Payload {
    pub(super) arena: Arc<Bump>,
    pub(super) group_types: Vec<DataType>,
    pub(super) aggrs: Vec<AggregateFunctionRef>,
    pub(super) row_layout: RowLayout,

    pub(super) pages: Vec<Page>,
    pub(super) tuple_size: usize,
    row_per_page: usize,

    total_rows: usize,

    // Starts from 1, zero means no page allocated
    current_write_page: usize,

    // if set, the payload contains at least duplicate rows
    min_cardinality: Option<usize>,
}

pub(super) struct PayloadTransferBatch<'a> {
    rows: usize,
    addresses: &'a [RowPtr; BATCH_SIZE],
    partitions: &'a [(u16, SelectVector)],
}

#[derive(Default)]
pub(super) struct PayloadTransferStateOffsets {
    offsets: Vec<(usize, usize, usize)>,
}

impl PayloadTransferStateOffsets {
    fn record(&mut self, payload_index: usize, offsets: Vec<(usize, usize)>) {
        self.offsets.extend(
            offsets
                .into_iter()
                .map(|(page_index, offset)| (payload_index, page_index, offset)),
        );
    }

    fn commit(self, payloads: &mut [Payload]) {
        for (payload_index, page_index, offset) in self.offsets {
            let payload = payloads
                .get_mut(payload_index)
                .expect("transfer target payload index is out of bounds");

            let page = payload
                .pages
                .get_mut(page_index)
                .expect("transfer target page index is out of bounds");
            let next_state_offsets = page
                .state_offsets
                .checked_add(offset)
                .expect("transfer target state offsets overflow");
            let initialized_states = page
                .rows
                .checked_mul(payload.aggrs.len())
                .expect("transfer target initialized states overflow");

            assert!(
                next_state_offsets <= initialized_states,
                "transfer target state offsets exceed initialized aggregate states"
            );
            page.state_offsets = next_state_offsets;
        }
    }
}

impl<'a> PayloadTransferBatch<'a> {
    pub(super) fn from_flush_state(flush_state: &'a PayloadFlushState) -> Self {
        Self {
            rows: flush_state.row_count,
            addresses: &flush_state.addresses,
            partitions: &flush_state.probe_state.partition_entries,
        }
    }

    pub(super) fn copy_all_partitions_to(
        &self,
        payloads: &mut [Payload],
        state_offsets: &mut PayloadTransferStateOffsets,
    ) {
        debug_assert_eq!(payloads.len(), self.partitions.len());
        debug_assert_eq!(
            self.rows,
            self.partitions
                .iter()
                .map(|(count, _)| *count as usize)
                .sum::<usize>()
        );

        for (payload_index, (payload, (count, sel))) in
            payloads.iter_mut().zip(self.partitions).enumerate()
        {
            if *count > 0 {
                let offsets = payload
                    .copy_state_addr_rows_for_transfer(&sel[..*count as usize], self.addresses);
                state_offsets.record(payload_index, offsets);
            }
        }
    }
}

unsafe impl Send for Payload {}
unsafe impl Sync for Payload {}

pub(super) struct Page {
    // RowRef values into this buffer may be stored in hash indexes and flush
    // state. A page must not reallocate after such row refs are published.
    data: Vec<MaybeUninit<u8>>,
    pub(super) rows: usize,
    // state_offset = state_rows * agg_len
    // which mark that the offset to clean the agg states
    state_offsets: usize,
    capacity: usize,
}

impl Page {
    fn is_partial_state(&self, agg_len: usize) -> bool {
        self.rows * agg_len != self.state_offsets
    }

    pub(super) fn row_ptr(&self, row: usize, row_size: usize) -> RowPtr {
        debug_assert!(row < self.rows);
        debug_assert_eq!(self.data.len(), self.rows * row_size);
        RowPtr::new(unsafe { self.data.as_ptr().add(row * row_size) as *const u8 })
    }

    fn reserve_row(&mut self, row_size: usize) -> RowMut {
        debug_assert!(self.rows < self.capacity);
        let row_offset = self.data.len();
        unsafe {
            self.data.set_len(row_offset + row_size);
        }
        self.rows += 1;
        let ptr = unsafe { self.data.as_mut_ptr().add(row_offset) as *mut u8 };
        RowMut::new(ptr)
    }
}

impl Payload {
    pub fn new(
        arena: Arc<Bump>,
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
        states_layout: Option<StatesLayout>,
    ) -> Self {
        let mut tuple_size = 0;
        let mut validity_offsets = Vec::with_capacity(group_types.len());
        for x in group_types.iter() {
            if x.is_nullable() {
                validity_offsets.push(tuple_size);
                tuple_size += 1;
            } else {
                validity_offsets.push(0);
            }
        }

        let mut group_offsets = Vec::with_capacity(group_types.len());
        let mut group_sizes = Vec::with_capacity(group_types.len());

        for x in group_types.iter() {
            group_offsets.push(tuple_size);
            let size = rowformat_size(x);
            group_sizes.push(size);
            tuple_size += size;
        }

        let hash_offset = tuple_size;
        tuple_size += size_of::<u64>();

        let state_offset = tuple_size;
        if !aggrs.is_empty() {
            tuple_size += size_of::<StateAddr>();
        }

        let row_per_page = (u16::MAX as usize).min(MAX_PAGE_SIZE / tuple_size).max(1);

        Self {
            arena,
            pages: vec![],
            current_write_page: 0,
            group_types,
            aggrs,
            tuple_size,
            row_per_page,
            min_cardinality: None,
            total_rows: 0,
            row_layout: RowLayout {
                hash_offset,
                state_offset,
                validity_offsets,
                group_offsets,
                group_sizes,
                states_layout,
            },
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.total_rows
    }

    #[inline]
    pub fn memory_size(&self) -> usize {
        self.total_rows * self.tuple_size
    }

    pub(super) fn commit_transferred_state_offsets(
        mut self,
        state_offsets: PayloadTransferStateOffsets,
        payloads: &mut [Payload],
    ) {
        // Rows are shallow-copied and still point at the same aggregate states.
        // The source must give up state-drop ownership before target offsets
        // make those states visible to target payload drops.
        self.row_layout.states_layout = None;
        state_offsets.commit(payloads);
    }

    #[inline]
    pub(super) fn writable_page(&mut self) -> (&mut Page, usize) {
        if self.current_write_page == 0
            || self.pages[self.current_write_page - 1].rows
                == self.pages[self.current_write_page - 1].capacity
        {
            self.current_write_page += 1;
            if self.current_write_page > self.pages.len() {
                self.pages.push(Page {
                    data: Vec::with_capacity(self.row_per_page * self.tuple_size),
                    rows: 0,
                    state_offsets: 0,
                    capacity: self.row_per_page,
                });
            }
        }
        let page_index = self.current_write_page - 1;
        (&mut self.pages[page_index], page_index)
    }

    pub(super) fn data_ptr(&self, page: &Page, row: usize) -> RowPtr {
        page.row_ptr(row, self.tuple_size)
    }

    pub(super) fn reserve_append_rows(
        &mut self,
        select_vector: &[RowID],
        group_hashes: &[u64; BATCH_SIZE],
        address: &mut [RowPtr; BATCH_SIZE],
        page_index: &mut [usize],
        group_columns: ProjectedBlock,
    ) {
        let tuple_size = self.tuple_size;
        let (mut page, mut page_index_value) = self.writable_page();
        let address = unsafe {
            // SAFETY: RowRef and RowMut are repr(transparent) wrappers over raw
            // u8 pointers. Raw pointer mutability does not change layout, and
            // row_ptr.rs has compile-time size/alignment assertions for these
            // two wrappers.
            std::mem::transmute::<&mut [RowPtr; BATCH_SIZE], &mut [RowMut; BATCH_SIZE]>(address)
        };
        for row in select_vector {
            let row_mut = page.reserve_row(tuple_size);
            address[*row] = row_mut;
            page_index[*row] = page_index_value;

            if page.rows == page.capacity {
                (page, page_index_value) = self.writable_page();
            }
        }

        self.append_rows(
            select_vector,
            group_hashes,
            address,
            page_index,
            group_columns,
        )
    }

    fn append_rows(
        &mut self,
        select_vector: &[RowID],
        group_hashes: &[u64; BATCH_SIZE],
        address: &mut [RowMut; BATCH_SIZE],
        page_index: &mut [usize],
        group_columns: ProjectedBlock,
    ) {
        let mut write_offset = 0;
        // write validity
        for entry in group_columns.iter() {
            match entry {
                BlockEntry::Const(scalar, DataType::Nullable(_), _) => {
                    let val = if scalar.is_null() { 0 } else { 1 };
                    for row in select_vector {
                        unsafe {
                            address[*row].write_u8(write_offset, val);
                        }
                    }
                    write_offset += 1;
                }
                BlockEntry::Column(Column::Nullable(box c)) => {
                    let bitmap = c.validity();
                    if bitmap.null_count() == 0 || bitmap.null_count() == bitmap.len() {
                        let val: u8 = if bitmap.null_count() == 0 { 1 } else { 0 };
                        // faster path
                        for row in select_vector {
                            unsafe {
                                address[*row].write_u8(write_offset, val);
                            }
                        }
                    } else {
                        for row in select_vector {
                            unsafe {
                                address[*row]
                                    .write_u8(write_offset, bitmap.get_bit(row.to_usize()) as u8);
                            }
                        }
                    }
                    write_offset += 1;
                }
                _ => (),
            }
        }

        let mut scratch = vec![];
        for (idx, entry) in group_columns.iter().enumerate() {
            let offset = self.row_layout.group_offsets[idx];
            assert!(write_offset == offset);

            match entry {
                BlockEntry::Const(scalar, data_type, _) => unsafe {
                    serialize_const_column_to_rowformat(
                        &self.arena,
                        scalar,
                        data_type,
                        select_vector,
                        address,
                        offset,
                        &mut scratch,
                    )
                },
                BlockEntry::Column(column) => unsafe {
                    serialize_column_to_rowformat(
                        &self.arena,
                        column,
                        select_vector,
                        address,
                        offset,
                        &mut scratch,
                    );
                },
            }

            write_offset += self.row_layout.group_sizes[idx];
        }

        // write group hashes
        debug_assert!(write_offset == self.row_layout.hash_offset);
        for row in select_vector {
            address[*row].set_hash(&self.row_layout, group_hashes[*row]);
        }

        debug_assert!(write_offset + 8 == self.row_layout.state_offset);
        let states = self.row_layout.states_layout.as_ref().map(|states_layout| {
            (
                states_layout.layout.repeat(select_vector.len()).unwrap(),
                states_layout.states_loc.clone(),
                self.aggrs.clone(),
            )
        });

        if let Some(((array_layout, padded_size), states_loc, aggrs)) = states {
            // write states
            // Bump only allocates but does not drop, so there is no use after free for any item.
            let place = self.arena.alloc_layout(array_layout);
            for (row, place) in select_vector
                .iter()
                .copied()
                .enumerate()
                .map(|(i, row)| (row, unsafe { place.add(padded_size * i) }))
            {
                let place = StateAddr::from(place);
                address[row].set_state_addr(&self.row_layout, &place);
                let page = &mut self.pages[page_index[row]];
                for (aggr, loc) in aggrs.iter().zip(states_loc.iter()) {
                    aggr.init_state(AggrState::new(place, loc));
                    page.state_offsets += 1;
                }
            }

            #[cfg(debug_assertions)]
            {
                for page in self.pages.iter() {
                    assert_eq!(page.rows * self.aggrs.len(), page.state_offsets);
                }
            }
        }

        self.total_rows += select_vector.len();

        debug_assert_eq!(
            self.total_rows,
            self.pages.iter().map(|x| x.rows).sum::<usize>()
        );
    }

    pub(super) fn combine(&mut self, mut other: Payload) {
        debug_assert_eq!(
            other.total_rows,
            other.pages.iter().map(|x| x.rows).sum::<usize>()
        );

        self.total_rows += other.total_rows;
        self.pages.append(other.pages.as_mut());
    }

    pub(super) fn mark_min_cardinality(&mut self) {
        if self.min_cardinality.is_none() {
            self.min_cardinality = Some(self.total_rows);
        }
    }

    fn copy_state_addr_rows_for_transfer(
        &mut self,
        select_vector: &[RowID],
        row_refs: &[RowPtr; BATCH_SIZE],
    ) -> Vec<(usize, usize)> {
        let tuple_size = self.tuple_size;
        let agg_len = self.aggrs.len();
        let (mut page, mut page_index) = self.writable_page();
        let mut state_offsets = Vec::new();
        let mut page_state_offset = 0;

        for index in select_vector {
            let mut row_mut = page.reserve_row(tuple_size);
            unsafe {
                std::ptr::copy_nonoverlapping(
                    row_refs[*index].as_ptr(),
                    row_mut.as_mut_ptr(),
                    tuple_size,
                )
            }
            page_state_offset += agg_len;

            if page.rows == page.capacity {
                if page_state_offset > 0 {
                    state_offsets.push((page_index, page_state_offset));
                }
                (page, page_index) = self.writable_page();
                page_state_offset = 0;
            }
        }
        if page_state_offset > 0 {
            state_offsets.push((page_index, page_state_offset));
        }

        self.total_rows += select_vector.len();

        debug_assert_eq!(
            self.total_rows,
            self.pages.iter().map(|x| x.rows).sum::<usize>()
        );

        state_offsets
    }

    pub(super) fn scan_hash_partition_transfer<'a>(
        &self,
        flush_state: &'a mut PayloadFlushState,
        partition_count: usize,
    ) -> Option<PayloadTransferBatch<'a>> {
        debug_assert!(partition_count > 0);

        loop {
            let page = self.pages.get(flush_state.flush_page)?;

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
                state.reset_partitions(partition_count);

                let mods: StrengthReducedU64 = StrengthReducedU64::new(partition_count as u64);

                for (idx, row_ptr) in flush_state.addresses[..rows].iter_mut().enumerate() {
                    *row_ptr = self.data_ptr(page, idx + flush_state.flush_page_row);
                    let hash = row_ptr.hash(&self.row_layout);
                    let partition_idx = (hash % mods) as usize;

                    let (count, sel) = &mut state.partition_entries[partition_idx];
                    sel[*count as usize] = idx.into();
                    *count += 1;
                }
            }

            flush_state.flush_page_row = end;
            return Some(PayloadTransferBatch::from_flush_state(flush_state));
        }
    }

    pub fn scatter_into_buckets(mut self, buckets: usize) -> Vec<Payload> {
        if buckets == 0 {
            return vec![];
        }

        let mut bucket_payloads = (0..buckets)
            .map(|_| {
                Payload::new(
                    self.arena.clone(),
                    self.group_types.clone(),
                    self.aggrs.clone(),
                    self.row_layout.states_layout.clone(),
                )
            })
            .collect::<Vec<_>>();

        let mut state = PayloadFlushState::default();
        self.transfer_hash_partitioned_rows(&mut bucket_payloads, &mut state);
        bucket_payloads
    }

    pub(super) fn transfer_hash_partitioned_rows(
        mut self,
        target_payloads: &mut [Payload],
        flush_state: &mut PayloadFlushState,
    ) {
        if self.len() == 0 || target_payloads.is_empty() {
            return;
        }

        flush_state.clear();
        let mut state_offsets = PayloadTransferStateOffsets::default();
        while let Some(batch) =
            self.scan_hash_partition_transfer(flush_state, target_payloads.len())
        {
            batch.copy_all_partitions_to(target_payloads, &mut state_offsets);
        }

        self.commit_transferred_state_offsets(state_offsets, target_payloads);
    }

    pub fn empty_block(&self, fake_rows: usize) -> DataBlock {
        assert_eq!(
            self.aggrs.is_empty(),
            self.row_layout.states_layout.is_none()
        );
        let entries = self
            .row_layout
            .states_layout
            .as_ref()
            .iter()
            .flat_map(|layout| layout.serialize_type.iter())
            .map(|serde_type| {
                ColumnBuilder::repeat_default(&serde_type.data_type(), fake_rows)
                    .build()
                    .into()
            })
            .chain(
                self.group_types
                    .iter()
                    .map(|t| ColumnBuilder::repeat_default(t, fake_rows).build().into()),
            )
            .collect();
        DataBlock::new(entries, fake_rows)
    }
}

impl Drop for Payload {
    fn drop(&mut self) {
        drop_guard(move || {
            let Some(states_layout) = self.row_layout.states_layout.as_ref() else {
                return;
            };

            'FOR: for (idx, (aggr, loc)) in self
                .aggrs
                .iter()
                .zip(states_layout.states_loc.iter())
                .enumerate()
            {
                if !aggr.need_manual_drop_state() {
                    continue;
                }

                for page in self.pages.iter() {
                    let is_partial_state = page.is_partial_state(self.aggrs.len());

                    if is_partial_state && idx == 0 {
                        info!(
                            "Cleaning partial page, state_offsets: {}, row: {}, agg length: {}",
                            page.state_offsets,
                            page.rows,
                            self.aggrs.len()
                        );
                    }
                    for row in 0..page.state_offsets.div_ceil(self.aggrs.len()) {
                        // When OOM, some states are not initialized, we don't need to destroy them
                        if is_partial_state && row * self.aggrs.len() + idx >= page.state_offsets {
                            continue 'FOR;
                        }
                        let addr = self.data_ptr(page, row).state_addr(&self.row_layout);
                        unsafe {
                            aggr.drop_state(AggrState::new(addr, loc));
                        }
                    }
                }
            }
        })
    }
}
