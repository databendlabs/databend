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
use std::alloc::alloc;
use std::alloc::dealloc;
use std::alloc::handle_alloc_error;
use std::ptr::NonNull;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_base::runtime::drop_guard;
use databend_common_exception::Result;
use log::info;

use super::AggrState;
use super::BATCH_SIZE;
use super::MAX_PAGE_SIZE;
use super::PayloadFlushState;
use super::RowID;
use super::StateAddr;
use super::StatesLayout;
use super::aggregate_function::AggregateCallRef;
use super::group_hash_route_index;
use super::payload_row::rowformat_size;
use super::payload_row::serialize_column_to_rowformat;
use super::payload_row::serialize_const_column_to_rowformat;
use super::probe_state::SelectVector;
use super::row_ptr::RowIdCodec;
use super::row_ptr::RowLayout;
use super::row_ptr::RowMut;
use super::row_ptr::RowPtr;
use super::row_ptr::StatesRegion;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::ProjectedBlock;
use crate::types::DataType;

// payload layout
// A page holds its rows, then the aggregate states of its rows in the same order; see
// `StatesRegion`. A row is
// [VALIDITY][GROUPS][HASH]
// [VALIDITY] is the validity bits of the data columns (including the HASH)
// [GROUPS] is the group data, could be multiple values, fixed size, strings are elsewhere
// [HASH] is the hash data of the groups
//
// The states of a row are found from its address, so moving rows between payloads moves their
// states bitwise: a state must never keep a pointer to itself.
pub struct Payload {
    pub(super) arena: Arc<Bump>,
    pub(super) group_types: Vec<DataType>,
    pub(super) aggrs: Vec<AggregateCallRef>,
    pub(super) row_layout: RowLayout,

    pub(super) pages: Vec<Page>,
    pub(super) tuple_size: usize,
    pub(super) row_per_page: usize,
    page_bytes: usize,

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
    data: PageData,
    pub(super) rows: usize,
    // state_offset = state_rows * agg_len
    // which mark that the offset to clean the agg states
    state_offsets: usize,
    capacity: usize,
}

/// The bytes of a page, aligned to `StatesRegion::page_align`.
struct PageData {
    ptr: NonNull<u8>,
    layout: Layout,
    len: usize,
}

impl PageData {
    fn with_capacity(capacity: usize, align: usize) -> Self {
        let layout = Layout::from_size_align(capacity, align).expect("invalid page layout");
        let ptr = match layout.size() {
            0 => NonNull::new(align as *mut u8).expect("alignment is never zero"),
            _ => {
                NonNull::new(unsafe { alloc(layout) }).unwrap_or_else(|| handle_alloc_error(layout))
            }
        };
        PageData {
            ptr,
            layout,
            len: 0,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    unsafe fn set_len(&mut self, len: usize) {
        debug_assert!(len <= self.layout.size());
        self.len = len;
    }

    fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }
}

impl Drop for PageData {
    fn drop(&mut self) {
        if self.layout.size() != 0 {
            unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
        }
    }
}

impl Page {
    fn is_partial_state(&self, agg_len: usize) -> bool {
        self.rows * agg_len != self.state_offsets
    }

    pub(super) fn row_ptr(&self, row: usize, row_size: usize) -> RowPtr {
        debug_assert!(row < self.rows);
        debug_assert_eq!(self.data.len(), self.rows * row_size);
        RowPtr::new(unsafe { self.data.as_ptr().add(row * row_size) })
    }

    fn reserve_row(&mut self, row_size: usize) -> RowMut {
        debug_assert!(self.rows < self.capacity);
        let row_offset = self.data.len();
        unsafe {
            self.data.set_len(row_offset + row_size);
        }
        self.rows += 1;
        let ptr = unsafe { self.data.as_mut_ptr().add(row_offset) };
        RowMut::new(ptr)
    }
}

impl Payload {
    pub fn new(
        arena: Arc<Bump>,
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateCallRef>,
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

        let (state_size, state_align) = match &states_layout {
            Some(states_layout) if !aggrs.is_empty() => (
                states_layout.layout.pad_to_align().size(),
                states_layout.layout.align(),
            ),
            _ => (0, 1),
        };
        let row_per_page = (u16::MAX as usize)
            .min(MAX_PAGE_SIZE.saturating_sub(state_align) / (tuple_size + state_size))
            .max(1);
        let states_offset = (row_per_page * tuple_size).next_multiple_of(state_align);
        let page_bytes = states_offset + row_per_page * state_size;
        let states = StatesRegion::new(
            page_bytes.next_power_of_two(),
            tuple_size,
            states_offset,
            state_size,
        );

        Self {
            arena,
            pages: vec![],
            current_write_page: 0,
            group_types,
            aggrs,
            tuple_size,
            row_per_page,
            page_bytes,
            min_cardinality: None,
            total_rows: 0,
            row_layout: RowLayout {
                hash_offset,
                states,
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
        self.total_rows * (self.tuple_size + self.row_layout.states.size)
    }

    pub fn merge_result(&self, flush_state: &mut PayloadFlushState) -> Result<Option<DataBlock>> {
        if !self.flush(flush_state) {
            return Ok(None);
        }

        let row_count = flush_state.row_count;
        flush_state.aggregate_results.clear();
        if let Some(states_layout) = self.row_layout.states_layout.as_ref() {
            for (aggr, loc) in self
                .aggrs
                .iter()
                .zip(states_layout.states_loc.iter().cloned())
            {
                let return_type = aggr.signature().return_type.clone();
                let mut builder = ColumnBuilder::with_capacity(&return_type, row_count * 4);
                for place in &flush_state.state_places.as_slice()[0..row_count] {
                    aggr.merge_result(AggrState::new(*place, &loc), &mut builder)?;
                }
                flush_state.aggregate_results.push(builder.build().into());
            }
        }

        let mut entries = flush_state.take_aggregate_results();
        entries.extend(flush_state.take_group_columns());
        Ok(Some(DataBlock::new(entries, row_count)))
    }

    pub(super) fn commit_transferred_state_offsets(
        mut self,
        state_offsets: PayloadTransferStateOffsets,
        payloads: &mut [Payload],
    ) {
        // Rows are copied with their aggregate states, so the states move to the targets.
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
                    data: PageData::with_capacity(
                        self.page_bytes,
                        self.row_layout.states.page_align(),
                    ),
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
        row_ids: (&mut [u64; BATCH_SIZE], RowIdCodec, usize),
        group_columns: ProjectedBlock,
    ) {
        let (row_ids, codec, partition) = row_ids;
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
            row_ids[*row] = codec.encode(partition, page_index_value, page.rows - 1);

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
                BlockEntry::Column(Column::Nullable(deref!(c))) => {
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

        if let Some(states_layout) = self.row_layout.states_layout.as_ref() {
            // write states, in the states region of the page
            for row in select_vector {
                let place = StateAddr::from(
                    self.row_layout
                        .states
                        .ptr(address[*row].as_mut_ptr().cast_const()),
                );
                let page = &mut self.pages[page_index[*row]];
                for (aggr, loc) in self.aggrs.iter().zip(states_layout.states_loc.iter()) {
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

    /// Move the pages into payloads of at most `max_rows` rows and `max_bytes` bytes each,
    /// without copying rows; a page over a limit makes a payload of its own. The payloads share
    /// the arena of the states, which is freed with the last of them.
    pub fn split_pages(mut self, max_rows: usize, max_bytes: usize) -> Vec<Payload> {
        if self.total_rows <= max_rows && self.memory_size() <= max_bytes {
            return vec![self];
        }

        let mut payloads = vec![];
        let mut current = self.empty_like();
        for page in std::mem::take(&mut self.pages) {
            if page.rows == 0 {
                continue;
            }
            let rows = current.total_rows + page.rows;
            let row_bytes = self.tuple_size + self.row_layout.states.size;
            if current.total_rows != 0 && (rows > max_rows || rows * row_bytes > max_bytes) {
                payloads.push(std::mem::replace(&mut current, self.empty_like()));
            }
            current.total_rows += page.rows;
            current.pages.push(page);
            current.current_write_page = current.pages.len();
        }
        if current.total_rows != 0 {
            payloads.push(current);
        }
        self.total_rows = 0;
        self.current_write_page = 0;
        payloads
    }

    fn empty_like(&self) -> Payload {
        Payload::new(
            self.arena.clone(),
            self.group_types.clone(),
            self.aggrs.clone(),
            self.row_layout.states_layout.clone(),
        )
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
        let states = self.row_layout.states;
        let agg_len = self.aggrs.len();
        let (mut page, mut page_index) = self.writable_page();
        let mut state_offsets = Vec::new();
        let mut page_state_offset = 0;

        for index in select_vector {
            let mut row_mut = page.reserve_row(tuple_size);
            unsafe {
                let source = row_refs[*index].as_ptr();
                std::ptr::copy_nonoverlapping(source, row_mut.as_mut_ptr(), tuple_size);
                std::ptr::copy_nonoverlapping(
                    states.ptr(source),
                    states.ptr(row_mut.as_mut_ptr().cast_const()),
                    states.size,
                );
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

                for (idx, row_ptr) in flush_state.addresses[..rows].iter_mut().enumerate() {
                    *row_ptr = self.data_ptr(page, idx + flush_state.flush_page_row);
                    let hash = row_ptr.hash(&self.row_layout);
                    let partition_idx =
                        group_hash_route_index(hash, partition_count as u64) as usize;

                    let (count, sel) = &mut state.partition_entries[partition_idx];
                    sel[*count as usize] = idx.into();
                    *count += 1;
                }
            }

            flush_state.flush_page_row = end;
            return Some(PayloadTransferBatch::from_flush_state(flush_state));
        }
    }

    /// Split the rows into `buckets` payloads by `group_hash_route_index` of their group hash, the
    /// routing of raw rows with the same group keys.
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
                if !aggr.state().need_manual_drop() {
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
