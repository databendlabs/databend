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

use super::payload_row::rowformat_size;
use super::payload_row::serialize_column_to_rowformat;
use super::row_ptr::RowLayout;
use super::row_ptr::RowPtr;
use super::RowID;
use crate::types::DataType;
use crate::AggrState;
use crate::AggregateFunctionRef;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::PayloadFlushState;
use crate::ProjectedBlock;
use crate::StateAddr;
use crate::StatesLayout;
use crate::BATCH_SIZE;
use crate::MAX_PAGE_SIZE;

// payload layout
// [VALIDITY][GROUPS][HASH][STATE_ADDRS]
// [VALIDITY] is the validity bits of the data columns (including the HASH)
// [GROUPS] is the group data, could be multiple values, fixed size, strings are elsewhere
// [HASH] is the hash data of the groups
// [STATE_ADDRS] is the state_addrs of the aggregate functions, 8 bytes each
pub struct Payload {
    pub arena: Arc<Bump>,
    // if true, the states are moved out of the payload into other payload, and will not be dropped
    pub state_move_out: bool,
    pub group_types: Vec<DataType>,
    pub aggrs: Vec<AggregateFunctionRef>,

    pub pages: Pages,
    pub tuple_size: usize,
    pub row_per_page: usize,

    pub total_rows: usize,

    // Starts from 1, zero means no page allocated
    pub current_write_page: usize,

    pub(super) row_layout: RowLayout,

    // if set, the payload contains at least duplicate rows
    pub min_cardinality: Option<usize>,
}

unsafe impl Send for Payload {}
unsafe impl Sync for Payload {}

pub struct Page {
    pub(crate) data: Vec<MaybeUninit<u8>>,
    pub(crate) rows: usize,
    // state_offset = state_rows * agg_len
    // which mark that the offset to clean the agg states
    pub(crate) state_offsets: usize,
    pub(crate) capacity: usize,
}

impl Page {
    pub fn is_partial_state(&self, agg_len: usize) -> bool {
        self.rows * agg_len != self.state_offsets
    }

    pub(super) fn data_ptr(&self, row: usize, row_size: usize) -> RowPtr {
        RowPtr::new(unsafe { self.data.as_ptr().add(row * row_size) as _ })
    }
}

pub type Pages = Vec<Page>;

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
            state_move_out: false,
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

    pub fn clear(&mut self) {
        self.total_rows = 0;
        self.pages.clear();
    }

    #[inline]
    pub fn memory_size(&self) -> usize {
        self.total_rows * self.tuple_size
    }

    pub fn states_layout(&self) -> Option<&StatesLayout> {
        self.row_layout.states_layout.as_ref()
    }

    #[inline]
    pub fn writable_page(&mut self) -> (&mut Page, usize) {
        if self.current_write_page == 0
            || self.pages[self.current_write_page - 1].rows
                == self.pages[self.current_write_page - 1].capacity
        {
            self.current_write_page += 1;
            if self.current_write_page > self.pages.len() {
                let data = Vec::with_capacity(self.row_per_page * self.tuple_size);
                self.pages.push(Page {
                    data,
                    rows: 0,
                    state_offsets: 0,
                    capacity: self.row_per_page,
                });
            }
        }
        (
            &mut self.pages[self.current_write_page - 1],
            self.current_write_page - 1,
        )
    }

    pub(super) fn data_ptr(&self, page: &Page, row: usize) -> RowPtr {
        page.data_ptr(row, self.tuple_size)
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
        for row in select_vector {
            address[*row] = page.data_ptr(page.rows, tuple_size);
            page_index[*row] = page_index_value;
            page.rows += 1;

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
        address: &mut [RowPtr; BATCH_SIZE],
        page_index: &mut [usize],
        group_columns: ProjectedBlock,
    ) {
        let mut write_offset = 0;
        // write validity
        for entry in group_columns.iter() {
            if let Column::Nullable(c) = entry.to_column() {
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
        }

        let mut scratch = vec![];
        for (idx, entry) in group_columns.iter().enumerate() {
            let offset = self.row_layout.group_offsets[idx];
            assert!(write_offset == offset);

            unsafe {
                serialize_column_to_rowformat(
                    &self.arena,
                    &entry.to_column(),
                    select_vector,
                    address,
                    offset,
                    &mut scratch,
                );
            }
            write_offset += self.row_layout.group_sizes[idx];
        }

        // write group hashes
        debug_assert!(write_offset == self.row_layout.hash_offset);
        for row in select_vector {
            address[*row].set_hash(&self.row_layout, group_hashes[*row]);
        }

        debug_assert!(write_offset + 8 == self.row_layout.state_offset);
        if let Some(StatesLayout {
            layout, states_loc, ..
        }) = &self.row_layout.states_layout
        {
            // write states
            let (array_layout, padded_size) = layout.repeat(select_vector.len()).unwrap();
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
                for (aggr, loc) in self.aggrs.iter().zip(states_loc.iter()) {
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

    pub fn combine(&mut self, mut other: Payload) {
        debug_assert_eq!(
            other.total_rows,
            other.pages.iter().map(|x| x.rows).sum::<usize>()
        );

        self.total_rows += other.total_rows;
        self.pages.append(other.pages.as_mut());
    }

    pub fn mark_min_cardinality(&mut self) {
        if self.min_cardinality.is_none() {
            self.min_cardinality = Some(self.total_rows);
        }
    }

    pub fn copy_rows(&mut self, select_vector: &[RowID], address: &[RowPtr; BATCH_SIZE]) {
        let tuple_size = self.tuple_size;
        let agg_len = self.aggrs.len();
        let (mut page, _) = self.writable_page();

        for index in select_vector {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    address[*index].as_ptr(),
                    page.data.as_mut_ptr().add(page.rows * tuple_size) as _,
                    tuple_size,
                )
            }
            page.rows += 1;
            page.state_offsets += agg_len;

            if page.rows == page.capacity {
                (page, _) = self.writable_page();
            }
        }

        self.total_rows += select_vector.len();

        debug_assert_eq!(
            self.total_rows,
            self.pages.iter().map(|x| x.rows).sum::<usize>()
        );
    }

    pub fn scatter(&self, flush_state: &mut PayloadFlushState, partition_count: usize) -> bool {
        if flush_state.flush_page >= self.pages.len() {
            return false;
        }

        let page = &self.pages[flush_state.flush_page];

        // ToNext
        if flush_state.flush_page_row >= page.rows {
            flush_state.flush_page += 1;
            flush_state.flush_page_row = 0;
            flush_state.row_count = 0;
            return self.scatter(flush_state, partition_count);
        }

        let end = (flush_state.flush_page_row + BATCH_SIZE).min(page.rows);
        let rows = end - flush_state.flush_page_row;
        flush_state.row_count = rows;

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
        flush_state.flush_page_row = end;
        true
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
            // drop states
            if self.state_move_out {
                return;
            }

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
