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
use crate::read;
use crate::store;
use crate::types::DataType;
use crate::AggrState;
use crate::AggregateFunctionRef;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::PayloadFlushState;
use crate::ProjectedBlock;
use crate::SelectVector;
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

    pub group_offsets: Vec<usize>,
    pub group_sizes: Vec<usize>,
    pub validity_offsets: Vec<usize>,
    pub hash_offset: usize,
    pub state_offset: usize,
    pub states_layout: Option<StatesLayout>,

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

        let hash_size = 8;
        tuple_size += hash_size;

        let state_offset = tuple_size;
        if !aggrs.is_empty() {
            tuple_size += 8;
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
            group_offsets,
            group_sizes,
            validity_offsets,
            hash_offset,
            state_offset,
            states_layout,
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

    #[inline]
    pub fn data_ptr(&self, page: &Page, row: usize) -> *const u8 {
        unsafe { page.data.as_ptr().add(row * self.tuple_size) as _ }
    }

    pub fn reserve_append_rows(
        &mut self,
        select_vector: &SelectVector,
        group_hashes: &[u64],
        address: &mut [*const u8],
        page_index: &mut [usize],
        new_group_rows: usize,
        group_columns: ProjectedBlock,
    ) {
        let tuple_size = self.tuple_size;
        let (mut page, mut page_index_value) = self.writable_page();
        for idx in select_vector.iter().take(new_group_rows).copied() {
            address[idx] = unsafe { page.data.as_ptr().add(page.rows * tuple_size) as *const u8 };
            page_index[idx] = page_index_value;
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
            new_group_rows,
            group_columns,
        )
    }

    pub fn append_rows(
        &mut self,
        select_vector: &SelectVector,
        group_hashes: &[u64],
        address: &mut [*const u8],
        page_index: &mut [usize],
        new_group_rows: usize,
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
                    for idx in select_vector.iter().take(new_group_rows).copied() {
                        unsafe {
                            let dst = address[idx].add(write_offset);
                            store::<u8>(&val, dst as *mut u8);
                        }
                    }
                } else {
                    for idx in select_vector.iter().take(new_group_rows).copied() {
                        unsafe {
                            let dst = address[idx].add(write_offset);
                            store::<u8>(&(bitmap.get_bit(idx) as u8), dst as *mut u8);
                        }
                    }
                }
                write_offset += 1;
            }
        }

        let mut scratch = vec![];
        for (idx, entry) in group_columns.iter().enumerate() {
            debug_assert!(write_offset == self.group_offsets[idx]);

            unsafe {
                serialize_column_to_rowformat(
                    &self.arena,
                    &entry.to_column(),
                    select_vector,
                    new_group_rows,
                    address,
                    write_offset,
                    &mut scratch,
                );
            }
            write_offset += self.group_sizes[idx];
        }

        // write group hashes
        debug_assert!(write_offset == self.hash_offset);
        for idx in select_vector.iter().take(new_group_rows).copied() {
            unsafe {
                let dst = address[idx].add(write_offset);
                store::<u64>(&group_hashes[idx], dst as *mut u8);
            }
        }

        write_offset += 8;
        debug_assert!(write_offset == self.state_offset);
        if let Some(StatesLayout {
            layout, states_loc, ..
        }) = &self.states_layout
        {
            // write states
            let (array_layout, padded_size) = layout.repeat(new_group_rows).unwrap();
            // Bump only allocates but does not drop, so there is no use after free for any item.
            let place = self.arena.alloc_layout(array_layout);
            for (idx, place) in select_vector
                .iter()
                .take(new_group_rows)
                .copied()
                .enumerate()
                .map(|(i, idx)| (idx, unsafe { place.add(padded_size * i) }))
            {
                unsafe {
                    let dst = address[idx].add(write_offset);
                    store::<u64>(&(place.as_ptr() as u64), dst as *mut u8);
                }

                let place = StateAddr::from(place);
                let page = &mut self.pages[page_index[idx]];
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

        self.total_rows += new_group_rows;

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

    pub fn copy_rows(
        &mut self,
        select_vector: &SelectVector,
        row_count: usize,
        address: &[*const u8],
    ) {
        let tuple_size = self.tuple_size;
        let agg_len = self.aggrs.len();
        let (mut page, _) = self.writable_page();
        for i in 0..row_count {
            let index = select_vector[i];

            unsafe {
                std::ptr::copy_nonoverlapping(
                    address[index],
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

        self.total_rows += row_count;

        debug_assert_eq!(
            self.total_rows,
            self.pages.iter().map(|x| x.rows).sum::<usize>()
        );
    }

    pub fn scatter(&self, state: &mut PayloadFlushState, partition_count: usize) -> bool {
        if state.flush_page >= self.pages.len() {
            return false;
        }

        let page = &self.pages[state.flush_page];

        // ToNext
        if state.flush_page_row >= page.rows {
            state.flush_page += 1;
            state.flush_page_row = 0;
            state.row_count = 0;
            return self.scatter(state, partition_count);
        }

        let end = (state.flush_page_row + BATCH_SIZE).min(page.rows);
        let rows = end - state.flush_page_row;
        state.row_count = rows;

        state.probe_state.reset_partitions(partition_count);

        let mods: StrengthReducedU64 = StrengthReducedU64::new(partition_count as u64);
        for idx in 0..rows {
            state.addresses[idx] = self.data_ptr(page, idx + state.flush_page_row);

            let hash = unsafe { read::<u64>(state.addresses[idx].add(self.hash_offset) as _) };

            let partition_idx = (hash % mods) as usize;

            let sel = &mut state.probe_state.partition_entries[partition_idx];
            sel[state.probe_state.partition_count[partition_idx]] = idx;
            state.probe_state.partition_count[partition_idx] += 1;
        }
        state.flush_page_row = end;
        true
    }

    pub fn empty_block(&self, fake_rows: Option<usize>) -> DataBlock {
        let fake_rows = fake_rows.unwrap_or(0);
        let entries = (0..self.aggrs.len())
            .map(|_| {
                ColumnBuilder::repeat_default(&DataType::Binary, fake_rows)
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

            let Some(states_layout) = self.states_layout.as_ref() else {
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
                        let ptr = self.data_ptr(page, row);
                        unsafe {
                            let state_addr = read::<u64>(ptr.add(self.state_offset) as _) as usize;
                            aggr.drop_state(AggrState::new(StateAddr::new(state_addr), loc));
                        }
                    }
                }
            }
        })
    }
}
