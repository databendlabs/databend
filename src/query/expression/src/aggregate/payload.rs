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
use std::mem::MaybeUninit;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_base::runtime::drop_guard;
use itertools::Itertools;
use strength_reduce::StrengthReducedU64;

use super::payload_row::rowformat_size;
use super::payload_row::serialize_column_to_rowformat;
use crate::get_layout_offsets;
use crate::read;
use crate::store;
use crate::types::DataType;
use crate::AggregateFunctionRef;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::PayloadFlushState;
use crate::SelectVector;
use crate::StateAddr;
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
    pub state_addr_offsets: Vec<usize>,
    pub state_layout: Option<Layout>,

    // if set, the payload contains at least duplicate rows
    pub min_cardinality: Option<usize>,
}

unsafe impl Send for Payload {}
unsafe impl Sync for Payload {}

pub struct Page {
    pub(crate) data: Vec<MaybeUninit<u8>>,
    pub(crate) rows: usize,
    pub(crate) capacity: usize,
}

pub type Pages = Vec<Page>;

// TODO FIXME
impl Payload {
    pub fn new(
        arena: Arc<Bump>,
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
    ) -> Self {
        let mut state_addr_offsets = Vec::new();
        let state_layout = if !aggrs.is_empty() {
            Some(get_layout_offsets(&aggrs, &mut state_addr_offsets).unwrap())
        } else {
            None
        };

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
            state_addr_offsets,
            state_layout,
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
    pub fn writable_page(&mut self) -> &mut Page {
        if self.current_write_page == 0
            || self.pages[self.current_write_page - 1].rows
                == self.pages[self.current_write_page - 1].capacity
        {
            self.current_write_page += 1;
            if self.current_write_page > self.pages.len() {
                self.pages.push(Page {
                    data: Vec::with_capacity(self.row_per_page * self.tuple_size),
                    rows: 0,
                    capacity: self.row_per_page,
                });
            }
        }
        &mut self.pages[self.current_write_page - 1]
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
        new_group_rows: usize,
        group_columns: &[Column],
    ) {
        let tuple_size = self.tuple_size;
        let mut page = self.writable_page();
        for idx in select_vector.iter().take(new_group_rows).copied() {
            address[idx] = unsafe { page.data.as_ptr().add(page.rows * tuple_size) as *const u8 };
            page.rows += 1;

            if page.rows == page.capacity {
                page = self.writable_page();
            }
        }

        self.total_rows += new_group_rows;

        debug_assert_eq!(
            self.total_rows,
            self.pages.iter().map(|x| x.rows).sum::<usize>()
        );

        self.append_rows(
            select_vector,
            group_hashes,
            address,
            new_group_rows,
            group_columns,
        )
    }

    pub fn append_rows(
        &mut self,
        select_vector: &SelectVector,
        group_hashes: &[u64],
        address: &mut [*const u8],
        new_group_rows: usize,
        group_columns: &[Column],
    ) {
        let mut write_offset = 0;
        // write validity
        for col in group_columns {
            if let Column::Nullable(c) = col {
                let bitmap = &c.validity;
                if bitmap.unset_bits() == 0 || bitmap.unset_bits() == bitmap.len() {
                    let val: u8 = if bitmap.unset_bits() == 0 { 1 } else { 0 };
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
        for (idx, col) in group_columns.iter().enumerate() {
            debug_assert!(write_offset == self.group_offsets[idx]);

            unsafe {
                serialize_column_to_rowformat(
                    &self.arena,
                    col,
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
        if let Some(layout) = self.state_layout {
            // write states
            for idx in select_vector.iter().take(new_group_rows).copied() {
                let place = self.arena.alloc_layout(layout);
                unsafe {
                    let dst = address[idx].add(write_offset);
                    store::<u64>(&(place.as_ptr() as u64), dst as *mut u8);
                }

                let place = StateAddr::from(place);
                for (aggr, offset) in self.aggrs.iter().zip(self.state_addr_offsets.iter()) {
                    aggr.init_state(place.next(*offset));
                }
            }
        }
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
        let mut page = self.writable_page();
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

            if page.rows == page.capacity {
                page = self.writable_page();
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

    pub fn empty_block(&self) -> DataBlock {
        let columns = self
            .aggrs
            .iter()
            .map(|f| ColumnBuilder::with_capacity(&f.return_type().unwrap(), 0).build())
            .chain(
                self.group_types
                    .iter()
                    .map(|t| ColumnBuilder::with_capacity(t, 0).build()),
            )
            .collect_vec();
        DataBlock::new_from_columns(columns)
    }
}

impl Drop for Payload {
    fn drop(&mut self) {
        drop_guard(move || {
            // drop states
            if !self.state_move_out {
                for (aggr, addr_offset) in self.aggrs.iter().zip(self.state_addr_offsets.iter()) {
                    if aggr.need_manual_drop_state() {
                        for page in self.pages.iter() {
                            for row in 0..page.rows {
                                unsafe {
                                    let state_place = StateAddr::new(read::<u64>(
                                        self.data_ptr(page, row).add(self.state_offset) as _,
                                    )
                                        as usize);

                                    aggr.drop_state(state_place.next(*addr_offset));
                                }
                            }
                        }
                    }
                }
            }
        })
    }
}
