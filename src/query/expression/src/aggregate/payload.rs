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
use std::sync::Arc;

use bumpalo::Bump;

use super::payload_row::rowformat_size;
use super::payload_row::serialize_column_to_rowformat;
use super::probe_state::ProbeState;
use crate::get_layout_offsets;
use crate::load;
use crate::select_vector::SelectVector;
use crate::store;
use crate::types::DataType;
use crate::AggregateFunctionRef;
use crate::Column;
use crate::StateAddr;

const MAX_PAGE_SIZE: usize = 256 * 1024;
// payload layout
// [VALIDITY][GROUPS][HASH][STATE_ADDRS]
// [VALIDITY] is the validity bits of the data columns (including the HASH)
// [GROUPS] is the group data, could be multiple values, fixed size, strings are elsewhere
// [HASH] is the hash data of the groups
// [STATE_ADDRS] is the state_addrs of the aggregate functions, 8 bytes each
pub struct Payload {
    pub arena: Arc<Bump>,
    pub group_types: Vec<DataType>,
    pub aggrs: Vec<AggregateFunctionRef>,

    pub pages: Vec<Vec<u8>>,
    pub tuple_size: usize,
    pub row_per_page: usize,
    pub current_row: usize,

    pub group_offsets: Vec<usize>,
    pub group_sizes: Vec<usize>,
    pub validity_offsets: Vec<usize>,
    pub hash_offset: usize,
    pub state_offset: usize,
    pub state_addr_offsets: Vec<usize>,
    pub state_layout: Layout,
}

// TODO FIXME
impl Payload {
    pub fn new(
        arena: Arc<Bump>,
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
    ) -> Self {
        let mut state_addr_offsets = Vec::new();
        let state_layout = get_layout_offsets(&aggrs, &mut state_addr_offsets).unwrap();

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
        tuple_size += 8;

        Self {
            arena,
            pages: vec![],
            group_types,
            aggrs,
            tuple_size,
            row_per_page: (u16::MAX as usize).min(MAX_PAGE_SIZE / tuple_size).max(1),
            current_row: 0,
            group_offsets,
            group_sizes,
            validity_offsets,
            hash_offset,
            state_offset,
            state_addr_offsets,
            state_layout,
        }
    }

    pub fn len(&self) -> usize {
        self.current_row
    }

    pub fn get_page_ptr(&self, page_nr: usize) -> *const u8 {
        self.pages[page_nr].as_ptr()
    }

    pub fn try_reverse(&mut self, additional_rows: usize) {
        let mut row_capacity = self.pages.len() * self.row_per_page - self.current_row;

        while row_capacity < additional_rows {
            self.pages
                .push(vec![0; self.row_per_page * self.tuple_size]);
            row_capacity += self.row_per_page;
        }
    }

    pub fn get_row_ptr(&self, row: usize) -> *const u8 {
        let page = row / self.row_per_page;
        let page_ptr = self.get_page_ptr(page);
        let row_offset = (row % self.row_per_page) * self.tuple_size;

        unsafe { page_ptr.offset(row_offset as isize) }
    }

    pub fn append_rows(
        &mut self,
        state: &mut ProbeState,
        group_hashes: &[u64],
        select_vector: &SelectVector,
        new_group_rows: usize,
        group_columns: &[Column],
    ) {
        self.try_reverse(new_group_rows);

        for i in 0..new_group_rows {
            let idx = select_vector.get_index(i);

            state.addresses[idx] = self.get_row_ptr(self.current_row);
            self.current_row += 1;
        }

        let address = state.addresses.as_slice();

        let mut write_offset = 0;
        // write validity
        for col in group_columns {
            if let Column::Nullable(c) = col {
                let bitmap = &c.validity;
                for i in 0..new_group_rows {
                    let idx = select_vector.get_index(i);
                    if bitmap.get_bit(idx) {
                        unsafe {
                            let dst = address[i].offset(write_offset as isize);
                            store(&1, dst as *mut u8);
                        }
                    }
                }
                write_offset += 1;
            }
        }

        let mut scratch = vec![];
        for (idx, col) in group_columns.iter().enumerate() {
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
        for i in 0..new_group_rows {
            let idx = select_vector.get_index(i);
            unsafe {
                let dst = address[i].offset(write_offset as isize);
                store(&group_hashes[idx], dst as *mut u8);
            }
        }

        write_offset += 8;

        // write states
        for i in 0..new_group_rows {
            let place = self.arena.alloc_layout(self.state_layout);
            let idx = select_vector.get_index(i);
            unsafe {
                let dst = address[idx].offset(write_offset as isize);
                store(&(place.as_ptr() as u64), dst as *mut u8);
            }

            let place = StateAddr::from(place);
            for (aggr, offset) in self.aggrs.iter().zip(self.state_addr_offsets.iter()) {
                aggr.init_state(place.next(*offset));
            }
        }
    }
}

impl Drop for Payload {
    fn drop(&mut self) {
        // drop states
        for (aggr, addr_offset) in self.aggrs.iter().zip(self.state_addr_offsets.iter()) {
            if aggr.need_manual_drop_state() {
                for row in 0..self.len() {
                    let row_ptr = self.get_row_ptr(row);

                    unsafe {
                        let state_addr: u64 = load(row_ptr.offset(self.state_offset as isize));
                        aggr.drop_state(StateAddr::new(state_addr as usize + *addr_offset))
                    };
                }
            }
        }
    }
}
