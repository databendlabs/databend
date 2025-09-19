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

use super::PartitionedPayload;
use super::ProbeState;
use super::RowPtr;
use super::BATCH_SIZE;
use super::LOAD_FACTOR;
use crate::ProjectedBlock;

pub(super) struct HashIndex {
    pub entries: Vec<Entry>,
    pub count: usize,
    pub capacity: usize,
}

impl HashIndex {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: vec![Entry::default(); capacity],
            count: 0,
            capacity,
        }
    }

    fn init_slot(&self, hash: u64) -> usize {
        hash as usize & (self.capacity - 1)
    }

    fn find_or_insert(&mut self, mut slot: usize, salt: u16) -> (usize, bool) {
        loop {
            let entry = &mut self.entries[slot];
            if entry.is_occupied() {
                if entry.get_salt() == salt {
                    return (slot, false);
                } else {
                    slot += 1;
                    if slot >= self.capacity {
                        slot = 0;
                    }
                    continue;
                }
            } else {
                entry.set_salt(salt);
                return (slot, true);
            }
        }
    }

    pub fn probe_slot(&mut self, hash: u64) -> usize {
        let mut slot = self.init_slot(hash);
        while self.entries[slot].is_occupied() {
            slot += 1;
            if slot >= self.capacity {
                slot = 0;
            }
        }
        slot as _
    }

    pub fn mut_entry(&mut self, slot: usize) -> &mut Entry {
        &mut self.entries[slot]
    }

    pub fn reset(&mut self) {
        self.count = 0;
        self.entries.fill(Entry::default());
    }

    pub fn resize_threshold(&self) -> usize {
        (self.capacity as f64 / LOAD_FACTOR) as usize
    }

    pub fn allocated_bytes(&self) -> usize {
        self.entries.len() * std::mem::size_of::<Entry>()
    }
}

/// Upper 16 bits are salt
const SALT_MASK: u64 = 0xFFFF000000000000;
/// Lower 48 bits are the pointer
const POINTER_MASK: u64 = 0x0000FFFFFFFFFFFF;

// The high 16 bits are the salt, the low 48 bits are the pointer address
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) struct Entry(pub(super) u64);

impl Entry {
    pub fn hash_to_salt(hash: u64) -> u16 {
        (hash >> 48) as _
    }

    pub fn get_salt(&self) -> u16 {
        (self.0 >> 48) as _
    }

    pub fn set_salt(&mut self, salt: u16) {
        self.0 = POINTER_MASK | (salt as u64) << 48;
    }

    pub fn set_hash(&mut self, hash: u64) {
        self.0 = hash | POINTER_MASK
    }

    pub fn is_occupied(&self) -> bool {
        self.0 != 0
    }

    pub fn get_pointer(&self) -> RowPtr {
        RowPtr::new((self.0 & POINTER_MASK) as *const u8)
    }

    pub fn set_pointer(&mut self, ptr: RowPtr) {
        let ptr_value = ptr.as_ptr() as u64;
        // Pointer shouldn't use upper bits
        debug_assert!(ptr_value & SALT_MASK == 0);
        // Value should have all 1's in the pointer area
        debug_assert!(self.0 & POINTER_MASK == POINTER_MASK);

        self.0 &= ptr_value | SALT_MASK;
    }
}

pub(super) trait TableAdapter {
    fn append_rows(&mut self, state: &mut ProbeState, new_entry_count: usize);

    fn compare(
        &mut self,
        state: &mut ProbeState,
        need_compare_count: usize,
        no_match_count: usize,
    ) -> usize;
}

impl HashIndex {
    pub fn probe_and_create(
        &mut self,
        state: &mut ProbeState,
        row_count: usize,
        mut adapter: impl TableAdapter,
    ) -> usize {
        #[derive(Default, Clone, Copy, Debug)]
        struct Item {
            slot: usize,
            hash: u64,
        }

        let mut items = [Item::default(); BATCH_SIZE];

        for row in 0..row_count {
            items[row] = Item {
                slot: self.init_slot(state.group_hashes[row]),
                hash: state.group_hashes[row],
            };
            state.no_match_vector[row] = row;
        }

        let mut new_group_count = 0;
        let mut remaining_entries = row_count;

        while remaining_entries > 0 {
            let mut new_entry_count = 0;
            let mut need_compare_count = 0;
            let mut no_match_count = 0;

            // 1. inject new_group_count, new_entry_count, need_compare_count, no_match_count
            for row in state.no_match_vector[..remaining_entries].iter().copied() {
                let item = &mut items[row];

                let is_new;
                (item.slot, is_new) = self.find_or_insert(item.slot, Entry::hash_to_salt(item.hash));

                if is_new {
                    state.empty_vector[new_entry_count] = row;
                    new_entry_count += 1;
                } else {
                    state.group_compare_vector[need_compare_count] = row;
                    need_compare_count += 1;
                }
            }

            // 2. append new_group_count to payload
            if new_entry_count != 0 {
                new_group_count += new_entry_count;

                adapter.append_rows(state, new_entry_count);

                for row in state.empty_vector[..new_entry_count].iter().copied() {
                    let entry = self.mut_entry(items[row].slot);
                    entry.set_pointer(state.addresses[row]);
                    debug_assert_eq!(entry.get_pointer(), state.addresses[row]);
                }
            }

            // 3. set address of compare vector
            if need_compare_count > 0 {
                for row in state.group_compare_vector[..need_compare_count]
                    .iter()
                    .copied()
                {
                    let entry = self.mut_entry(items[row].slot);

                    debug_assert!(entry.is_occupied());
                    debug_assert_eq!(entry.get_salt(), (items[row].hash >> 48) as u16);
                    state.addresses[row] = entry.get_pointer();
                }

                // 4. compare
                no_match_count = adapter.compare(state, need_compare_count, no_match_count);
            }

            // 5. Linear probing, just increase iter_times
            for row in state.no_match_vector[..no_match_count].iter().copied() {
                let slot = &mut items[row].slot;
                *slot += 1;
                if *slot >= self.capacity {
                    *slot = 0;
                }
            }
            remaining_entries = no_match_count;
        }

        self.count += new_group_count;

        new_group_count
    }
}

pub(super) struct AdapterImpl<'a> {
    pub payload: &'a mut PartitionedPayload,
    pub group_columns: ProjectedBlock<'a>,
}

impl<'a> TableAdapter for AdapterImpl<'a> {
    fn append_rows(&mut self, state: &mut ProbeState, count: usize) {
        self.payload.append_rows(state, count, self.group_columns);
    }

    fn compare(
        &mut self,
        state: &mut ProbeState,
        need_compare_count: usize,
        no_match_count: usize,
    ) -> usize {
        state.row_match_columns(
            self.group_columns,
            &self.payload.row_layout,
            (need_compare_count, no_match_count),
        )
    }
}
