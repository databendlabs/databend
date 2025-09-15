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

use super::RowPtr;
use super::LOAD_FACTOR;

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

    pub fn init_slot(&self, hash: Entry) -> usize {
        hash.0 as usize & (self.capacity - 1)
    }

    pub fn probe_slot(&mut self, hash: Entry) -> usize {
        let mut slot = self.init_slot(hash);
        while self.entries[slot].is_occupied() {
            slot += 1;
            if slot >= self.capacity {
                slot = 0;
            }
        }
        slot as _
    }

    pub fn insert(&mut self, mut slot: usize, salt: u64) -> (usize, bool) {
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
    pub fn get_salt(&self) -> u64 {
        self.0 | POINTER_MASK
    }

    pub fn set_salt(&mut self, salt: u64) {
        self.0 = salt;
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
