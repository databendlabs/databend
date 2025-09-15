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

use super::Entry;
use crate::StateAddr;
use crate::StatesLayout;

/// A wrapper around raw pointer that provides safe and convenient methods
/// for accessing row data in the aggregate hash table.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct RowPtr(*const u8);

impl RowPtr {
    pub(super) fn new(ptr: *const u8) -> Self {
        Self(ptr)
    }

    pub(super) fn null() -> Self {
        Self(std::ptr::null())
    }

    pub(super) fn as_ptr(&self) -> *const u8 {
        self.0
    }

    fn add(&self, offset: usize) -> Self {
        Self(unsafe { self.0.add(offset) })
    }

    pub(super) unsafe fn read<T>(&self, offset: usize) -> T {
        let ptr = self.add(offset).as_ptr() as _;
        core::ptr::read_unaligned::<T>(ptr)
    }

    pub(super) unsafe fn write<T: Copy>(&self, offset: usize, value: &T) {
        let ptr = self.add(offset).as_ptr() as *mut u8;
        core::ptr::copy_nonoverlapping(
            value as *const T as *const u8,
            ptr,
            std::mem::size_of::<T>(),
        );
    }

    pub(super) unsafe fn write_bytes(&self, offset: usize, value: &[u8]) {
        self.write(offset, &(value.len() as u32));
        self.write(offset + 4, &(value.as_ptr() as u64));
    }

    pub(super) unsafe fn read_bytes(&self, offset: usize) -> &[u8] {
        let len = self.read::<u32>(offset) as usize;
        let data_ptr = self.read::<u64>(offset + 4) as *const u8;
        std::slice::from_raw_parts(data_ptr, len)
    }

    pub(super) unsafe fn is_bytes_eq(&self, offset: usize, other: &[u8]) -> bool {
        let scalar = self.read_bytes(offset);
        scalar.len() == other.len() && databend_common_hashtable::fast_memcmp(scalar, other)
    }

    pub(super) unsafe fn write_u8(&self, offset: usize, value: u8) {
        self.write::<u8>(offset, &value);
    }

    pub(super) fn hash(&self, layout: &RowLayout) -> Entry {
        unsafe { self.read::<Entry>(layout.hash_offset) }
    }

    pub(super) fn set_hash(&self, layout: &RowLayout, value: Entry) {
        unsafe {
            self.write(layout.hash_offset, &value);
        }
    }

    pub(super) fn state_addr(&self, layout: &RowLayout) -> StateAddr {
        StateAddr::new(unsafe { self.read::<u64>(layout.state_offset) } as _)
    }

    pub(super) fn set_state_addr(&self, layout: &RowLayout, value: &StateAddr) {
        unsafe {
            self.write(layout.state_offset, &(value.addr() as u64));
        }
    }
}

#[derive(Clone, Debug)]
pub struct RowLayout {
    pub(super) hash_offset: usize,
    pub(super) state_offset: usize,
    pub(super) validity_offsets: Vec<usize>,
    pub(super) group_offsets: Vec<usize>,
    pub(super) group_sizes: Vec<usize>,
    pub(super) states_layout: Option<StatesLayout>,
}
