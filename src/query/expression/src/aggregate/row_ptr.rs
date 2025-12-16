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

use super::RowID;
use crate::types::StringColumn;
use crate::StateAddr;
use crate::StatesLayout;

/// A wrapper around raw pointer that provides safe and convenient methods
/// for accessing row data in the aggregate hash table.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct RowPtr(*mut u8);

impl RowPtr {
    pub(super) fn new(ptr: *mut u8) -> Self {
        Self(ptr)
    }

    pub(super) fn null() -> Self {
        Self(std::ptr::null_mut())
    }

    pub(super) fn as_ptr(&self) -> *const u8 {
        self.0.cast_const()
    }

    pub(super) unsafe fn read<T>(&self, offset: usize) -> T { unsafe {
        core::ptr::read_unaligned(self.0.add(offset).cast::<T>().cast_const())
    }}

    pub(super) unsafe fn write<T: Copy>(&mut self, offset: usize, value: &T) { unsafe {
        core::ptr::copy_nonoverlapping(
            value as *const T as *const u8,
            self.0.add(offset),
            size_of::<T>(),
        );
    }}

    pub(super) unsafe fn write_bytes(&mut self, offset: usize, value: &[u8]) { unsafe {
        self.write(offset, &(value.len() as u32));
        self.write(offset + 4, &(value.as_ptr() as u64));
    }}

    pub(super) unsafe fn read_bytes(&self, offset: usize) -> &[u8] { unsafe {
        let len = self.read::<u32>(offset) as usize;
        let data_ptr = self.read::<u64>(offset + 4) as *const u8;
        std::slice::from_raw_parts(data_ptr, len)
    }}

    pub(super) unsafe fn is_bytes_eq(&self, offset: usize, other: &[u8]) -> bool { unsafe {
        let scalar = self.read_bytes(offset);
        scalar.len() == other.len() && databend_common_hashtable::fast_memcmp(scalar, other)
    }}

    pub(super) unsafe fn eq_string_view(
        &self,
        offset: usize,
        str_view: &StringColumn,
        row: RowID,
    ) -> bool { unsafe {
        let row = row.to_usize();
        let v = str_view.views().get_unchecked(row);
        let len = self.read::<u32>(offset);
        if v.length != len {
            return false;
        }
        let scalar = {
            let data_ptr = self.read::<u64>(offset + size_of::<u32>()) as *const u8;
            std::slice::from_raw_parts(data_ptr, len as _)
        };
        let other = v.get_slice_unchecked(str_view.data_buffers());
        databend_common_hashtable::fast_memcmp(scalar, other)
    }}

    pub(super) unsafe fn read_bool(&self, offset: usize) -> bool { unsafe {
        self.read::<u8>(offset) != 0
    }}

    pub(super) unsafe fn write_u8(&mut self, offset: usize, value: u8) { unsafe {
        self.write::<u8>(offset, &value);
    }}

    pub(super) fn hash(&self, layout: &RowLayout) -> u64 {
        unsafe { self.read::<u64>(layout.hash_offset) }
    }

    pub(super) fn set_hash(&mut self, layout: &RowLayout, value: u64) {
        unsafe {
            self.write(layout.hash_offset, &value);
        }
    }

    pub(super) fn state_addr(&self, layout: &RowLayout) -> StateAddr {
        unsafe { self.read::<StateAddr>(layout.state_offset) }
    }

    pub(super) fn set_state_addr(&mut self, layout: &RowLayout, value: &StateAddr) {
        unsafe {
            self.write(layout.state_offset, value);
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
