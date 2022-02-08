// Copyright 2021 Datafuse Labs.
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
use std::ptr::NonNull;

use crate::aggregates::AggregateFunctionRef;

#[derive(Clone, Copy)]
pub struct StateAddr {
    addr: usize,
}

pub type StateAddrs = Vec<StateAddr>;

impl StateAddr {
    #[inline]
    pub fn new(addr: usize) -> StateAddr {
        Self { addr }
    }

    #[inline]
    pub fn get<'a, T>(&self) -> &'a mut T {
        unsafe { &mut *(self.addr as *mut T) }
    }

    #[inline]
    pub fn addr(&self) -> usize {
        self.addr
    }

    /// # Safety
    /// ptr must ensure point to valid memory
    #[inline]
    pub unsafe fn from_ptr(ptr: *mut u8) -> Self {
        Self { addr: ptr as usize }
    }

    #[inline]
    #[must_use]
    pub fn next(&self, offset: usize) -> Self {
        Self {
            addr: self.addr + offset,
        }
    }

    #[inline]
    #[must_use]
    pub fn prev(&self, offset: usize) -> Self {
        Self {
            addr: self.addr.wrapping_sub(offset),
        }
    }

    #[inline]
    pub fn write<T, F>(&self, f: F)
    where F: FnOnce() -> T {
        unsafe {
            let ptr = self.addr as *mut T;
            std::ptr::write(ptr, f());
        }
    }
}

impl From<NonNull<u8>> for StateAddr {
    fn from(s: NonNull<u8>) -> Self {
        Self {
            addr: s.as_ptr() as usize,
        }
    }
}

impl From<usize> for StateAddr {
    fn from(addr: usize) -> Self {
        Self { addr }
    }
}

impl From<StateAddr> for NonNull<u8> {
    fn from(s: StateAddr) -> Self {
        unsafe { NonNull::new_unchecked(s.addr as *mut u8) }
    }
}

impl From<StateAddr> for usize {
    fn from(s: StateAddr) -> Self {
        s.addr
    }
}

/// # Safety
/// layout must ensure to be aligned
pub unsafe fn get_layout_offsets(funcs: &[AggregateFunctionRef]) -> (Layout, Vec<usize>) {
    let mut max_align = 0;
    let mut total_size = 0;

    let mut offsets = Vec::with_capacity(funcs.len());
    for func in funcs {
        let layout = func.state_layout();
        let align = layout.align();

        total_size = (total_size + align - 1) / align * align;

        offsets.push(total_size);

        max_align = max_align.max(align);
        total_size += layout.size();
    }
    let layout = Layout::from_size_align_unchecked(total_size, max_align);
    (layout, offsets)
}
