// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::alloc::Layout;
use std::ptr::NonNull;

use super::AggregateFunctionRef;

#[derive(Clone, Copy)]
pub struct StateAddr {
    addr: usize,
}

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
    pub fn next(&self, offset: usize) -> Self {
        Self {
            addr: self.addr + offset,
        }
    }

    #[inline]
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
    let mut align_aggregate_states = 0;
    let mut total_size_aggregate_states = 0;

    let mut offsets_aggregate_states = Vec::with_capacity(funcs.len());
    for i in 0..funcs.len() {
        offsets_aggregate_states.push(total_size_aggregate_states);
        let layout = funcs[i].state_layout();

        total_size_aggregate_states += layout.size();
        align_aggregate_states = align_aggregate_states.max(layout.align());
        // Not the last aggregate_state
        if i + 1 < funcs.len() {
            let next_layout = funcs[i + 1].state_layout();
            total_size_aggregate_states = (total_size_aggregate_states + next_layout.align() - 1)
                / next_layout.align()
                * next_layout.align();
        }
    }
    let layout =
        Layout::from_size_align_unchecked(total_size_aggregate_states, align_aggregate_states);
    (layout, offsets_aggregate_states)
}
