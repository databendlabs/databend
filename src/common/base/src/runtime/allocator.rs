// Copyright 2022 Datafuse Labs.
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
// Borrow from https://github.com/near/near-memory-tracker.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::runtime::MemoryTracker;

const COUNTERS_SIZE: usize = 16384;
static MEM_SIZE: [AtomicUsize; COUNTERS_SIZE] = unsafe {
    // SAFETY: Rust [guarantees](https://doc.rust-lang.org/stable/std/sync/atomic/struct.AtomicUsize.html)
    // that `usize` and `AtomicUsize` have the same representation.
    std::mem::transmute::<[usize; COUNTERS_SIZE], [AtomicUsize; COUNTERS_SIZE]>(
        [0_usize; COUNTERS_SIZE],
    )
};
static MEM_CNT: [AtomicUsize; COUNTERS_SIZE] = unsafe {
    std::mem::transmute::<[usize; COUNTERS_SIZE], [AtomicUsize; COUNTERS_SIZE]>(
        [0_usize; COUNTERS_SIZE],
    )
};

#[must_use]
pub fn get_tid() -> usize {
    MemoryTracker::get_id()
}

#[must_use]
pub fn total_memory_usage() -> usize {
    MEM_SIZE.iter().map(|v| v.load(Ordering::Relaxed)).sum()
}

pub fn current_thread_memory_usage() -> usize {
    let tid = get_tid();

    MEM_SIZE[tid % COUNTERS_SIZE].load(Ordering::Relaxed)
}

pub fn thread_memory_usage(tid: usize) -> usize {
    MEM_SIZE[tid % COUNTERS_SIZE].load(Ordering::Relaxed)
}

pub fn thread_memory_count(tid: usize) -> usize {
    MEM_CNT[tid % COUNTERS_SIZE].load(Ordering::Relaxed)
}

pub struct ProxyAllocator<A> {
    inner: A,
}

impl<A> ProxyAllocator<A> {
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }
}

unsafe impl<A: GlobalAlloc> GlobalAlloc for ProxyAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let tid = get_tid();
        let slot = tid % COUNTERS_SIZE;
        MEM_SIZE[slot].fetch_add(layout.size(), Ordering::Relaxed);
        MEM_CNT[slot].fetch_add(1, Ordering::Relaxed);

        self.inner.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let tid = get_tid();
        let slot = tid % COUNTERS_SIZE;
        MEM_SIZE[slot].fetch_sub(layout.size(), Ordering::Relaxed);
        MEM_CNT[slot].fetch_sub(1, Ordering::Relaxed);
        self.inner.dealloc(ptr, layout);
    }
}

pub fn print_memory_stats() {
    tracing::info!(tid = get_tid(), "tid");
    let mut total_cnt: usize = 0;
    let mut total_size: usize = 0;
    for idx in 0..COUNTERS_SIZE {
        let val = MEM_SIZE[idx].load(Ordering::Relaxed);
        if val != 0 {
            let cnt = MEM_CNT[idx].load(Ordering::Relaxed);
            total_cnt += cnt;
            total_size += val;

            tracing::info!(idx, cnt, val, "COUNTERS");
        }
    }
    tracing::info!(total_cnt, total_size, "COUNTERS TOTAL");
}
