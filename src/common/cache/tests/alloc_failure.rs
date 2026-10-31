// Copyright 2026 Datafuse Labs
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

//! Allocation failures inside S3-FIFO operations, which panic in Databend as here: the
//! structure must stay consistent. Own binary, for its allocator.

#![feature(alloc_error_hook)]

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::cell::Cell;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use databend_common_cache::S3Fifo;
use databend_common_cache::S3FifoConfig;

thread_local! {
    /// Allocations left before one fails; 0 when none is to fail.
    static FAIL_IN: Cell<usize> = const { Cell::new(0) };
}

struct FailingAlloc;

unsafe impl GlobalAlloc for FailingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if FAIL_IN.with(|n| n.get() > 0 && n.replace(n.get() - 1) == 1) {
            return std::ptr::null_mut();
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if FAIL_IN.with(|n| n.get() > 0 && n.replace(n.get() - 1) == 1) {
            return std::ptr::null_mut();
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOC: FailingAlloc = FailingAlloc;

const CLASSES: usize = 2;

type Fifo = S3Fifo<u32, u32>;

fn class(key: u32) -> usize {
    key as usize % CLASSES
}

/// Runs `op` on a cache built by `build`, failing its 1st, 2nd, ... allocation until it
/// completes without reaching it. After each failure the cache must be consistent and stay
/// usable.
fn fail_each_allocation(build: impl Fn() -> Fifo, op: impl Fn(&mut Fifo)) {
    std::alloc::set_alloc_error_hook(|_| panic!("allocation failed"));
    std::panic::set_hook(Box::new(|_| {}));
    for nth in 1.. {
        let mut c = build();
        FAIL_IN.with(|n| n.set(nth));
        // Not `common_base::runtime::catch_unwind`: this crate does not depend on it, and the
        // plain one is all a test needs.
        #[allow(clippy::disallowed_methods)]
        let result = catch_unwind(AssertUnwindSafe(|| op(&mut c)));
        let failed = FAIL_IN.with(|n| n.replace(0)) == 0;
        check(&mut c);
        // A failed allocation panics, unless it was fallible (the ghost's) and given up.
        if !failed {
            assert!(result.is_ok());
            break;
        }
    }
    let _ = std::panic::take_hook();
}

/// Lookups, counts and the eviction order agree; then the cache still takes entries and
/// evicts every one of them.
fn check(c: &mut Fifo) {
    let entries: Vec<(u32, u32)> = c.iter().map(|(k, v)| (*k, *v)).collect();
    assert_eq!(entries.len(), c.len());
    assert!(entries.iter().all(|(k, v)| c.peek(k) == Some(v)));
    let class_bytes: usize = (0..CLASSES).map(|class| c.class_bytes(class)).sum();
    let class_len: usize = (0..CLASSES).map(|class| c.class_len(class)).sum();
    assert_eq!((class_len, class_bytes), (c.len(), c.bytes()));

    c.insert_in(class(1_000_000), 1_000_000, 0, 1);
    let mut bytes = 0;
    let mut popped = 0;
    while let Some((key, _, size)) = c.pop_evictable(|_| false, usize::MAX) {
        assert!(!c.contains(&key));
        bytes += size;
        popped += 1;
    }
    assert_eq!((popped, bytes), (entries.len() + 1, class_bytes + 1));
    assert!(c.is_empty() && c.bytes() == 0);
}

fn filled(entries: u32) -> Fifo {
    let mut c = S3Fifo::new(S3FifoConfig {
        classes: CLASSES,
        ghost_entries: 64,
        ..Default::default()
    });
    for k in 0..entries {
        c.insert_in(class(k), k, k, 1 + k as usize % 7);
    }
    for k in (0..entries).step_by(3) {
        c.get(&k);
    }
    c
}

#[test]
fn test_allocation_failures_leave_s3fifo_consistent() {
    // Growing the table, the slab and the free list.
    fail_each_allocation(
        || filled(0),
        |c| {
            for k in 0..3000 {
                c.insert_in(class(k), k, k, 1);
            }
        },
    );
    // Removals that compact the slab.
    fail_each_allocation(
        || filled(3000),
        |c| {
            for k in 0..2900 {
                c.remove(&k);
            }
        },
    );
    fail_each_allocation(
        || filled(3000),
        |c| {
            c.remove_if(|k, _| *k > 10);
        },
    );
    fail_each_allocation(
        || filled(3000),
        |c| {
            c.remove_class(1);
        },
    );
    // Evictions, which record in the ghost and return their victims.
    fail_each_allocation(
        || filled(3000),
        |c| {
            c.evict_class(0, |v| v % 5 == 0, usize::MAX, usize::MAX);
        },
    );
    fail_each_allocation(
        || filled(3000),
        |c| while c.pop_evictable(|v| v % 5 == 0, 64).is_some() {},
    );
}

/// Pressure eviction must not grow the ghost or rebuild a sparse slab, even when those
/// allocations could succeed under the memory manager's unlimited guard.
#[test]
fn test_pressure_eviction_does_not_allocate() {
    let mut c = filled(3000);
    let slots = c.slots();
    let mut steps = 30_000;
    FAIL_IN.with(|n| n.set(1));
    #[allow(clippy::disallowed_methods)]
    let result = catch_unwind(AssertUnwindSafe(|| {
        while c.pop_evictable_bounded(|_| false, 64, &mut steps).is_some() {}
    }));
    let remaining = FAIL_IN.with(|n| n.replace(0));
    assert!(result.is_ok());
    assert_eq!(remaining, 1, "pressure eviction attempted an allocation");
    assert!(c.is_empty());
    assert_eq!(c.ghost_len(), 0);
    assert_eq!(c.slots(), slots, "pressure eviction compacted the slab");
}
