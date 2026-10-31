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

//! The long soak of the shared backend: the phased churn of `shared::soak`, for as long as
//! `CACHE_SOAK_SECS` says (default 60), also checking that the process heap levels off and comes
//! back once the caches are off, and that throughput holds. Its own test binary, so the heap is
//! the soak's alone. Ignored by default:
//!
//! `CACHE_SOAK_SECS=1800 cargo test -p databend-storages-common-cache --test soak -- --ignored --nocapture`

#[path = "it/shared/soak_engine.rs"]
mod soak_engine;

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::sync::atomic::AtomicIsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use soak_engine::Phases;
use soak_engine::SoakConfig;
use soak_engine::soak;

/// Bytes allocated and not freed, by the whole process.
static HEAP: AtomicIsize = AtomicIsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        HEAP.fetch_add(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        HEAP.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        HEAP.fetch_add(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        HEAP.fetch_add(
            new_size as isize - layout.size() as isize,
            Ordering::Relaxed,
        );
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const MIB: isize = 1 << 20;
const WARMUP_PHASES: usize = 5;

fn heap() -> isize {
    HEAP.load(Ordering::Relaxed)
}

#[test]
#[ignore]
fn soak_shared_backend() {
    let secs = std::env::var("CACHE_SOAK_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let start_heap = heap();
    let mut warm_heap = 0;
    let mut first_rate = None;
    let reports = soak(
        SoakConfig {
            threads: 16,
            phases: Phases::For(Duration::from_secs(secs)),
            ops_per_phase: 20_000,
            cap: 64 << 20,
            keys: 20_000,
            seed: 7,
        },
        |report| {
            let heap_mib = (heap() - start_heap) / MIB;
            let cached: usize = report.in_cache.iter().sum();
            let structure = |i: usize| {
                let s = report.structures[i];
                format!("{}/{}/{}", s.entries, s.queue_positions, s.ghost_entries)
            };
            println!(
                "phase {:4} {:8.0} ops/s  heap {:5} MiB  cached {:4} MiB  alive {:7}  \
                 entries/queue/ghost meta {} index {} data {}  samples {}",
                report.phase,
                report.ops_per_sec(),
                heap_mib,
                cached >> 20,
                report.live_values,
                structure(0),
                structure(1),
                structure(2),
                report.monitor_samples,
            );
            if report.phase + 1 == WARMUP_PHASES {
                warm_heap = heap() - start_heap;
                first_rate = Some(report.ops_per_sec());
            }
            if report.phase >= WARMUP_PHASES {
                // The caches are full after warming up: the heap levels off, with room for the
                // pools growing to twice their starting cap.
                let limit = warm_heap * 2 + 64 * MIB;
                assert!(
                    heap() - start_heap <= limit,
                    "heap grows: {heap_mib} MiB, warm {} MiB",
                    warm_heap / MIB
                );
                let rate = report.ops_per_sec();
                assert!(
                    rate >= first_rate.unwrap() / 4.0,
                    "throughput fell from {:.0} to {rate:.0} ops/s",
                    first_rate.unwrap()
                );
            }
        },
    );
    assert!(
        reports.len() > WARMUP_PHASES,
        "too short to check: {} phases",
        reports.len()
    );
    // The soak turned every pool off: what it allocated is back, but for thread and
    // allocator leftovers.
    let left = heap() - start_heap;
    println!("left after the soak: {} KiB", left >> 10);
    assert!(left < 16 * MIB, "{} MiB not freed", left / MIB);
}
