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

//! Real allocator -> memory manager -> shared store -> tracked deallocation. Separate binary
//! because the allocator, memory limits and reclaim handler are process-global.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use databend_common_base::mem_allocator::TrackingGlobalAllocator;
use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_base::runtime::GLOBAL_QUERIES_MANAGER;
use databend_common_base::runtime::GlobalStatBuffer;
use databend_common_base::runtime::LimitMemGuard;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::MemStatBuffer;
use databend_common_base::runtime::Thread;
use databend_common_base::runtime::ThreadTracker;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CachedItem;
use databend_storages_common_cache::MemCache;
use databend_storages_common_cache::PoolCaps;
use databend_storages_common_cache::backend::KindStore;
use databend_storages_common_cache::kinds::table;
use databend_storages_common_cache::shared::SharedStore;

thread_local! {
    // Enabled only around memory-manager operations; count allocations and unguarded ones.
    static CHECK_GUARD: Cell<(bool, usize, usize)> = const { Cell::new((false, 0, 0)) };
}

fn check_guard() {
    let _ = CHECK_GUARD.try_with(|c| {
        let (active, allocations, unguarded) = c.get();
        if active {
            c.set((
                true,
                allocations + 1,
                unguarded + usize::from(!LimitMemGuard::is_unlimited()),
            ));
        }
    });
}

struct CheckedAllocator;
static TRACKING: TrackingGlobalAllocator = TrackingGlobalAllocator::create();

unsafe impl GlobalAlloc for CheckedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        check_guard();
        unsafe { TRACKING.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        check_guard();
        unsafe { TRACKING.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        check_guard();
        unsafe { TRACKING.realloc(ptr, layout, size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { TRACKING.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CheckedAllocator = CheckedAllocator;

fn assert_guarded_allocations(f: impl FnOnce()) {
    CHECK_GUARD.with(|c| c.set((true, 0, 0)));
    f();
    let (_, allocations, unguarded) = CHECK_GUARD.with(|c| c.replace((false, 0, 0)));
    assert!(
        allocations > 0,
        "the operation did not exercise an allocation"
    );
    assert_eq!(
        unguarded, 0,
        "memory-manager allocation could reenter reclaim"
    );
}

struct Blob(Vec<u8>);

impl CachedItem for Blob {
    fn mem_bytes(&self) -> usize {
        self.0.capacity()
    }
}

fn flush() {
    let _guard = LimitMemGuard::enter_unlimited();
    MemStatBuffer::current().flush::<false>(0).unwrap();
    GlobalStatBuffer::current().flush::<false>(0).unwrap();
}

/// Simulate other live memory rather than physically allocating the minimum 256 MiB limit.
/// The cache buffers, requesting allocation and their allocation metadata are all real.
struct Pressure {
    bytes: i64,
    limit: usize,
}

impl Pressure {
    fn new() -> Self {
        flush();
        let bytes = 256 << 20;
        let limit = GLOBAL_MEM_STAT.get_memory_usage() + bytes as usize;
        GLOBAL_MEM_STAT.set_limit(limit as i64, false);
        let _guard = LimitMemGuard::enter_unlimited();
        GLOBAL_MEM_STAT.record_memory::<false>(bytes, 0).unwrap();
        Self { bytes, limit }
    }
}

impl Drop for Pressure {
    fn drop(&mut self) {
        let _guard = LimitMemGuard::enter_unlimited();
        GLOBAL_MEM_STAT.set_limit(0, false);
        GLOBAL_MEM_STAT.rollback(self.bytes);
    }
}

fn exercise() {
    ThreadTracker::init();
    let store = Arc::new(SharedStore::new(
        PoolCaps {
            meta: 8 << 20,
            index: 0,
            data: 0,
        },
        [],
        1,
    ));
    let cache = MemCache::new(store.clone(), CacheKind::of::<table::Snapshot>());
    let calls = Arc::new(AtomicUsize::new(0));
    let kills = Arc::new(AtomicUsize::new(0));
    GLOBAL_QUERIES_MANAGER.set_gc_handle({
        let kills = kills.clone();
        move |_, _| {
            kills.fetch_add(1, Ordering::Relaxed);
            true
        }
    });
    GLOBAL_QUERIES_MANAGER.set_reclaim_handle({
        let (store, calls) = (store.clone(), calls.clone());
        move |relieved| {
            assert!(LimitMemGuard::is_unlimited());
            calls.fetch_add(1, Ordering::Relaxed);
            store.shrink_for_pressure(relieved).freed
        }
    });

    // Both operations really allocate in the manager: resource registration, then the first
    // insertion in killed_resources during cleanup. Neither may recursively start reclaim.
    let tag = "guarded-memory-manager".to_string();
    let stat = MemStat::create(tag.clone());
    assert_guarded_allocations(|| {
        GLOBAL_QUERIES_MANAGER.request_exceeded_memory(&stat, Some(&tag))
    });
    assert_guarded_allocations(|| GLOBAL_QUERIES_MANAGER.drop_memory_stat(Some(&tag)));
    drop(stat);

    // Allocate the queue's reusable slots outside a query. Only the payload allocation
    // below should retain the finished loader's MemStat.
    cache.put("tracked", Blob(Vec::new()));
    assert!(cache.evict("tracked"));
    for queried in [true, false] {
        let owner = MemStat::create("finished-cache-loader".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(owner.clone());
        {
            let tracking = ThreadTracker::tracking(payload);
            assert!(cache.put("tracked", Blob(vec![0; 1 << 20])).is_charged());
            tracking.flush().unwrap();
        }
        flush();
        // No Weak: allocator metadata requires MemStat to have no weak references. After
        // dropping this handle, only allocations retained by the cache own the finished stat.
        assert_eq!(
            Arc::strong_count(&owner),
            2,
            "only the cached buffer should retain the stat"
        );
        drop(owner);

        let requester = MemStat::create("requesting-query".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        if queried {
            payload.mem_stat = Some(requester);
        }
        let before = calls.load(Ordering::Relaxed);
        let pressure = Pressure::new();
        {
            let tracking = ThreadTracker::tracking(payload);
            let allocation = vec![0u8; 512 << 10];
            // This flush crosses the global limit. Reclaim must free tracked cache memory,
            // including its last MemStat references, then observe the buffered frees.
            tracking.flush().unwrap();
            assert_eq!(calls.load(Ordering::Relaxed), before + 1);
            assert_eq!(
                store
                    .kind_stats(CacheKind::of::<table::Snapshot>())
                    .resident,
                0
            );
            assert!(GLOBAL_MEM_STAT.get_memory_usage() <= pressure.limit);
            std::hint::black_box(&allocation);
        }
        drop(pressure);
        flush();
    }
    assert_eq!(kills.load(Ordering::Relaxed), 0);
    assert_eq!(store.reclaim_stats().unwrap().freed_bytes, 2 << 20);
    GLOBAL_QUERIES_MANAGER.set_reclaim_handle(|_| 0);
}

#[test]
fn test_tracked_reclaim_and_memory_manager_guards() {
    let (done, result) = mpsc::channel();
    let thread = Thread::spawn(move || {
        exercise();
        done.send(()).unwrap();
    });
    result
        .recv_timeout(Duration::from_secs(30))
        .expect("tracked reclaim did not finish");
    thread.join().unwrap();
}
