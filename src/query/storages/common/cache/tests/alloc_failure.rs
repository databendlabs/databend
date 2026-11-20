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

//! Allocation failure across the typed handle, two budgets and shared queues. Own binary
//! for its allocator and hooks; the failures are injected one at a time, not during cleanup.

#![feature(alloc_error_hook)]

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_common_base::runtime::catch_unwind;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CachedItem;
use databend_storages_common_cache::KindLimit;
use databend_storages_common_cache::MemCache;
use databend_storages_common_cache::Pool;
use databend_storages_common_cache::PoolCaps;
use databend_storages_common_cache::backend::KindStore;
use databend_storages_common_cache::kinds::segment;
use databend_storages_common_cache::kinds::table;
use databend_storages_common_cache::shared::SharedStore;

thread_local! {
    static FAIL_IN: Cell<usize> = const { Cell::new(0) };
}

fn fail_now() -> bool {
    FAIL_IN.with(|n| n.get() > 0 && n.replace(n.get() - 1) == 1)
}

struct FailingAlloc;

unsafe impl GlobalAlloc for FailingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if fail_now() {
            return std::ptr::null_mut();
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if fail_now() {
            return std::ptr::null_mut();
        }
        unsafe { System.realloc(ptr, layout, size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: FailingAlloc = FailingAlloc;

#[derive(Default)]
struct Live {
    items: AtomicUsize,
    bytes: AtomicUsize,
}

struct Value {
    live: Arc<Live>,
    bytes: usize,
}

impl CachedItem for Value {
    fn mem_bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for Value {
    fn drop(&mut self) {
        self.live.items.fetch_sub(1, Ordering::Relaxed);
        self.live.bytes.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

const CAP: usize = 48_000;

struct Fixture {
    store: Arc<SharedStore>,
    cache: MemCache<Value>,
    other: MemCache<Value>,
    live: Arc<Live>,
}

impl Fixture {
    fn new() -> Self {
        let store = Arc::new(SharedStore::new(
            PoolCaps {
                meta: CAP,
                index: 0,
                data: 0,
            },
            [],
            1,
        ));
        Self {
            cache: MemCache::new(store.clone(), CacheKind::of::<table::Snapshot>()),
            other: MemCache::new(store.clone(), CacheKind::of::<segment::Info>()),
            store,
            live: Arc::new(Live::default()),
        }
    }

    fn value(&self, bytes: usize) -> Value {
        self.live.items.fetch_add(1, Ordering::Relaxed);
        self.live.bytes.fetch_add(bytes, Ordering::Relaxed);
        Value {
            live: self.live.clone(),
            bytes,
        }
    }

    fn filled(other_kind: bool) -> Self {
        let f = Self::new();
        let cache = if other_kind { &f.other } else { &f.cache };
        for i in 0..3000 {
            cache.put(&i.to_string(), f.value(16));
        }
        f
    }

    fn check(&self) {
        let pool = self.store.pool_stats()[Pool::Meta.index()];
        let a = self.store.kind_stats(CacheKind::of::<table::Snapshot>());
        let b = self.store.kind_stats(CacheKind::of::<segment::Info>());
        assert_eq!(pool.resident, pool.in_cache);
        assert_eq!(pool.resident, a.resident + b.resident);
        assert_eq!(pool.items, a.items + b.items);
        assert_eq!(pool.resident, self.live.bytes.load(Ordering::Relaxed));
        assert_eq!(pool.items, self.live.items.load(Ordering::Relaxed));
        assert!(pool.resident <= CAP);
        assert_eq!((a.resident, b.resident), (a.in_cache, b.in_cache));
    }
}

fn fail_each_allocation(build: impl Fn() -> Fixture, op: impl Fn(&Fixture)) {
    for nth in 1.. {
        let f = build();
        FAIL_IN.with(|n| n.set(nth));
        let result = catch_unwind(|| op(&f));
        let failed = FAIL_IN.with(|n| n.replace(0)) == 0;
        // An interrupted eviction may have removed entries. Its remaining values and both
        // budgets must still agree, rather than rolling back already-completed evictions.
        f.check();
        f.store.set_pool_cap(Pool::Meta, 0).unwrap();
        f.check();
        assert_eq!(f.live.items.load(Ordering::Relaxed), 0);
        f.store
            .set_kind_limit(CacheKind::of::<table::Snapshot>(), KindLimit::UNLIMITED);
        f.store.set_pool_cap(Pool::Meta, CAP).unwrap();
        assert!(f.cache.put("after-unwind", f.value(16)).is_charged());
        f.check();
        if !failed {
            assert!(result.is_ok());
            break;
        }
    }
}

#[test]
fn test_shared_store_allocation_failures_preserve_budgets() {
    std::alloc::set_alloc_error_hook(|_| panic!("injected allocation failure"));
    let old_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    // Key allocation and queue growth after both budgets have been charged.
    fail_each_allocation(Fixture::new, |f| {
        for i in 0..100 {
            f.cache.put(&i.to_string(), f.value(16));
        }
    });
    // Pool eviction while the incoming kind's charge is already held.
    fail_each_allocation(
        || Fixture::filled(true),
        |f| {
            f.cache.put("new-kind", f.value(16));
        },
    );
    // Kind turnover, replacement, and compaction during bulk removal.
    fail_each_allocation(
        || Fixture::filled(false),
        |f| {
            f.store
                .set_kind_limit(CacheKind::of::<table::Snapshot>(), KindLimit::bytes(CAP));
            f.cache.put("2999", f.value(32));
        },
    );
    fail_each_allocation(
        || Fixture::filled(false),
        |f| {
            f.store
                .set_kind_limit(CacheKind::of::<table::Snapshot>(), KindLimit::DISABLED);
        },
    );

    std::panic::set_hook(old_hook);
    let _ = std::alloc::take_alloc_error_hook();
}
