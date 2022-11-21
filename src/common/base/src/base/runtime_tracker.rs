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

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::mem_allocator::GlobalAllocator;

#[thread_local]
static mut TRACKER: *mut ThreadTracker = std::ptr::null_mut();

static UNTRACKED_MEMORY_LIMIT: i64 = 4 * 1024 * 1024;

pub struct ThreadTracker {
    mem_tracker: Arc<MemoryTracker>,
    untracked_memory: i64,
}

impl ThreadTracker {
    pub fn create(mem_tracker: Arc<MemoryTracker>) -> *mut ThreadTracker {
        unsafe {
            TRACKER = Box::into_raw(Box::new(ThreadTracker {
                mem_tracker,
                untracked_memory: 0,
            }));

            TRACKER
        }
    }

    #[inline]
    pub fn current() -> *mut ThreadTracker {
        unsafe { TRACKER }
    }

    #[inline]
    pub fn current_mem_tracker() -> Option<Arc<MemoryTracker>> {
        unsafe {
            match TRACKER.is_null() {
                true => None,
                false => Some((*TRACKER).mem_tracker.clone()),
            }
        }
    }

    /// Accumulate allocated memory.
    ///
    /// `size` is the positive number of allocated bytes.
    /// `p` is the pointer to the allocated memory.
    #[inline]
    pub fn alloc_memory<T: ?Sized>(size: i64, p: &NonNull<T>) {
        let _ = p;

        unsafe {
            if !TRACKER.is_null() {
                (*TRACKER).untracked_memory += size;

                if (*TRACKER).untracked_memory > UNTRACKED_MEMORY_LIMIT {
                    (*TRACKER)
                        .mem_tracker
                        .alloc_memory((*TRACKER).untracked_memory);
                    (*TRACKER).untracked_memory = 0;
                }
            }
        }
    }

    /// Accumulate deallocated memory.
    ///
    /// `size` is positive number of bytes of the memory to deallocate.
    /// `p` is the pointer to the memory to deallocate.
    #[inline]
    pub fn dealloc_memory<T>(size: i64, p: &NonNull<T>) {
        // size > 0
        let _ = p;

        unsafe {
            if !TRACKER.is_null() {
                (*TRACKER).untracked_memory -= size;

                if (*TRACKER).untracked_memory < -UNTRACKED_MEMORY_LIMIT {
                    (*TRACKER)
                        .mem_tracker
                        .dealloc_memory(-(*TRACKER).untracked_memory);
                    (*TRACKER).untracked_memory = 0;
                }
            }
        }
    }
}

pub struct MemoryTracker {
    /// Count of calls to `alloc`.
    n_alloc: AtomicI64,

    /// Number of allocated bytes.
    bytes_alloc: AtomicI64,

    /// Count of calls to `dealloc`.
    n_dealloc: AtomicI64,

    /// Number of deallocated bytes.
    bytes_dealloc: AtomicI64,

    parent_memory_tracker: Option<Arc<MemoryTracker>>,
}

impl MemoryTracker {
    pub fn create() -> Arc<MemoryTracker> {
        let parent = MemoryTracker::current();
        MemoryTracker::create_sub_tracker(parent)
    }

    pub fn create_sub_tracker(
        parent_memory_tracker: Option<Arc<MemoryTracker>>,
    ) -> Arc<MemoryTracker> {
        Arc::new(MemoryTracker {
            parent_memory_tracker,
            n_alloc: AtomicI64::new(0),
            bytes_alloc: AtomicI64::new(0),
            n_dealloc: AtomicI64::new(0),
            bytes_dealloc: AtomicI64::new(0),
        })
    }

    #[inline]
    pub fn alloc_memory(&self, size: i64) {
        self.bytes_alloc.fetch_add(size, Ordering::Relaxed);
        self.n_alloc.fetch_add(1, Ordering::Relaxed);

        if let Some(parent_memory_tracker) = &self.parent_memory_tracker {
            parent_memory_tracker.alloc_memory(size);
        }
    }

    #[inline]
    pub fn dealloc_memory(&self, size: i64) {
        self.bytes_dealloc.fetch_add(size, Ordering::Relaxed);
        self.n_dealloc.fetch_add(1, Ordering::Relaxed);

        if let Some(parent_memory_tracker) = &self.parent_memory_tracker {
            parent_memory_tracker.dealloc_memory(size);
        }
    }

    #[inline]
    pub fn current() -> Option<Arc<MemoryTracker>> {
        unsafe {
            let thread_tracker = ThreadTracker::current();
            match thread_tracker.is_null() {
                true => None,
                false => Some((*thread_tracker).mem_tracker.clone()),
            }
        }
    }

    #[inline]
    pub fn get_memory_usage(&self) -> i64 {
        self.bytes_alloc.load(Ordering::Relaxed) - self.bytes_dealloc.load(Ordering::Relaxed)
    }
}

impl MemoryTracker {
    pub fn on_stop_thread(self: &Arc<Self>) -> impl Fn() {
        move || unsafe {
            let thread_tracker = std::mem::replace(&mut TRACKER, std::ptr::null_mut());

            std::ptr::drop_in_place(thread_tracker as usize as *mut ThreadTracker);
            GlobalAllocator.dealloc(thread_tracker as *mut u8, Layout::new::<ThreadTracker>())
        }
    }

    pub fn on_start_thread(self: &Arc<Self>) -> impl Fn() {
        // TODO: log::info("thread {}-{} started", thread_id, thread_name);
        let mem_tracker = self.clone();

        move || {
            ThreadTracker::create(mem_tracker.clone());
        }
    }
}
