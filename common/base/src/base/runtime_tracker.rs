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
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[thread_local]
static mut TRACKER: *mut ThreadTracker = std::ptr::null_mut();

static UNTRACKED_MEMORY_LIMIT: i64 = 4 * 1024 * 1024;

pub struct ThreadTracker {
    rt_tracker: Arc<RuntimeTracker>,
    untracked_memory: i64,
}

impl ThreadTracker {
    pub fn create(rt_tracker: Arc<RuntimeTracker>) -> *mut ThreadTracker {
        unsafe {
            TRACKER = Box::into_raw(Box::new(ThreadTracker {
                rt_tracker,
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
    pub fn current_runtime_tracker() -> Option<Arc<RuntimeTracker>> {
        unsafe {
            match TRACKER.is_null() {
                true => None,
                false => Some((*TRACKER).rt_tracker.clone()),
            }
        }
    }

    #[inline]
    pub fn alloc_memory(size: i64) {
        unsafe {
            if !TRACKER.is_null() {
                (*TRACKER).untracked_memory += size;

                if (*TRACKER).untracked_memory > UNTRACKED_MEMORY_LIMIT {
                    (*TRACKER)
                        .rt_tracker
                        .memory_tracker
                        .alloc_memory((*TRACKER).untracked_memory);
                    (*TRACKER).untracked_memory = 0;
                }
            }
        }
    }

    #[inline]
    pub fn dealloc_memory(size: i64) {
        unsafe {
            if !TRACKER.is_null() {
                (*TRACKER).untracked_memory -= size;

                if (*TRACKER).untracked_memory < -UNTRACKED_MEMORY_LIMIT {
                    (*TRACKER)
                        .rt_tracker
                        .memory_tracker
                        .dealloc_memory(-(*TRACKER).untracked_memory);
                    (*TRACKER).untracked_memory = 0;
                }
            }
        }
    }

    #[inline]
    pub fn realloc_memory(old_size: i64, new_size: i64) {
        let addition = new_size - old_size;
        match addition > 0 {
            true => Self::alloc_memory(addition),
            false => Self::dealloc_memory(-addition),
        }
    }
}

pub struct MemoryTracker {
    memory_usage: AtomicI64,
    parent_memory_tracker: Option<Arc<MemoryTracker>>,
}

impl MemoryTracker {
    pub fn create(parent_memory_tracker: Option<Arc<MemoryTracker>>) -> Arc<MemoryTracker> {
        Arc::new(MemoryTracker {
            parent_memory_tracker,
            memory_usage: AtomicI64::new(0),
        })
    }

    #[inline]
    pub fn alloc_memory(&self, size: i64) {
        self.memory_usage.fetch_add(size, Ordering::Relaxed);

        if let Some(parent_memory_tracker) = &self.parent_memory_tracker {
            parent_memory_tracker.alloc_memory(size);
        }
    }

    #[inline]
    pub fn dealloc_memory(&self, size: i64) {
        self.memory_usage.fetch_sub(size, Ordering::Relaxed);

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
                false => Some((*thread_tracker).rt_tracker.memory_tracker.clone()),
            }
        }
    }

    #[inline]
    pub fn get_memory_usage(&self) -> i64 {
        self.memory_usage.load(Ordering::Relaxed)
    }
}

pub struct RuntimeTracker {
    memory_tracker: Arc<MemoryTracker>,
}

impl RuntimeTracker {
    pub fn create() -> Arc<RuntimeTracker> {
        let parent_memory_tracker = MemoryTracker::current();
        Arc::new(RuntimeTracker {
            memory_tracker: MemoryTracker::create(parent_memory_tracker),
        })
    }

    #[inline]
    pub fn get_memory_tracker(&self) -> &MemoryTracker {
        &self.memory_tracker
    }

    pub fn on_stop_thread(self: &Arc<Self>) -> impl Fn() {
        move || unsafe {
            let tracker = std::mem::replace(&mut TRACKER, std::ptr::null_mut());

            std::ptr::drop_in_place(tracker as usize as *mut ThreadTracker);
            std::alloc::dealloc(tracker as *mut u8, Layout::new::<ThreadTracker>());
        }
    }

    pub fn on_start_thread(self: &Arc<Self>) -> impl Fn() {
        // TODO: log::info("thread {}-{} started", thread_id, thread_name);
        let rt_tracker = self.clone();

        move || {
            ThreadTracker::create(rt_tracker.clone());
        }
    }
}
