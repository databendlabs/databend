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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[thread_local]
static mut TRACKER: *const ThreadTracker = std::ptr::null();

pub struct ThreadTracker {
    rt_tracker: Arc<RuntimeTracker>,
}

impl ThreadTracker {
    pub fn create(rt_tracker: Arc<RuntimeTracker>) -> *mut ThreadTracker {
        Box::into_raw(Box::new(ThreadTracker { rt_tracker }))
    }

    #[inline]
    pub fn current() -> *const ThreadTracker {
        unsafe { TRACKER }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn set_current(value: *const ThreadTracker) {
        unsafe { TRACKER = value }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn get_memory_tracker(pointer: *const Self) -> Arc<MemoryTracker> {
        debug_assert!(!pointer.is_null());
        unsafe { (*pointer).rt_tracker.get_memory_tracker() }
    }
}

pub struct MemoryTracker {
    memory_usage: AtomicUsize,
    parent_memory_tracker: Option<Arc<MemoryTracker>>,
}

impl MemoryTracker {
    pub fn create(parent_memory_tracker: Option<Arc<MemoryTracker>>) -> Arc<MemoryTracker> {
        Arc::new(MemoryTracker {
            parent_memory_tracker,
            memory_usage: AtomicUsize::new(0),
        })
    }

    #[inline]
    pub fn alloc_memory(&self, size: usize) {
        self.memory_usage.fetch_add(size, Ordering::Relaxed);

        if let Some(parent_memory_tracker) = &self.parent_memory_tracker {
            parent_memory_tracker.alloc_memory(size);
        }
    }

    #[inline]
    pub fn dealloc_memory(&self, size: usize) {
        self.memory_usage.fetch_sub(size, Ordering::Relaxed);

        if let Some(parent_memory_tracker) = &self.parent_memory_tracker {
            parent_memory_tracker.dealloc_memory(size);
        }
    }

    #[inline]
    pub fn realloc_memory(&self, old_size: usize, new_size: usize) {
        self.memory_usage.fetch_sub(old_size, Ordering::Relaxed);
        self.memory_usage.fetch_add(new_size, Ordering::Relaxed);

        if let Some(parent_memory_tracker) = &self.parent_memory_tracker {
            parent_memory_tracker.realloc_memory(old_size, new_size);
        }
    }

    pub fn current() -> Option<Arc<MemoryTracker>> {
        let thread_trckcer = ThreadTracker::current();
        match thread_trckcer.is_null() {
            true => None,
            false => Some(ThreadTracker::get_memory_tracker(thread_trckcer)),
        }
    }

    pub fn get_memory_usage(&self) -> usize {
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

    pub fn get_memory_tracker(&self) -> Arc<MemoryTracker> {
        self.memory_tracker.clone()
    }

    pub fn on_stop_thread(self: &Arc<Self>) -> impl Fn() {
        move || unsafe {
            let tracker = std::mem::replace(&mut TRACKER, std::ptr::null());

            std::ptr::drop_in_place(tracker as usize as *mut ThreadTracker);
            std::alloc::dealloc(tracker as *mut u8, Layout::new::<ThreadTracker>());
        }
    }

    pub fn on_start_thread(self: &Arc<Self>) -> impl Fn() {
        // TODO: log::info("thread {}-{} started", thread_id, thread_name);
        let rt_tracker = self.clone();

        move || {
            let thread_tracker = ThreadTracker::create(rt_tracker.clone());
            ThreadTracker::set_current(thread_tracker);
        }
    }
}
