// Copyright 2020 Datafuse Labs.
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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[thread_local]
static mut TRACKER: *const ThreadTracker = std::ptr::null();

pub struct ThreadTracker {
    rt_tracker: Arc<RuntimeTracker>,
    parent_tracker: *const ThreadTracker,
}

impl ThreadTracker {
    pub fn create(rt_tracker: Arc<RuntimeTracker>) -> *mut ThreadTracker {
        Box::into_raw(Box::new(ThreadTracker {
            rt_tracker,
            parent_tracker: std::ptr::null(),
        }))
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
    pub fn alloc_memory(pointer: *const Self, size: usize) {
        unsafe {
            if !pointer.is_null() {
                (*pointer)
                    .rt_tracker
                    .memory_usage
                    .fetch_add(size, Ordering::Relaxed);
            }

            Self::alloc_memory((*pointer).parent_tracker, size);
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn dealloc_memory(pointer: *const Self, size: usize) {
        unsafe {
            if !pointer.is_null() {
                (*pointer)
                    .rt_tracker
                    .memory_usage
                    .fetch_sub(size, Ordering::Relaxed);
            }

            Self::dealloc_memory((*pointer).parent_tracker, size);
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn realloc_memory(pointer: *const Self, old_size: usize, new_size: usize) {
        unsafe {
            if !pointer.is_null() {
                (*pointer)
                    .rt_tracker
                    .memory_usage
                    .fetch_sub(old_size, Ordering::Relaxed);

                (*pointer)
                    .rt_tracker
                    .memory_usage
                    .fetch_add(new_size, Ordering::Relaxed);
            }

            Self::realloc_memory((*pointer).parent_tracker, old_size, new_size);
        }
    }
}

pub struct RuntimeTracker {
    pub memory_usage: AtomicUsize,
}

impl RuntimeTracker {
    pub fn create() -> Arc<RuntimeTracker> {
        Arc::new(RuntimeTracker {
            memory_usage: AtomicUsize::new(0),
        })
    }

    pub fn on_stop_thread(self: &Arc<Self>) -> impl Fn() {
        let _self = self.clone();
        move || { /* do nothing */ }
    }

    pub fn on_start_thread(self: &Arc<Self>) -> impl Fn() {
        // TODO: log::info("thread {}-{} started", thread_id, thread_name);

        let _self = self.clone();
        let parent_tracker = ThreadTracker::current() as usize;

        move || unsafe {
            let parent_tracker = parent_tracker as *const ThreadTracker;
            let mut thread_tracker = ThreadTracker::create(_self.clone());
            (*thread_tracker).parent_tracker = parent_tracker;
            ThreadTracker::set_current(thread_tracker);
        }
    }
}
