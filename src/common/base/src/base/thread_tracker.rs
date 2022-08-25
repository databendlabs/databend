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

use core::panicking::panic;
use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::sync::atomic::{AtomicI64, AtomicUsize};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::mem_allocator::ALLOC;

#[thread_local]
static mut TRACKER: *mut ThreadTracker = std::ptr::null_mut();

static UNTRACKED_MEMORY_LIMIT: i64 = 4 * 1024 * 1024;

pub struct ThreadTracker {
    global: Arc<GlobalTracker>,
    query: Option<Arc<QueryTracker>>,
    untracked_memory: i64,
}

impl ThreadTracker {
    pub fn init(global: Arc<GlobalTracker>, query: Option<Arc<QueryTracker>>) {
        unsafe {
            if !TRACKER.is_null() {
                panic!("The thread tracker cannot be set twice");
            }

            global.create_thread();
            if let Some(query) = &query {
                query.create_thread();
            }

            TRACKER = Box::into_raw(Box::new(ThreadTracker { global, query, untracked_memory: 0 }));
        }
    }

    pub fn destroy() {
        unsafe {
            let tracker = std::mem::replace(&mut TRACKER, std::ptr::null_mut());

            (*tracker).global.destroy_thread();
            if let Some(query) = &(*tracker).query {
                query.destroy_thread();
            }

            std::ptr::drop_in_place(tracker as usize as *mut ThreadTracker);
            ALLOC.dealloc(tracker as *mut u8, Layout::new::<ThreadTracker>())
        }
    }

    #[inline]
    pub fn current() -> *mut ThreadTracker {
        unsafe { TRACKER }
    }

    #[inline]
    pub fn alloc_memory(size: i64) {
        unsafe {
            if !TRACKER.is_null() {
                (*TRACKER).untracked_memory += size;

                if (*TRACKER).untracked_memory > UNTRACKED_MEMORY_LIMIT {
                    if let Some(query) = &(*TRACKER).query {
                        query.alloc_memory((*TRACKER).untracked_memory as usize);
                    }

                    (*TRACKER).global.alloc_memory((*TRACKER).untracked_memory as usize);
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
                    if let Some(query) = &(*TRACKER).query {
                        query.dealloc_memory((-(*TRACKER).untracked_memory) as usize);
                    }

                    (*TRACKER).global.dealloc_memory((-(*TRACKER).untracked_memory) as usize);
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

pub struct GlobalTracker {
    memory_usage: AtomicUsize,
    max_memory_usage: AtomicUsize,
    total_thread_size: AtomicUsize,
}

impl GlobalTracker {
    pub fn current() -> Arc<GlobalTracker> {
        unsafe {
            ThreadTracker::current().global.clone()
        }
    }

    pub fn create() -> Arc<GlobalTracker> {
        Arc::new(GlobalTracker {
            memory_usage: AtomicUsize::new(0),
            max_memory_usage: AtomicUsize::new(0),
            total_thread_size: AtomicUsize::new(0),
        })
    }

    pub fn alloc_memory(&self, size: usize) {
        let current_memory_usage = self.memory_usage.load(Ordering::Relaxed);
        if current_memory_usage + size >= self.max_memory_usage.load(Ordering::Relaxed) {
            panic!(format!(
                "The current memory usage({} * {} threads + {}) has exceeded the maximum memory usage({}) limit.",
                UNTRACKED_MEMORY_LIMIT,
                self.total_thread_size.load(Ordering::Relaxed),
                current_memory_usage,
                (self.max_memory_usage.load(Ordering::Relaxed) + UNTRACKED_MEMORY_LIMIT * self.total_thread_size.load(Ordering::Relaxed))
            ));
        }

        self.memory_usage.fetch_add(size, Ordering::Relaxed);
    }

    #[inline]
    pub fn dealloc_memory(&self, size: usize) {
        self.memory_usage.fetch_sub(size, Ordering::Relaxed);
    }

    pub fn create_thread(&self) {
        self.total_thread_size.fetch_add(1, Ordering::Relaxed);
        self.max_memory_usage.fetch_sub(UNTRACKED_MEMORY_LIMIT as usize, Ordering::Relaxed);
    }

    pub fn destroy_thread(&self) {
        self.total_thread_size.fetch_sub(1, Ordering::Relaxed);
        self.max_memory_usage.fetch_add(UNTRACKED_MEMORY_LIMIT as usize, Ordering::Relaxed);
    }
}

pub struct QueryTracker {
    memory_usage: AtomicUsize,
    max_memory_usage: AtomicUsize,
    total_thread_size: AtomicUsize,
}

impl QueryTracker {
    pub fn current() -> Option<Arc<QueryTracker>> {
        unsafe {
            (*ThreadTracker::current()).query.clone()
        }
    }

    #[inline]
    pub fn alloc_memory(&self, size: usize) {
        let current_memory_usage = self.memory_usage.load(Ordering::Relaxed);
        if current_memory_usage + size >= self.max_memory_usage.load(Ordering::Relaxed) {
            panic!(format!(
                "The current memory usage({} * {} threads + {}) has exceeded the query maximum memory usage({}) limit.",
                UNTRACKED_MEMORY_LIMIT,
                self.total_thread_size.load(Ordering::Relaxed),
                current_memory_usage,
                (self.max_memory_usage.load(Ordering::Relaxed) + UNTRACKED_MEMORY_LIMIT * self.total_thread_size.load(Ordering::Relaxed))
            ));
        }

        self.memory_usage.fetch_add(size, Ordering::Relaxed);
    }

    #[inline]
    pub fn dealloc_memory(&self, size: usize) {
        self.memory_usage.fetch_sub(size as usize, Ordering::Relaxed);
    }

    pub fn create_thread(&self) {
        self.total_thread_size.fetch_add(1, Ordering::Relaxed);
        self.max_memory_usage.fetch_sub(UNTRACKED_MEMORY_LIMIT as usize, Ordering::Relaxed);
    }

    pub fn destroy_thread(&self) {
        self.total_thread_size.fetch_sub(1, Ordering::Relaxed);
        self.max_memory_usage.fetch_add(UNTRACKED_MEMORY_LIMIT as usize, Ordering::Relaxed);
    }

    pub fn get_memory_usage(&self) -> usize {
        self.memory_usage.load(Ordering::Relaxed)
            + UNTRACKED_MEMORY_LIMIT * self.total_thread_size.load(Ordering::Relaxed)
    }
}
