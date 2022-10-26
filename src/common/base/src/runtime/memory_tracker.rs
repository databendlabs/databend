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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::runtime::ThreadTracker;

pub struct MemoryTracker {
    memory_usage: AtomicUsize,
}

impl MemoryTracker {
    pub fn create() -> Arc<MemoryTracker> {
        Arc::new(MemoryTracker {
            memory_usage: AtomicUsize::new(0),
        })
    }

    #[inline]
    pub fn alloc_memory(&self, size: usize) {
        self.memory_usage.fetch_add(size, Ordering::Relaxed);
    }

    #[inline]
    pub fn dealloc_memory(&self, size: usize) {
        self.memory_usage.fetch_sub(size, Ordering::Relaxed);
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
    pub fn get_memory_usage(&self) -> usize {
        self.memory_usage.load(Ordering::Relaxed)
    }
}
