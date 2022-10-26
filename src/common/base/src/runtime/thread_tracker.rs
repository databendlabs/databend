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

use std::sync::Arc;

use crate::runtime::runtime_tracker::RuntimeTracker;

#[thread_local]
pub static mut TRACKER: *mut ThreadTracker = std::ptr::null_mut();

static UNTRACKED_MEMORY_LIMIT: usize = 4 * 1024 * 1024;

pub struct ThreadTracker {
    pub rt_tracker: Arc<RuntimeTracker>,
    untracked_memory: usize,
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
    pub fn alloc_memory(size: usize) {
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
    pub fn dealloc_memory(size: usize) {
        unsafe {
            if !TRACKER.is_null() {
                (*TRACKER).untracked_memory -= size;
            }
        }
    }

    #[inline]
    pub fn realloc_memory(old_size: usize, new_size: usize) {
        let addition = new_size - old_size;
        match addition > 0 {
            true => Self::alloc_memory(addition),
            false => Self::dealloc_memory(addition),
        }
    }
}
