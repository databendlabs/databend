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

use crate::runtime::thread_memory_usage;

static mut GLOBAL_ID: AtomicUsize = AtomicUsize::new(0);

#[thread_local]
pub static mut MEM_TRACKER: *mut MemoryTracker = std::ptr::null_mut();

pub struct MemoryTracker {
    track_id: usize,
}

impl MemoryTracker {
    pub fn create() -> *mut MemoryTracker {
        let track_id = unsafe { GLOBAL_ID.fetch_add(1, Ordering::SeqCst) };
        let tracker = Box::new(MemoryTracker { track_id });
        unsafe {
            MEM_TRACKER = Box::into_raw(tracker);
            MEM_TRACKER
        }
    }

    pub fn get_id() -> usize {
        unsafe { (*MEM_TRACKER).track_id }
    }

    #[inline]
    pub fn get_memory_usage(&self) -> usize {
        thread_memory_usage(unsafe { (*MEM_TRACKER).track_id })
    }
}
