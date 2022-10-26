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

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::sync::Arc;

use crate::mem_allocator::ALLOC;
use crate::runtime::MemoryTracker;
use crate::runtime::ThreadTracker;
use crate::runtime::TRACKER;

pub struct RuntimeTracker {
    pub memory_tracker: Arc<MemoryTracker>,
}

impl RuntimeTracker {
    pub fn create() -> Arc<RuntimeTracker> {
        Arc::new(RuntimeTracker {
            memory_tracker: MemoryTracker::create(),
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
            ALLOC.dealloc(tracker as *mut u8, Layout::new::<ThreadTracker>())
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
