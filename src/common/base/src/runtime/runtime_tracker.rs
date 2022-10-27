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

use crate::runtime::MemoryTracker;

static mut GLOBAL_ID: AtomicUsize = AtomicUsize::new(0);

pub struct RuntimeTracker {
    id: usize,
}

impl RuntimeTracker {
    pub fn create() -> Arc<RuntimeTracker> {
        let id = unsafe { GLOBAL_ID.fetch_add(1, Ordering::SeqCst) };
        Arc::new(RuntimeTracker { id })
    }

    pub fn get_memory_usage(&self) -> usize {
        MemoryTracker::get_memory_usage(self.id)
    }

    pub fn on_stop_thread(self: &Arc<Self>) -> impl Fn() {
        move || {}
    }

    pub fn on_start_thread(self: &Arc<Self>) -> impl Fn() {
        let id = self.id;
        move || MemoryTracker::create(id)
    }
}
