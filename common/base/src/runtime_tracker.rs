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

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

thread_local! {
    static TRACKER: RefCell<Option<Arc<ThreadTracker>>> = RefCell::new(None)
}

pub struct ThreadTracker {
    rt_tracker: Arc<RuntimeTracker>,
    parent_tracker: Option<Arc<ThreadTracker>>,
}

impl ThreadTracker {
    pub(in crate::runtime_tracker) fn create(rt_tracker: Arc<RuntimeTracker>) -> ThreadTracker {
        ThreadTracker {
            rt_tracker,
            parent_tracker: None,
        }
    }

    #[inline]
    pub fn current() -> Arc<ThreadTracker> {
        Self::current_opt().unwrap()
    }

    #[inline]
    pub fn current_opt() -> Option<Arc<ThreadTracker>> {
        TRACKER.with(|tracker| tracker.borrow().clone())
    }

    #[inline]
    pub fn set_current(value: Arc<ThreadTracker>) {
        TRACKER.with(move |tracker| {
            tracker.borrow_mut().replace(value);
        });
    }

    #[inline]
    pub fn alloc_memory(&self, size: usize) {
        self.rt_tracker.memory_usage.fetch_add(size, Ordering::Relaxed);

        if let Some(parent_tracker) = &self.parent_tracker {
            parent_tracker.alloc_memory(size);
        }
    }

    #[inline]
    pub fn dealloc_memory(&self, size: usize) {
        self.rt_tracker.memory_usage.fetch_sub(size, Ordering::Relaxed);

        if let Some(parent_tracker) = &self.parent_tracker {
            parent_tracker.dealloc_memory(size);
        }
    }
}

pub struct RuntimeTracker {
    pub memory_usage: AtomicUsize,
}

impl RuntimeTracker {
    pub fn create() -> Arc<RuntimeTracker> {
        Arc::new(RuntimeTracker {
            memory_usage: AtomicUsize::new(0)
        })
    }

    pub fn on_stop_thread(self: &Arc<Self>) -> impl Fn() {
        let _self = self.clone();
        move || { /* do nothing */ }
    }

    pub fn on_start_thread(self: &Arc<Self>) -> impl Fn() {
        // TODO: log::info("thread {}-{} started", thread_id, thread_name);

        let _self = self.clone();
        let parent_tracker = ThreadTracker::current_opt();

        move || {
            let mut thread_tracker = ThreadTracker::create(_self.clone());
            thread_tracker.parent_tracker = parent_tracker.clone();
            ThreadTracker::set_current(Arc::new(thread_tracker));
        }
    }
}
