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

use std::cell::RefCell;

use crate::runtime::thread_memory_usage;

thread_local! {
    static LOCAL: RefCell<Option<MemoryTracker>> = const { RefCell::new(None) }
}

#[derive(Debug, Clone)]
pub struct MemoryTracker {
    id: usize,
}

impl MemoryTracker {
    pub fn create(id: usize) {
        let tracker = MemoryTracker { id };
        LOCAL.with(|t| *t.borrow_mut() = Some(tracker));
    }

    pub fn get_id() -> usize {
        match LOCAL.with(|x| x.borrow().clone()) {
            None => 0,
            Some(v) => v.id,
        }
    }

    pub fn set_to_null() {
        LOCAL.with(|t| *t.borrow_mut() = None);
    }

    #[inline]
    pub fn get_memory_usage(id: usize) -> usize {
        thread_memory_usage(id)
    }
}
