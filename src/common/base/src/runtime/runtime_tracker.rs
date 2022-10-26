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

use crate::runtime::MemoryTracker;

pub struct RuntimeTracker {
    memory_tracker: Arc<MemoryTracker>,
}

impl RuntimeTracker {
    pub fn create() -> Arc<RuntimeTracker> {
        let memory_tracker = unsafe { Arc::from_raw(MemoryTracker::create()) };
        Arc::new(RuntimeTracker { memory_tracker })
    }

    pub fn get_memory_usage(&self) -> usize {
        self.memory_tracker.get_memory_usage()
    }
}
