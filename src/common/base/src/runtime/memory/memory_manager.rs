// Copyright 2021 Datafuse Labs
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

use std::cell::LazyCell;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use crate::runtime::ThreadTracker;

pub static GLOBAL_QUERIES_MANAGER: QueriesMemoryManager = QueriesMemoryManager::global();

struct QueryMemoryInfo {
    priority: usize,
    notify: std::sync::Condvar,
    exceeded_memory_flag: Arc<AtomicUsize>,
}

pub struct QueriesMemoryManager {
    // wait_release_mutex: Mutex<usize>,
    exceeded_memory_list: LazyCell<Mutex<HashMap<String, Arc<AtomicUsize>>>>,
}

impl QueriesMemoryManager {
    pub const fn new() -> QueriesMemoryManager {
        QueriesMemoryManager {
            exceeded_memory_list: LazyCell::new(|| Mutex::new(HashMap::new())),
        }
    }

    pub fn request_exceeded_memory(&self, mode: Arc<AtomicUsize>) {
        let Some(query_id) = ThreadTracker::query_id() else {
            return;
        };

        let mut exceeded_memory_list = self
            .exceeded_memory_list
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        exceeded_memory_list.insert(query_id.clone(), mode);
    }

    pub fn wait_release_memory(&self, need_memory_usage: u64) {
        // let mut exceeded_memory_list = self.exceeded_memory_list.lock().unwrap_or_else(PoisonError::into_inner);
        // TODO:
    }

    pub fn release_memory(&self, release_memory_usage: u64) {}
}
