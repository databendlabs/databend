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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::OnceLock;
use std::sync::PoisonError;

use crate::runtime::OutOfLimit;

pub static GLOBAL_QUERIES_MANAGER: QueriesMemoryManager = QueriesMemoryManager::create();

#[allow(dead_code)]
struct QueryMemoryInfo {
    priority: usize,
    condvar: Condvar,
    exceeded_memory_flag: Arc<AtomicUsize>,
}

#[allow(dead_code)]
struct ExceededMemoryState {
    canceled_set: HashSet<usize>,
    exceeded_list: HashMap<usize, QueryMemoryInfo>,
}

pub struct QueriesMemoryManager {
    state: OnceLock<Mutex<ExceededMemoryState>>,
}

impl QueriesMemoryManager {
    pub const fn create() -> QueriesMemoryManager {
        QueriesMemoryManager {
            state: OnceLock::new(),
        }
    }

    pub fn request_exceeded_memory(&self, id: usize, priority: usize, mode: Arc<AtomicUsize>) {
        let mut exceeded_state = self.get_state();

        exceeded_state.exceeded_list.insert(id, QueryMemoryInfo {
            priority,
            condvar: Condvar::new(),
            exceeded_memory_flag: mode,
        });
    }

    pub fn wait_release_memory(
        &self,
        id: usize,
        out_of_limit: OutOfLimit,
    ) -> Result<(), OutOfLimit> {
        let exceeded_memory_state = self.get_state();

        if exceeded_memory_state.exceeded_list.is_empty() {
            return Err(out_of_limit);
        }

        if exceeded_memory_state.exceeded_list.len() == 1
            && exceeded_memory_state.exceeded_list.contains_key(&id)
        {
            return Err(out_of_limit);
        }

        // if let Some(low_priority_query) = exceeded_memory_state.values().min_by(|x, y| x.priority.cmp(&y.priority)) {
        //     // kill it.
        //     // low_priority_query.exceeded_memory_flag.compare_exchange();
        //     // low_priority_query.condvar.wait_timeout(exceeded_memory_list, Duration::from_millis(100));
        // }

        Err(out_of_limit)
    }

    //
    pub fn release_memory(&self, id: usize) {
        let mut exceeded_queries_state = self.get_state();
        if let Some(query_memory_info) = exceeded_queries_state.exceeded_list.remove(&id) {
            query_memory_info.condvar.notify_all();
        }
    }

    fn get_state(&self) -> MutexGuard<'_, ExceededMemoryState> {
        self.state
            .get_or_init(|| {
                Mutex::new(ExceededMemoryState {
                    canceled_set: Default::default(),
                    exceeded_list: Default::default(),
                })
            })
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}
