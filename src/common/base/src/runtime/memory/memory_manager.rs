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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::time::Duration;

use crate::runtime::memory::mem_stat::MEMORY_LIMIT_EXCEEDED_NO_ERROR;
use crate::runtime::memory::mem_stat::MEMORY_LIMIT_EXCEEDED_REPORTING_ERROR;
use crate::runtime::MemStat;
use crate::runtime::OutOfLimit;

pub static GLOBAL_QUERIES_MANAGER: QueriesMemoryManager = QueriesMemoryManager::create();

#[allow(dead_code)]
struct QueryMemoryInfo {
    priority: usize,
    condvar: Arc<Condvar>,
    exceeded_memory_flag: Arc<AtomicUsize>,
}

#[allow(dead_code)]
struct ExceededMemoryState {
    kill_queries: HashMap<usize, QueryMemoryInfo>,
    running_queries: HashMap<usize, QueryMemoryInfo>,
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

        exceeded_state.running_queries.insert(id, QueryMemoryInfo {
            priority,
            exceeded_memory_flag: mode,
            condvar: Arc::new(Condvar::new()),
        });
    }

    pub fn wait_memory(&self, mem_stat: &MemStat, cause: OutOfLimit) -> Result<(), OutOfLimit> {
        let mut state = self.get_state();

        loop {
            if state.running_queries.is_empty() {
                return Err(cause);
            }

            let id = mem_stat.id;
            if state.running_queries.len() == 1 && state.running_queries.contains_key(&id) {
                return Err(cause);
            }

            let Some(min_priority_id) = state
                .running_queries
                .iter()
                .min_by(|(_, x), (_, y)| x.priority.cmp(&y.priority))
                .map(|(min_id, _)| *min_id)
            else {
                return Err(cause);
            };

            if let Some(query) = state.running_queries.remove(&min_priority_id) {
                let _ = query.exceeded_memory_flag.compare_exchange(
                    MEMORY_LIMIT_EXCEEDED_NO_ERROR,
                    MEMORY_LIMIT_EXCEEDED_REPORTING_ERROR,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );

                let condvar = query.condvar.clone();
                state.kill_queries.insert(min_priority_id, query);

                // max wait 10 seconds per query
                for _index in 0..20 {
                    let wait_time = Duration::from_millis(500);
                    let wait_result = condvar.wait_timeout(state, wait_time);
                    let wait_result = wait_result.unwrap_or_else(PoisonError::into_inner);

                    if mem_stat.recheck_limit().is_ok() {
                        eprintln!("return ok");
                        return Ok(());
                    }

                    state = wait_result.0;
                }
            }
        }
    }

    pub fn release_memory(&self, id: usize) {
        let mut exceeded_queries_state = self.get_state();

        if let Some(query_memory_info) = exceeded_queries_state.kill_queries.remove(&id) {
            query_memory_info.condvar.notify_all();
        }

        if let Some(query_memory_info) = exceeded_queries_state.running_queries.remove(&id) {
            query_memory_info.condvar.notify_all();
        }
    }

    fn get_state(&self) -> MutexGuard<'_, ExceededMemoryState> {
        self.state
            .get_or_init(|| {
                Mutex::new(ExceededMemoryState {
                    kill_queries: Default::default(),
                    running_queries: Default::default(),
                })
            })
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    use super::*;
    use crate::base::GlobalUniqName;
    use crate::runtime::Thread;

    #[test]
    fn test_single_query_returns_error() {
        let out_of_limit = OutOfLimit::new(0, 0);
        let mem_stat = MemStat::create(GlobalUniqName::unique());

        let manager = QueriesMemoryManager::create();
        let mode = Arc::new(AtomicUsize::new(MEMORY_LIMIT_EXCEEDED_NO_ERROR));
        manager.request_exceeded_memory(mem_stat.id, 1, mode.clone());

        let result = manager.wait_memory(&mem_stat, out_of_limit);

        assert!(result.is_err());
    }

    #[test]
    fn test_kill_lowest_priority_query() {
        let mem_stat = MemStat::create(GlobalUniqName::unique());
        let manager = Arc::new(QueriesMemoryManager::create());
        let mode1 = Arc::new(AtomicUsize::new(MEMORY_LIMIT_EXCEEDED_NO_ERROR));
        let mode2 = Arc::new(AtomicUsize::new(MEMORY_LIMIT_EXCEEDED_NO_ERROR));

        manager.request_exceeded_memory(0, 1, mode1.clone());
        manager.request_exceeded_memory(mem_stat.id, 2, mode2.clone());

        Thread::spawn({
            let mode1 = mode1.clone();
            let manager = manager.clone();

            move || {
                std::thread::sleep(Duration::from_millis(500));
                mode1.store(MEMORY_LIMIT_EXCEEDED_NO_ERROR, Ordering::SeqCst);
                manager.release_memory(0);
            }
        });

        let result = manager.wait_memory(&mem_stat, OutOfLimit::new(0, 0));

        assert!(result.is_ok());
    }

    #[test]
    fn test_release_removes_from_kill_and_running() {
        let manager = QueriesMemoryManager::create();
        let mode = Arc::new(AtomicUsize::new(MEMORY_LIMIT_EXCEEDED_NO_ERROR));

        manager.request_exceeded_memory(1, 1, mode.clone());
        manager.release_memory(1);

        manager.release_memory(1);
    }

    #[test]
    fn test_wait_memory_timeout() {
        let manager = QueriesMemoryManager::create();
        let mode1 = Arc::new(AtomicUsize::new(MEMORY_LIMIT_EXCEEDED_NO_ERROR));
        let mode2 = Arc::new(AtomicUsize::new(MEMORY_LIMIT_EXCEEDED_NO_ERROR));

        let out_of_limit = OutOfLimit::new(0, 0);
        let mem_stat = MemStat::create(GlobalUniqName::unique());
        mem_stat.set_limit(1);
        mem_stat
            .used
            .fetch_add(1 * 1024 * 1024 * 1024, Ordering::SeqCst);

        manager.request_exceeded_memory(0, 1, mode1);
        manager.request_exceeded_memory(mem_stat.id, 2, mode2.clone());

        let result = manager.wait_memory(&mem_stat, out_of_limit);

        assert!(result.is_err());
    }

    #[test]
    fn test_duplicate_id_override() {
        let manager = QueriesMemoryManager::create();
        let mode1 = Arc::new(AtomicUsize::new(MEMORY_LIMIT_EXCEEDED_NO_ERROR));
        let mode2 = Arc::new(AtomicUsize::new(MEMORY_LIMIT_EXCEEDED_NO_ERROR));

        manager.request_exceeded_memory(1, 1, mode1.clone());
        manager.request_exceeded_memory(1, 2, mode2.clone());

        let state = manager.get_state();
        let query = state.running_queries.get(&1).unwrap();
        assert_eq!(query.priority, 2);
        assert!(Arc::ptr_eq(&query.exceeded_memory_flag, &mode2));
    }
}
