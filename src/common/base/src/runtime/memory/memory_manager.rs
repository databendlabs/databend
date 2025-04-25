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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::time::Duration;
use std::time::Instant;

use crate::runtime::memory::mem_stat::ParentMemStat;
use crate::runtime::MemStat;
use crate::runtime::OutOfLimit;

pub static GLOBAL_QUERIES_MANAGER: QueriesMemoryManager = QueriesMemoryManager::create();

type ResourceTag = String;
type Priority = usize;

#[derive(Eq, PartialEq, Debug, Clone)]
enum QueryGcState {
    Running,
    Killed,
}

#[derive(Debug, Clone)]
struct ResourceMemoryInfo {
    condvar: Arc<Condvar>,
    mutex: Arc<Mutex<QueryGcState>>,
    killing_instant: Option<Instant>,
}

unsafe impl Send for ResourceMemoryInfo {}
unsafe impl Sync for ResourceMemoryInfo {}

type GcHandler = Arc<dyn Fn(&String, bool) -> bool + Send + Sync + 'static>;

#[allow(dead_code)]
struct ExceededMemoryState {
    resources: HashMap<ResourceTag, ResourceMemoryInfo>,
    groups: HashMap<usize, HashSet<(Priority, ResourceTag)>>,
    memory_gc_handler: Option<GcHandler>,
}

impl ExceededMemoryState {
    pub fn low_priority_resource(&self, id: usize) -> Option<String> {
        let resources = self.groups.get(&id)?;
        resources.iter().min().map(|(_, tag)| tag.clone())
    }

    pub fn terminate(&mut self, id: &String, resource: &ResourceMemoryInfo) {
        {
            let mutex = resource.mutex.lock();
            let mut mutex = mutex.unwrap_or_else(PoisonError::into_inner);
            *mutex = QueryGcState::Killed;
        }

        for value in self.groups.values_mut() {
            value.retain(|(_, v)| v != id);
        }
    }
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

    pub fn set_gc_handle<F: Fn(&ResourceTag, bool) -> bool + Send + Sync + 'static>(&self, f: F) {
        let mut state = self.get_state();
        state.memory_gc_handler = Some(Arc::new(f));
    }

    pub fn request_exceeded_memory(&self, mem_stat: &MemStat, tag: Option<&ResourceTag>) {
        let Some(resource_tag) = tag else {
            return;
        };

        let priority = 0;
        let mut exceeded_state = self.get_state();

        let mut parent = &mem_stat.parent_memory_stat;

        loop {
            match parent {
                ParentMemStat::Root => {
                    break;
                }
                ParentMemStat::StaticRef(_) => {
                    match exceeded_state.groups.entry(0) {
                        Entry::Vacant(v) => {
                            v.insert(HashSet::new())
                                .insert((priority, resource_tag.clone()));
                        }
                        Entry::Occupied(mut v) => {
                            v.get_mut().insert((priority, resource_tag.clone()));
                        }
                    };

                    break;
                }
                ParentMemStat::Normal(parent_mem_stat) => {
                    match exceeded_state.groups.entry(parent_mem_stat.id) {
                        Entry::Vacant(v) => {
                            v.insert(HashSet::new())
                                .insert((priority, resource_tag.clone()));
                        }
                        Entry::Occupied(mut v) => {
                            v.get_mut().insert((priority, resource_tag.clone()));
                        }
                    };

                    parent = &parent_mem_stat.parent_memory_stat;
                }
            }
        }

        let Entry::Vacant(v) = exceeded_state.resources.entry(resource_tag.clone()) else {
            unreachable!(
                "Memory-exceeding trackers require strict one-to-one binding with queries."
            );
        };

        v.insert(ResourceMemoryInfo {
            killing_instant: None,
            condvar: Arc::new(Default::default()),
            mutex: Arc::new(Mutex::new(QueryGcState::Running)),
        });
    }

    fn recheck_limit(mem_stat: &MemStat) -> bool {
        match &mem_stat.parent_memory_stat {
            ParentMemStat::Root => true,
            ParentMemStat::StaticRef(global) => global.recheck_limit().is_ok(),
            ParentMemStat::Normal(mem_stat) => mem_stat.recheck_limit().is_ok(),
        }
    }

    pub fn wait_memory(
        &self,
        mem_stat: &MemStat,
        tag: Option<&ResourceTag>,
        cause: OutOfLimit,
    ) -> Result<(), OutOfLimit> {
        let Some(waiting_id) = tag else {
            return Ok(());
        };

        let parent_id = match &mem_stat.parent_memory_stat {
            ParentMemStat::Root => 0,
            ParentMemStat::StaticRef(_) => 0,
            ParentMemStat::Normal(mem_stat) => mem_stat.id,
        };

        for _index in 0..5 {
            let mut state = self.get_state();

            if let Some(waiting_resource) = state.resources.get(waiting_id) {
                // kill by other
                if waiting_resource.killing_instant.is_some() {
                    return Ok(());
                }
            }

            let Some(terminate_id) = state.low_priority_resource(parent_id) else {
                return Err(cause);
            };

            if &terminate_id == waiting_id {
                return Ok(());
            }

            let memory_gc_handler = state.memory_gc_handler.clone();

            let Entry::Occupied(mut entry) = state.resources.entry(terminate_id) else {
                continue;
            };

            if let Some(instant) = entry.get().killing_instant.as_ref() {
                if instant.elapsed() > Duration::from_secs(10) {
                    let (terminate_id, info) = entry.remove_entry();
                    state.terminate(&terminate_id, &info);
                    continue;
                }
            }

            if let Some(handler) = memory_gc_handler {
                let mutex = entry.get().mutex.clone();
                let condvar = entry.get().condvar.clone();
                let terminate_id = entry.key().clone();

                if entry.get().killing_instant.is_none() {
                    let handler = handler.clone();
                    entry.get_mut().killing_instant = Some(Instant::now());

                    drop(state);
                    if !handler(&terminate_id, false) {
                        if Self::recheck_limit(mem_stat) {
                            return Ok(());
                        }

                        if !handler(&terminate_id, true) {
                            let mut state = self.get_state();

                            if let Some(resource) = state.resources.remove(&terminate_id) {
                                state.terminate(&terminate_id, &resource);
                            }

                            continue;
                        }
                    }
                }

                let mutex = mutex.lock();
                let mut mutex = mutex.unwrap_or_else(PoisonError::into_inner);
                eprintln!("exceeded {} wait killing {}", waiting_id, terminate_id);

                // max wait 10 seconds per query
                for _index in 0..100 {
                    if *mutex == QueryGcState::Killed {
                        if Self::recheck_limit(mem_stat) {
                            return Ok(());
                        }

                        break;
                    }

                    let wait_time = Duration::from_millis(100);
                    let wait_result = condvar.wait_timeout(mutex, wait_time);
                    let wait_result = wait_result.unwrap_or_else(PoisonError::into_inner);

                    if wait_result.1.timed_out() {
                        if Self::recheck_limit(mem_stat) {
                            return Ok(());
                        }

                        mutex = wait_result.0;
                        continue;
                    }

                    break;
                }
            }
        }

        Err(cause)
    }

    pub fn release_memory(&self, tag: Option<&ResourceTag>) {
        eprintln!("exceeded release {:?}", tag);
        let Some(terminate_id) = tag else {
            return;
        };

        let mut state = self.get_state();

        if let Some(info) = state.resources.remove(terminate_id) {
            state.terminate(terminate_id, &info);
            drop(state);
            let mutex = info.mutex.lock();
            let mut mutex = mutex.unwrap_or_else(PoisonError::into_inner);
            *mutex = QueryGcState::Killed;
            info.condvar.notify_all();
        }
    }

    fn get_state(&self) -> MutexGuard<'_, ExceededMemoryState> {
        self.state
            .get_or_init(|| {
                Mutex::new(ExceededMemoryState {
                    groups: Default::default(),
                    resources: Default::default(),
                    memory_gc_handler: None,
                })
            })
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    // use std::collections::HashMap;
    // use std::collections::HashSet;
    // use std::sync::atomic::Ordering;
    // use std::sync::Arc;
    // use std::time::Duration;
    // use std::time::Instant;
    //
    // use crate::base::GlobalUniqName;
    // use crate::runtime::memory::mem_stat::MEMORY_LIMIT_EXCEEDED_NO_ERROR;
    // use crate::runtime::memory::mem_stat::MEMORY_LIMIT_EXCEEDED_REPORTING_ERROR;
    // use crate::runtime::memory::memory_manager::QueriesMemoryManager;
    // use crate::runtime::MemStat;
    // use crate::runtime::OutOfLimit;
    // use crate::runtime::Thread;
    // use crate::runtime::ThreadTracker;
    //
    // #[test]
    // fn test_exclusion_list() {
    //     let mut queries = HashMap::new();
    //     queries.insert(1, ("a".into(), 1));
    //     queries.insert(2, ("b".into(), 1));
    //     queries.insert(3, ("c".into(), 1));
    //
    //     let exclude: HashSet<usize> = [1, 2].iter().cloned().collect();
    //     let selected = QueriesMemoryManager::select_children(&queries, &exclude);
    //     assert_eq!(selected, Some(3));
    // }
    //
    // #[test]
    // fn test_tie_breaker_with_query_id() {
    //     let mut queries = HashMap::new();
    //     queries.insert(1, ("query_2".into(), 1));
    //     queries.insert(2, ("query_1".into(), 1));
    //
    //     let selected = QueriesMemoryManager::select_children(&queries, &HashSet::new());
    //     assert_eq!(selected, Some(2));
    // }
    //
    // #[test]
    // fn test_global_parent_registration() {
    //     let manager = QueriesMemoryManager::create();
    //     let parent = MemStat::create(GlobalUniqName::unique());
    //     let child = MemStat::create_child(GlobalUniqName::unique(), Some(parent.clone()), 0);
    //
    //     let mut payload = ThreadTracker::new_tracking_payload();
    //     payload.query_id = Some(GlobalUniqName::unique());
    //     let _guard = ThreadTracker::tracking(payload);
    //
    //     manager.request_exceeded_memory(&child);
    //
    //     let state = manager.get_state();
    //     assert!(state.mem_stat[&0].contains_key(&child.id));
    //     assert!(state.mem_stat[&parent.id].contains_key(&child.id));
    //
    //     assert!(!state.mem_stat[&0].contains_key(&parent.id));
    //     assert!(!state.mem_stat[&parent.id].contains_key(&0));
    // }
    //
    // #[test]
    // #[should_panic(expected = "unreachable")]
    // fn test_duplicate_registration_panic() {
    //     let manager = QueriesMemoryManager::create();
    //     let mem_stat = MemStat::create(GlobalUniqName::unique());
    //
    //     let mut payload = ThreadTracker::new_tracking_payload();
    //     payload.query_id = Some(GlobalUniqName::unique());
    //     let _guard = ThreadTracker::tracking(payload);
    //
    //     manager.request_exceeded_memory(&mem_stat);
    //     manager.request_exceeded_memory(&mem_stat);
    // }
    //
    // #[test]
    // fn test_self_termination_when_no_children() {
    //     let manager = QueriesMemoryManager::create();
    //     let mem_stat = MemStat::create(GlobalUniqName::unique());
    //
    //     {
    //         let mut payload = ThreadTracker::new_tracking_payload();
    //         payload.query_id = Some(GlobalUniqName::unique());
    //         let _guard = ThreadTracker::tracking(payload);
    //
    //         mem_stat
    //             .exceeded_limit_state
    //             .store(MEMORY_LIMIT_EXCEEDED_NO_ERROR, Ordering::SeqCst);
    //         manager.request_exceeded_memory(&mem_stat);
    //     }
    //
    //     let result = manager.wait_memory(&mem_stat, OutOfLimit::new(0, 0));
    //     assert!(result.is_err());
    //     assert_eq!(
    //         mem_stat.exceeded_limit_state.load(Ordering::SeqCst),
    //         MEMORY_LIMIT_EXCEEDED_REPORTING_ERROR
    //     );
    // }
    //
    // #[test]
    // fn test_priority_based_selection() {
    //     let manager = QueriesMemoryManager::create();
    //     let parent = MemStat::create(GlobalUniqName::unique());
    //
    //     let children = vec![
    //         (1, 3, "low_pri".into()),
    //         (2, 2, "mid_pri".into()),
    //         (3, 1, "high_pri".into()),
    //     ];
    //
    //     for (_id, pri, qid) in children {
    //         let mut payload = ThreadTracker::new_tracking_payload();
    //         payload.query_id = Some(qid);
    //         let _guard = ThreadTracker::tracking(payload);
    //
    //         let mem = MemStat::create_child(GlobalUniqName::unique(), Some(parent.clone()), pri);
    //
    //         mem.exceeded_limit_state
    //             .store(MEMORY_LIMIT_EXCEEDED_NO_ERROR, Ordering::SeqCst);
    //         manager.request_exceeded_memory(&mem);
    //     }
    //
    //     let target = MemStat::create_child(GlobalUniqName::unique(), Some(parent.clone()), 0);
    //     let result = manager.wait_memory(&target, OutOfLimit::new(0, 0));
    //
    //     // recheck limit passed.
    //     assert!(result.is_ok());
    //     let state = manager.get_state();
    //
    //     let killed_children = state
    //         .running_queries
    //         .iter()
    //         .filter(|(_, x)| {
    //             x.exceeded_memory_state().load(Ordering::SeqCst)
    //                 == MEMORY_LIMIT_EXCEEDED_REPORTING_ERROR
    //         })
    //         .map(|(x, y)| (*x, y.clone()))
    //         .collect::<Vec<_>>();
    //
    //     assert_eq!(killed_children.len(), 1);
    //     assert_eq!(state.mem_stat[&0][&killed_children[0].0].0, "high_pri");
    //     assert_eq!(
    //         state.mem_stat[&parent.id][&killed_children[0].0].0,
    //         "high_pri"
    //     );
    // }
    //
    // #[test]
    // fn test_condvar_notification_with_no_child() {
    //     let manager = Arc::new(QueriesMemoryManager::create());
    //     let parent = MemStat::create(GlobalUniqName::unique());
    //     let child = MemStat::create_child(GlobalUniqName::unique(), Some(parent.clone()), 0);
    //
    //     {
    //         let mut payload = ThreadTracker::new_tracking_payload();
    //         payload.query_id = Some(GlobalUniqName::unique());
    //         let _guard = ThreadTracker::tracking(payload);
    //
    //         child
    //             .exceeded_limit_state
    //             .store(MEMORY_LIMIT_EXCEEDED_NO_ERROR, Ordering::SeqCst);
    //         manager.request_exceeded_memory(&child);
    //     }
    //
    //     let manager_clone = manager.clone();
    //     let child_clone = child.clone();
    //     let handle = Thread::spawn(move || {
    //         let instant = Instant::now();
    //         assert!(manager_clone
    //             .wait_memory(&child_clone, OutOfLimit::new(0, 0))
    //             .is_err());
    //         Ok::<_, OutOfLimit>(instant.elapsed())
    //     });
    //
    //     std::thread::sleep(Duration::from_millis(1000));
    //
    //     manager.release_memory(&child);
    //     assert!(handle.join().unwrap().unwrap() < Duration::from_millis(1000));
    // }
    //
    // #[test]
    // fn test_condvar_notification() {
    //     let manager = Arc::new(QueriesMemoryManager::create());
    //     let parent = MemStat::create(GlobalUniqName::unique());
    //     let child = MemStat::create_child(GlobalUniqName::unique(), Some(parent.clone()), 0);
    //
    //     {
    //         let mut payload = ThreadTracker::new_tracking_payload();
    //         payload.query_id = Some(GlobalUniqName::unique());
    //         let _guard = ThreadTracker::tracking(payload);
    //         child
    //             .exceeded_limit_state
    //             .store(MEMORY_LIMIT_EXCEEDED_NO_ERROR, Ordering::SeqCst);
    //         manager.request_exceeded_memory(&child);
    //     }
    //
    //     // limit 500MB
    //     parent.set_limit(500 * 1024 * 1024, true);
    //     // used 1GB
    //     parent.used.fetch_add(1024 * 1024 * 1024, Ordering::SeqCst);
    //
    //     let manager_clone = manager.clone();
    //     let handle = Thread::spawn(move || {
    //         let instant = Instant::now();
    //         let target = MemStat::create_child(GlobalUniqName::unique(), Some(parent.clone()), 0);
    //         assert!(manager_clone
    //             .wait_memory(&target, OutOfLimit::new(0, 0))
    //             .is_err());
    //         Ok::<_, OutOfLimit>(instant.elapsed())
    //     });
    //
    //     std::thread::sleep(Duration::from_millis(1000));
    //
    //     manager.release_memory(&child);
    //
    //     assert!(handle.join().unwrap().unwrap() > Duration::from_millis(1000));
    // }
    //
    // #[test]
    // fn test_condvar_notification_with_recheck() {
    //     let manager = Arc::new(QueriesMemoryManager::create());
    //     let parent = MemStat::create(GlobalUniqName::unique());
    //     let child = MemStat::create_child(GlobalUniqName::unique(), Some(parent.clone()), 0);
    //
    //     {
    //         let mut payload = ThreadTracker::new_tracking_payload();
    //         payload.query_id = Some(GlobalUniqName::unique());
    //         let _guard = ThreadTracker::tracking(payload);
    //
    //         child
    //             .exceeded_limit_state
    //             .store(MEMORY_LIMIT_EXCEEDED_NO_ERROR, Ordering::SeqCst);
    //         manager.request_exceeded_memory(&child);
    //     }
    //
    //     // limit 500MB
    //     parent.set_limit(500 * 1024 * 1024, true);
    //     // used 1GB
    //     parent.used.fetch_add(1024 * 1024 * 1024, Ordering::SeqCst);
    //
    //     let handle = Thread::spawn({
    //         let parent = parent.clone();
    //         let manager = manager.clone();
    //         move || {
    //             let instant = Instant::now();
    //             let target =
    //                 MemStat::create_child(GlobalUniqName::unique(), Some(parent.clone()), 0);
    //             assert!(manager.wait_memory(&target, OutOfLimit::new(0, 0)).is_ok());
    //             Ok::<_, OutOfLimit>(instant.elapsed())
    //         }
    //     });
    //
    //     std::thread::sleep(Duration::from_millis(1000));
    //
    //     // reduce memory used.
    //     parent.used.fetch_sub(800 * 1024 * 1024, Ordering::SeqCst);
    //
    //     std::thread::sleep(Duration::from_millis(1000));
    //
    //     // release memory
    //     manager.release_memory(&child);
    //
    //     let duration = handle.join().unwrap().unwrap();
    //     assert!(duration > Duration::from_millis(1000));
    //     assert!(duration < Duration::from_millis(2000));
    // }
    //
    // #[test]
    // fn test_cleanup_after_release() {
    //     let manager = QueriesMemoryManager::create();
    //     let parent = MemStat::create(GlobalUniqName::unique());
    //     let child = MemStat::create_child(GlobalUniqName::unique(), Some(parent.clone()), 0);
    //
    //     let mut payload = ThreadTracker::new_tracking_payload();
    //     payload.query_id = Some(GlobalUniqName::unique());
    //     let _guard = ThreadTracker::tracking(payload);
    //
    //     manager.request_exceeded_memory(&child);
    //
    //     manager.release_memory(&child);
    //     let state = manager.get_state();
    //
    //     assert!(state.running_queries.is_empty());
    //     assert!(!state.mem_stat[&0].contains_key(&child.id));
    //     assert!(!state.mem_stat[&0].contains_key(&parent.id));
    //     assert!(!state.mem_stat.contains_key(&parent.id))
    // }
}
