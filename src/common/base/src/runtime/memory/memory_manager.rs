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
use crate::runtime::memory::mem_stat::MEMORY_LIMIT_EXCEEDED_REPORTED_ERROR;
use crate::runtime::memory::mem_stat::MEMORY_LIMIT_EXCEEDED_REPORTING_ERROR;
use crate::runtime::MemStat;
use crate::runtime::OutOfLimit;
use crate::runtime::ThreadTracker;
use crate::runtime::GLOBAL_MEM_STAT;

pub static GLOBAL_QUERIES_MANAGER: QueriesMemoryManager = QueriesMemoryManager::create();

type QueryId = String;
type Priority = usize;

#[derive(Debug, Clone)]
struct QueryMemoryInfo {
    condvar: Arc<Condvar>,
    exceeded_memory_flag: Arc<AtomicUsize>,
}

#[allow(dead_code)]
struct ExceededMemoryState {
    mem_stat: HashMap<usize, HashMap<usize, (QueryId, Priority)>>,
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

    pub fn request_exceeded_memory(&self, mem_stat: &MemStat, mode: Arc<AtomicUsize>) {
        let Some(query_id) = ThreadTracker::query_id().cloned() else {
            // global pass.
            return;
        };

        let priority = 0;
        let mut exceeded_state = self.get_state();

        let mut parent = mem_stat.parent_memory_stat.as_ref();

        while let Some(parent_mem_stat) = parent {
            match exceeded_state.mem_stat.entry(parent_mem_stat.id) {
                Entry::Vacant(v) => {
                    v.insert(HashMap::new())
                        .insert(mem_stat.id, (query_id.clone(), priority));
                }
                Entry::Occupied(mut v) => {
                    v.get_mut()
                        .insert(mem_stat.id, (query_id.clone(), priority));
                }
            };

            parent = parent_mem_stat.parent_memory_stat.as_ref();
        }

        if mem_stat.id != 0 {
            match exceeded_state.mem_stat.entry(0) {
                Entry::Vacant(v) => {
                    v.insert(HashMap::new())
                        .insert(mem_stat.id, (query_id.clone(), priority));
                }
                Entry::Occupied(mut v) => {
                    v.get_mut()
                        .insert(mem_stat.id, (query_id.clone(), priority));
                }
            };
        }

        let Entry::Vacant(v) = exceeded_state.running_queries.entry(mem_stat.id) else {
            unreachable!(
                "Memory-exceeding trackers require strict one-to-one binding with queries."
            );
        };

        v.insert(QueryMemoryInfo {
            exceeded_memory_flag: mode,
            condvar: Arc::new(Default::default()),
        });
    }

    pub(crate) fn select_children(
        queries: &HashMap<usize, (QueryId, Priority)>,
        exclude: &HashSet<usize>,
    ) -> Option<usize> {
        queries
            .iter()
            .filter(|(x, _)| !exclude.contains(x))
            .min_by(|(_, left), (_, right)| left.1.cmp(&right.1).then(left.0.cmp(&right.0)))
            .map(|(id, _)| *id)
    }

    fn recheck_limit(mem_stat: &MemStat) -> bool {
        if let Some(parent) = mem_stat.parent_memory_stat.as_ref() {
            return parent.recheck_limit().is_ok();
        }

        GLOBAL_MEM_STAT.recheck_limit().is_ok()
    }

    pub fn wait_memory(&self, mem_stat: &MemStat, cause: OutOfLimit) -> Result<(), OutOfLimit> {
        let parent_id = match &mem_stat.parent_memory_stat {
            None => 0,
            Some(mem_stat) => mem_stat.id,
        };

        let mut exclude = HashSet::new();
        let mut state = self.get_state();

        loop {
            if let Some(query) = state.running_queries.get(&mem_stat.id) {
                // kill by other
                let flag = query.exceeded_memory_flag.load(Ordering::SeqCst);
                if flag == MEMORY_LIMIT_EXCEEDED_REPORTING_ERROR
                    || flag == MEMORY_LIMIT_EXCEEDED_REPORTED_ERROR
                {
                    return Err(cause);
                }
            }

            // No children exceeded memory. random kill
            let Some(queries) = state.mem_stat.get_mut(&parent_id) else {
                return Err(cause);
            };

            if queries.is_empty() {
                return Err(cause);
            }

            // only has self not kill. kill self
            let Some(mem_stat_id) = Self::select_children(queries, &exclude) else {
                return Err(cause);
            };

            let Some(query) = state.running_queries.get(&mem_stat_id) else {
                return Err(cause);
            };

            let _ = query.exceeded_memory_flag.compare_exchange(
                MEMORY_LIMIT_EXCEEDED_NO_ERROR,
                MEMORY_LIMIT_EXCEEDED_REPORTING_ERROR,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );

            // kill self
            if mem_stat_id == mem_stat.id {
                return Err(cause);
            }

            let mut all_timeout = true;
            let condvar = query.condvar.clone();

            // max wait 10 seconds per query
            for _index in 0..20 {
                let wait_time = Duration::from_millis(500);
                let wait_result = condvar.wait_timeout(state, wait_time);
                let wait_result = wait_result.unwrap_or_else(PoisonError::into_inner);

                if Self::recheck_limit(mem_stat) {
                    return Ok(());
                }

                state = wait_result.0;

                // Query killed, but memory is still insufficient
                if !wait_result.1.timed_out() {
                    all_timeout = false;
                    break;
                }
            }

            exclude.insert(mem_stat_id);

            if all_timeout {
                log::warn!("Timeout: Unable to kill query in 10 seconds");
            }
        }
    }

    pub fn release_memory(&self, mem_stat: &MemStat) {
        let mut state = self.get_state();

        let mut parent = mem_stat.parent_memory_stat.as_ref();

        while let Some(parent_mem_stat) = parent.take() {
            if let Entry::Occupied(mut v) = state.mem_stat.entry(parent_mem_stat.id) {
                v.get_mut().remove(&mem_stat.id);

                if v.get_mut().is_empty() {
                    state.mem_stat.remove(&parent_mem_stat.id);
                }
            };

            parent = parent_mem_stat.parent_memory_stat.as_ref();
            if parent.is_none() && parent_mem_stat.id != 0 {
                // Global mem state
                if let Entry::Occupied(mut v) = state.mem_stat.entry(0) {
                    v.get_mut().remove(&mem_stat.id);
                }
            }
        }

        if let Some(query_memory_info) = state.running_queries.remove(&mem_stat.id) {
            query_memory_info.condvar.notify_all();
        }
    }

    fn get_state(&self) -> MutexGuard<'_, ExceededMemoryState> {
        self.state
            .get_or_init(|| {
                Mutex::new(ExceededMemoryState {
                    mem_stat: Default::default(),
                    running_queries: Default::default(),
                })
            })
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    use crate::base::GlobalUniqName;
    use crate::runtime::memory::mem_stat::MEMORY_LIMIT_EXCEEDED_NO_ERROR;
    use crate::runtime::memory::mem_stat::MEMORY_LIMIT_EXCEEDED_REPORTING_ERROR;
    use crate::runtime::memory::memory_manager::QueriesMemoryManager;
    use crate::runtime::MemStat;
    use crate::runtime::OutOfLimit;
    use crate::runtime::Thread;
    use crate::runtime::ThreadTracker;

    #[test]
    fn test_exclusion_list() {
        let mut queries = HashMap::new();
        queries.insert(1, ("a".into(), 1));
        queries.insert(2, ("b".into(), 1));
        queries.insert(3, ("c".into(), 1));

        let exclude: HashSet<usize> = [1, 2].iter().cloned().collect();
        let selected = QueriesMemoryManager::select_children(&queries, &exclude);
        assert_eq!(selected, Some(3));
    }

    #[test]
    fn test_tie_breaker_with_query_id() {
        let mut queries = HashMap::new();
        queries.insert(1, ("query_2".into(), 1));
        queries.insert(2, ("query_1".into(), 1));

        let selected = QueriesMemoryManager::select_children(&queries, &HashSet::new());
        assert_eq!(selected, Some(2));
    }

    #[test]
    fn test_global_parent_registration() {
        let manager = QueriesMemoryManager::create();
        let parent = MemStat::create(GlobalUniqName::unique());
        let child = MemStat::create_child(
            GlobalUniqName::unique(),
            Some(parent.clone()),
            0,
            AtomicBool::new(true),
        );

        let mut payload = ThreadTracker::new_tracking_payload();
        payload.query_id = Some(GlobalUniqName::unique());
        let _guard = ThreadTracker::tracking(payload);

        manager.request_exceeded_memory(&child, Arc::new(AtomicUsize::new(0)));

        let state = manager.get_state();
        assert!(state.mem_stat[&0].contains_key(&child.id));
        assert!(state.mem_stat[&parent.id].contains_key(&child.id));

        assert!(!state.mem_stat[&0].contains_key(&parent.id));
        assert!(!state.mem_stat[&parent.id].contains_key(&0));
    }

    #[test]
    #[should_panic(expected = "unreachable")]
    fn test_duplicate_registration_panic() {
        let manager = QueriesMemoryManager::create();
        let mem_stat = MemStat::create(GlobalUniqName::unique());

        let mut payload = ThreadTracker::new_tracking_payload();
        payload.query_id = Some(GlobalUniqName::unique());
        let _guard = ThreadTracker::tracking(payload);

        let mode = Arc::new(AtomicUsize::new(0));
        manager.request_exceeded_memory(&mem_stat, mode.clone());
        manager.request_exceeded_memory(&mem_stat, mode);
    }

    #[test]
    fn test_self_termination_when_no_children() {
        let manager = QueriesMemoryManager::create();
        let mem_stat = MemStat::create(GlobalUniqName::unique());
        let mode = Arc::new(AtomicUsize::new(MEMORY_LIMIT_EXCEEDED_NO_ERROR));

        {
            let mut payload = ThreadTracker::new_tracking_payload();
            payload.query_id = Some(GlobalUniqName::unique());
            let _guard = ThreadTracker::tracking(payload);

            manager.request_exceeded_memory(&mem_stat, mode.clone());
        }

        let result = manager.wait_memory(&mem_stat, OutOfLimit::new(0, 0));
        assert!(result.is_err());
        assert_eq!(
            mode.load(Ordering::SeqCst),
            MEMORY_LIMIT_EXCEEDED_REPORTING_ERROR
        );
    }

    #[test]
    fn test_priority_based_selection() {
        let manager = QueriesMemoryManager::create();
        let parent = MemStat::create(GlobalUniqName::unique());

        let children = vec![
            (1, 3, "low_pri".into()),
            (2, 2, "mid_pri".into()),
            (3, 1, "high_pri".into()),
        ];

        for (_id, pri, qid) in children {
            let mut payload = ThreadTracker::new_tracking_payload();
            payload.query_id = Some(qid);
            let _guard = ThreadTracker::tracking(payload);

            let mem = MemStat::create_child(
                GlobalUniqName::unique(),
                Some(parent.clone()),
                pri,
                AtomicBool::new(true),
            );
            manager.request_exceeded_memory(
                &mem,
                Arc::new(AtomicUsize::new(MEMORY_LIMIT_EXCEEDED_NO_ERROR)),
            );
        }

        let target = MemStat::create_child(
            GlobalUniqName::unique(),
            Some(parent.clone()),
            0,
            AtomicBool::new(true),
        );
        let result = manager.wait_memory(&target, OutOfLimit::new(0, 0));

        // recheck limit passed.
        assert!(result.is_ok());
        let state = manager.get_state();

        let killed_children = state
            .running_queries
            .iter()
            .filter(|(_, x)| {
                x.exceeded_memory_flag.load(Ordering::SeqCst)
                    == MEMORY_LIMIT_EXCEEDED_REPORTING_ERROR
            })
            .map(|(x, y)| (*x, y.clone()))
            .collect::<Vec<_>>();

        assert_eq!(killed_children.len(), 1);
        assert_eq!(state.mem_stat[&0][&killed_children[0].0].0, "high_pri");
        assert_eq!(
            state.mem_stat[&parent.id][&killed_children[0].0].0,
            "high_pri"
        );
    }

    #[test]
    fn test_condvar_notification_with_no_child() {
        let manager = Arc::new(QueriesMemoryManager::create());
        let parent = MemStat::create(GlobalUniqName::unique());
        let child = MemStat::create_child(
            GlobalUniqName::unique(),
            Some(parent.clone()),
            0,
            AtomicBool::new(true),
        );

        let child_mode = Arc::new(AtomicUsize::new(MEMORY_LIMIT_EXCEEDED_NO_ERROR));
        {
            let mut payload = ThreadTracker::new_tracking_payload();
            payload.query_id = Some(GlobalUniqName::unique());
            let _guard = ThreadTracker::tracking(payload);

            manager.request_exceeded_memory(&child, child_mode.clone());
        }

        let manager_clone = manager.clone();
        let child_clone = child.clone();
        let handle = Thread::spawn(move || {
            let instant = Instant::now();
            assert!(manager_clone
                .wait_memory(&child_clone, OutOfLimit::new(0, 0))
                .is_err());
            Ok::<_, OutOfLimit>(instant.elapsed())
        });

        std::thread::sleep(Duration::from_millis(1000));

        manager.release_memory(&child);
        assert!(handle.join().unwrap().unwrap() < Duration::from_millis(1000));
    }

    #[test]
    fn test_condvar_notification() {
        let manager = Arc::new(QueriesMemoryManager::create());
        let parent = MemStat::create(GlobalUniqName::unique());
        let child = MemStat::create_child(
            GlobalUniqName::unique(),
            Some(parent.clone()),
            0,
            AtomicBool::new(true),
        );

        let child_mode = Arc::new(AtomicUsize::new(MEMORY_LIMIT_EXCEEDED_NO_ERROR));
        {
            let mut payload = ThreadTracker::new_tracking_payload();
            payload.query_id = Some(GlobalUniqName::unique());
            let _guard = ThreadTracker::tracking(payload);
            manager.request_exceeded_memory(&child, child_mode.clone());
        }

        // limit 500MB
        parent.set_limit(500 * 1024 * 1024);
        // used 1GB
        parent.used.fetch_add(1024 * 1024 * 1024, Ordering::SeqCst);

        let manager_clone = manager.clone();
        let handle = Thread::spawn(move || {
            let instant = Instant::now();
            let target = MemStat::create_child(
                GlobalUniqName::unique(),
                Some(parent.clone()),
                0,
                AtomicBool::new(true),
            );
            assert!(manager_clone
                .wait_memory(&target, OutOfLimit::new(0, 0))
                .is_err());
            Ok::<_, OutOfLimit>(instant.elapsed())
        });

        std::thread::sleep(Duration::from_millis(1000));

        manager.release_memory(&child);

        assert!(handle.join().unwrap().unwrap() > Duration::from_millis(1000));
    }

    #[test]
    fn test_condvar_notification_with_recheck() {
        let manager = Arc::new(QueriesMemoryManager::create());
        let parent = MemStat::create(GlobalUniqName::unique());
        let child = MemStat::create_child(
            GlobalUniqName::unique(),
            Some(parent.clone()),
            0,
            AtomicBool::new(true),
        );

        let child_mode = Arc::new(AtomicUsize::new(MEMORY_LIMIT_EXCEEDED_NO_ERROR));
        {
            let mut payload = ThreadTracker::new_tracking_payload();
            payload.query_id = Some(GlobalUniqName::unique());
            let _guard = ThreadTracker::tracking(payload);
            manager.request_exceeded_memory(&child, child_mode.clone());
        }

        // limit 500MB
        parent.set_limit(500 * 1024 * 1024);
        // used 1GB
        parent.used.fetch_add(1024 * 1024 * 1024, Ordering::SeqCst);

        let handle = Thread::spawn({
            let parent = parent.clone();
            let manager = manager.clone();
            move || {
                let instant = Instant::now();
                let target = MemStat::create_child(
                    GlobalUniqName::unique(),
                    Some(parent.clone()),
                    0,
                    AtomicBool::new(true),
                );
                assert!(manager.wait_memory(&target, OutOfLimit::new(0, 0)).is_ok());
                Ok::<_, OutOfLimit>(instant.elapsed())
            }
        });

        std::thread::sleep(Duration::from_millis(1000));

        // reduce memory used.
        parent.used.fetch_sub(800 * 1024 * 1024, Ordering::SeqCst);

        std::thread::sleep(Duration::from_millis(1000));

        // release memory
        manager.release_memory(&child);

        let duration = handle.join().unwrap().unwrap();
        assert!(duration > Duration::from_millis(1000));
        assert!(duration < Duration::from_millis(2000));
    }

    #[test]
    fn test_cleanup_after_release() {
        let manager = QueriesMemoryManager::create();
        let parent = MemStat::create(GlobalUniqName::unique());
        let child = MemStat::create_child(
            GlobalUniqName::unique(),
            Some(parent.clone()),
            0,
            AtomicBool::new(true),
        );

        let mut payload = ThreadTracker::new_tracking_payload();
        payload.query_id = Some(GlobalUniqName::unique());
        let _guard = ThreadTracker::tracking(payload);

        manager.request_exceeded_memory(&child, Arc::new(AtomicUsize::new(0)));

        manager.release_memory(&child);
        let state = manager.get_state();

        assert!(state.running_queries.is_empty());
        assert!(!state.mem_stat[&0].contains_key(&child.id));
        assert!(!state.mem_stat[&0].contains_key(&parent.id));
        assert!(!state.mem_stat.contains_key(&parent.id))
    }
}
