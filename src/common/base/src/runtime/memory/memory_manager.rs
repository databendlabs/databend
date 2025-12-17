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
use std::collections::hash_map::Entry;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use crate::runtime::MemStat;
use crate::runtime::OutOfLimit;
use crate::runtime::memory::mem_stat::ParentMemStat;

pub static GLOBAL_QUERIES_MANAGER: QueriesMemoryManager = QueriesMemoryManager::create();

type ResourceTag = String;
type Priority = usize;

const RUNNING: u8 = 0;
const KILLING: u8 = 1;
const KILLED: u8 = 2;

#[derive(Debug, Clone)]
struct ResourceMemoryInfo {
    state: Arc<AtomicU8>,
    killing_instant: Option<Arc<Instant>>,
}

unsafe impl Send for ResourceMemoryInfo {}

unsafe impl Sync for ResourceMemoryInfo {}

type GcHandler = Arc<dyn Fn(&String, bool) -> bool + Send + Sync + 'static>;

#[derive(Debug)]
struct Group {
    priority: Priority,
    tag: ResourceTag,
    state: Arc<AtomicU8>,
}

impl Group {
    pub fn new(priority: Priority, tag: ResourceTag, state: Arc<AtomicU8>) -> Self {
        Self {
            tag,
            state,
            priority,
        }
    }
}

impl Hash for Group {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.tag.hash(state);
        self.priority.hash(state);
    }
}

impl Eq for Group {}

impl PartialEq<Self> for Group {
    fn eq(&self, other: &Self) -> bool {
        let left_state = self.state.load(Ordering::Acquire);
        let right_state = self.state.load(Ordering::Acquire);

        left_state == right_state && self.priority == other.priority && self.tag == other.tag
    }
}

impl PartialOrd<Self> for Group {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Group {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let left_state = self.state.load(Ordering::Acquire);
        let right_state = self.state.load(Ordering::Acquire);

        match left_state.cmp(&right_state) {
            std::cmp::Ordering::Less => std::cmp::Ordering::Greater,
            std::cmp::Ordering::Greater => std::cmp::Ordering::Less,
            std::cmp::Ordering::Equal => self
                .priority
                .cmp(&other.priority)
                .then(self.tag.cmp(&other.tag)),
        }
    }
}

#[allow(dead_code)]
struct ExceededMemoryState {
    condvar: Arc<Condvar>,
    resources: HashMap<ResourceTag, ResourceMemoryInfo>,
    killed_resources: HashMap<ResourceTag, ResourceMemoryInfo>,
    groups: HashMap<usize, HashSet<Group>>,
    memory_gc_handler: Option<GcHandler>,
}

impl ExceededMemoryState {
    pub fn low_priority_resource(&self, id: usize) -> Option<ResourceTag> {
        let resources = self.groups.get(&id)?;
        resources.iter().min().map(|group| group.tag.clone())
    }

    pub fn release(&mut self, id: &ResourceTag) {
        if let Some((key, info)) = self.resources.remove_entry(id) {
            info.state.store(KILLED, Ordering::Release);
            self.killed_resources.insert(key, info);
        }

        for value in self.groups.values_mut() {
            value.retain(|group| &group.tag != id);
        }

        self.condvar.notify_all();
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

        let priority = mem_stat.priority;
        let resource_state = Arc::new(AtomicU8::new(RUNNING));
        let mut state = self.get_state();

        if state.killed_resources.contains_key(resource_tag) {
            return;
        }

        let mut parent = &mem_stat.parent_memory_stat;

        loop {
            match parent {
                ParentMemStat::Root => {
                    break;
                }
                ParentMemStat::StaticRef(_) => {
                    match state.groups.entry(0) {
                        Entry::Vacant(v) => {
                            v.insert(HashSet::new()).insert(Group::new(
                                priority,
                                resource_tag.clone(),
                                resource_state.clone(),
                            ));
                        }
                        Entry::Occupied(mut v) => {
                            v.get_mut().insert(Group::new(
                                priority,
                                resource_tag.clone(),
                                resource_state.clone(),
                            ));
                        }
                    };

                    break;
                }
                ParentMemStat::Normal(parent_mem_stat) => {
                    match state.groups.entry(parent_mem_stat.id) {
                        Entry::Vacant(v) => {
                            v.insert(HashSet::new()).insert(Group::new(
                                priority,
                                resource_tag.clone(),
                                resource_state.clone(),
                            ));
                        }
                        Entry::Occupied(mut v) => {
                            v.get_mut().insert(Group::new(
                                priority,
                                resource_tag.clone(),
                                resource_state.clone(),
                            ));
                        }
                    };

                    parent = &parent_mem_stat.parent_memory_stat;
                }
            }
        }

        let Entry::Vacant(v) = state.resources.entry(resource_tag.clone()) else {
            unreachable!(
                "Memory-exceeding trackers require strict one-to-one binding with queries."
            );
        };

        v.insert(ResourceMemoryInfo {
            killing_instant: None,
            state: resource_state,
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

            if state.killed_resources.contains_key(waiting_id) {
                // kill by other
                return Ok(());
            }

            if let Some(waiting_resource) = state.resources.get(waiting_id) {
                // kill by other
                if waiting_resource.state.load(Ordering::Acquire) != RUNNING {
                    return Ok(());
                }
            }

            if Self::recheck_limit(mem_stat) {
                return Ok(());
            }

            let Some(terminate_id) = state.low_priority_resource(parent_id) else {
                return Err(cause);
            };

            if Self::recheck_limit(mem_stat) {
                return Ok(());
            }

            let Some(gc_handler) = state.memory_gc_handler.clone() else {
                return Err(cause);
            };

            let Entry::Occupied(mut entry) = state.resources.entry(terminate_id.clone()) else {
                debug_assert!(false, "Logical error");
                continue;
            };

            let terminate_state = entry.get().state.clone();

            if let Some(instant) = entry.get().killing_instant.as_ref() {
                // gc timeout, ignore this.
                if instant.elapsed() > Duration::from_secs(10) {
                    state.release(&terminate_id);
                    continue;
                }
            }

            if Self::recheck_limit(mem_stat) {
                return Ok(());
            }

            if entry.get().state.load(Ordering::Acquire) == RUNNING {
                entry.get().state.store(KILLING, Ordering::Release);
                entry.get_mut().killing_instant = Some(Arc::new(Instant::now()));

                drop(state);
                if !gc_handler(&terminate_id, false) && !gc_handler(&terminate_id, true) {
                    let mut state = self.get_state();
                    state.release(&terminate_id);

                    if Self::recheck_limit(mem_stat) {
                        return Ok(());
                    }

                    // Try kill next.
                    continue;
                }

                state = self.get_state();
            }

            // Kill self, skip wait
            if &terminate_id == waiting_id {
                return Ok(());
            }

            let condvar = state.condvar.clone();

            // max wait 10 seconds per query
            for _index in 0..100 {
                if terminate_state.load(Ordering::Acquire) == KILLED {
                    if Self::recheck_limit(mem_stat) {
                        return Ok(());
                    }

                    break;
                }

                let wait_time = Duration::from_millis(100);
                let wait_result = condvar.wait_timeout(state, wait_time);
                let wait_result = wait_result.unwrap_or_else(PoisonError::into_inner);

                if Self::recheck_limit(mem_stat) {
                    return Ok(());
                }

                state = wait_result.0;

                if wait_result.1.timed_out() {
                    continue;
                }

                if state.killed_resources.contains_key(waiting_id) {
                    // kill by other while in wait
                    return Ok(());
                }

                if let Some(waiting_resource) = state.resources.get(waiting_id)
                    && waiting_resource.state.load(Ordering::Acquire) != RUNNING
                {
                    // kill by other while in wait
                    return Ok(());
                }

                if terminate_state.load(Ordering::Acquire) != KILLED {
                    continue;
                }

                break;
            }
        }

        Err(cause)
    }

    pub fn release_memory(&self, tag: Option<&ResourceTag>) {
        let Some(terminate_id) = tag else {
            return;
        };

        let mut state = self.get_state();
        state.release(terminate_id);
    }

    pub fn drop_memory_stat(&self, tag: Option<&ResourceTag>) {
        let Some(drop_id) = tag else {
            return;
        };

        let mut state = self.get_state();
        state.release(drop_id);
        state.killed_resources.remove(drop_id);
    }

    fn get_state(&self) -> MutexGuard<'_, ExceededMemoryState> {
        self.state
            .get_or_init(|| {
                Mutex::new(ExceededMemoryState {
                    groups: Default::default(),
                    resources: Default::default(),
                    memory_gc_handler: None,
                    condvar: Arc::new(Condvar::new()),
                    killed_resources: Default::default(),
                })
            })
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicU8;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use crate::runtime::MemStat;
    use crate::runtime::OutOfLimit;
    use crate::runtime::ParentMemStat;
    use crate::runtime::Thread;
    use crate::runtime::memory::memory_manager::Group;
    use crate::runtime::memory::memory_manager::KILLING;
    use crate::runtime::memory::memory_manager::QueriesMemoryManager;
    use crate::runtime::memory::memory_manager::RUNNING;

    fn mock_exceeded_memory(mem_stat: &MemStat, set_water_height: bool) {
        mem_stat.set_limit(257 * 1024 * 1024, set_water_height);
        mem_stat
            .used
            .fetch_add(300 * 1024 * 1024, Ordering::Relaxed);
    }

    fn mock_mem_stat(
        name: String,
        priority: usize,
        parent_memory_stat: ParentMemStat,
        manager: &'static QueriesMemoryManager,
    ) -> Arc<MemStat> {
        let mut mem_stat = MemStat::create_child(Some(name), priority, parent_memory_stat);
        let mut_mem_stat = Arc::get_mut(&mut mem_stat).unwrap();
        mut_mem_stat.queries_memory_manager = manager;
        mem_stat
    }

    #[test]
    fn test_request_and_release() {
        static TEST_MANAGER: QueriesMemoryManager = QueriesMemoryManager::create();
        static TEST_GLOBAL_MEM_STAT: MemStat = MemStat::global(&TEST_MANAGER);

        let tag = "test_request_and_release".to_string();
        let mem_stat = mock_mem_stat(
            tag.clone(),
            1,
            ParentMemStat::StaticRef(&TEST_GLOBAL_MEM_STAT),
            &TEST_MANAGER,
        );

        TEST_MANAGER.request_exceeded_memory(&mem_stat, Some(&tag));

        {
            let state = TEST_MANAGER.get_state();
            assert!(state.resources.contains_key(&tag));
            assert!(state.groups.get(&0).unwrap().contains(&Group::new(
                1,
                tag.clone(),
                Arc::new(AtomicU8::new(RUNNING))
            )));
        }

        TEST_MANAGER.release_memory(Some(&tag));

        {
            let state = TEST_MANAGER.get_state();
            assert!(state.killed_resources.contains_key(&tag));
            assert!(!state.resources.contains_key(&tag));
            assert!(state.groups.get(&0).unwrap().is_empty());
        }
    }

    #[test]
    fn test_gc_handler_execution() {
        static TEST_MANAGER: QueriesMemoryManager = QueriesMemoryManager::create();
        static TEST_GLOBAL_MEM_STAT: MemStat = MemStat::global(&TEST_MANAGER);

        let tag = "test_gc_handler".to_string();
        let handler_called = Arc::new(AtomicBool::new(false));

        TEST_MANAGER.set_gc_handle({
            let tag = tag.clone();
            let handler_called = handler_called.clone();
            move |t, force| {
                assert_eq!(t, &tag);
                handler_called.store(true, Ordering::Relaxed);
                force
            }
        });

        let mem_stat = mock_mem_stat(
            tag.clone(),
            2,
            ParentMemStat::StaticRef(&TEST_GLOBAL_MEM_STAT),
            &TEST_MANAGER,
        );
        TEST_MANAGER.request_exceeded_memory(&mem_stat, Some(&tag));

        mock_exceeded_memory(&TEST_GLOBAL_MEM_STAT, false);
        let cause = OutOfLimit::new(100, 50);
        let result = TEST_MANAGER.wait_memory(&mem_stat, Some(&tag), cause.clone());

        assert!(result.is_ok());
        assert!(handler_called.load(Ordering::Relaxed));

        let state = TEST_MANAGER.get_state();
        assert_eq!(
            state
                .resources
                .get(&tag)
                .unwrap()
                .state
                .load(Ordering::Acquire),
            KILLING
        );
    }

    #[test]
    fn test_priority_order() {
        static TEST_MANAGER: QueriesMemoryManager = QueriesMemoryManager::create();
        static TEST_GLOBAL_MEM_STAT: MemStat = MemStat::global(&TEST_MANAGER);

        let low_tag = "low_priority".to_string();
        let high_tag = "high_priority".to_string();

        let low_mem = mock_mem_stat(
            low_tag.clone(),
            1,
            ParentMemStat::StaticRef(&TEST_GLOBAL_MEM_STAT),
            &TEST_MANAGER,
        );
        let high_mem = mock_mem_stat(
            high_tag.clone(),
            2,
            ParentMemStat::StaticRef(&TEST_GLOBAL_MEM_STAT),
            &TEST_MANAGER,
        );

        TEST_MANAGER.request_exceeded_memory(&low_mem, Some(&low_tag));
        TEST_MANAGER.request_exceeded_memory(&high_mem, Some(&high_tag));

        let state = TEST_MANAGER.get_state();
        let target = state.low_priority_resource(0).unwrap();
        assert_eq!(target, low_tag);
    }

    #[test]
    fn test_timeout_release() {
        static TEST_GLOBAL_MEM_STAT: MemStat = MemStat::global(&TEST_MANAGER);
        static TEST_MANAGER: QueriesMemoryManager = QueriesMemoryManager::create();

        let tag = "test_timeout".to_string();
        TEST_MANAGER.set_gc_handle(|_, _| false);

        let mem_stat = mock_mem_stat(
            tag.clone(),
            1,
            ParentMemStat::StaticRef(&TEST_GLOBAL_MEM_STAT),
            &TEST_MANAGER,
        );
        TEST_MANAGER.request_exceeded_memory(&mem_stat, Some(&tag));

        mock_exceeded_memory(&TEST_GLOBAL_MEM_STAT, false);
        let cause = OutOfLimit::new(100, 50);
        let _ = TEST_MANAGER.wait_memory(&mem_stat, Some(&tag), cause);

        let state = TEST_MANAGER.get_state();
        assert!(!state.resources.contains_key(&tag));
        assert!(state.killed_resources.contains_key(&tag));
    }

    #[test]
    fn test_drop_memory_stat() {
        static TEST_MANAGER: QueriesMemoryManager = QueriesMemoryManager::create();
        static TEST_GLOBAL_MEM_STAT: MemStat = MemStat::global(&TEST_MANAGER);

        let tag = "test_drop".to_string();
        let mem_stat = mock_mem_stat(
            tag.clone(),
            1,
            ParentMemStat::StaticRef(&TEST_GLOBAL_MEM_STAT),
            &TEST_MANAGER,
        );

        TEST_MANAGER.request_exceeded_memory(&mem_stat, Some(&tag));
        TEST_MANAGER.drop_memory_stat(Some(&tag));

        let state = TEST_MANAGER.get_state();
        assert!(!state.resources.contains_key(&tag));
        assert!(!state.killed_resources.contains_key(&tag));
    }

    const CONCURRENT_THREADS: usize = 16;
    const OPERATIONS_PER_THREAD: usize = 100;

    #[test]
    fn test_concurrent_resource_release_management() {
        static CHAOS_MANAGER: QueriesMemoryManager = QueriesMemoryManager::create();
        static CHAOS_GLOBAL_MEM: MemStat = MemStat::global(&CHAOS_MANAGER);

        let barrier = Arc::new(Barrier::new(CONCURRENT_THREADS));
        let counter = Arc::new(AtomicU64::new(0));

        let handles: Vec<_> = (0..CONCURRENT_THREADS)
            .map(|_| {
                let barrier = barrier.clone();
                let counter = counter.clone();
                Thread::spawn(move || {
                    barrier.wait();

                    for _ in 0..OPERATIONS_PER_THREAD {
                        let tag = format!("res-{}", counter.fetch_add(1, Ordering::SeqCst));
                        let mem_stat = mock_mem_stat(
                            tag.clone(),
                            rand::random::<usize>() % 5,
                            ParentMemStat::StaticRef(&CHAOS_GLOBAL_MEM),
                            &CHAOS_MANAGER,
                        );

                        if rand::random::<f32>() < 0.7 {
                            CHAOS_MANAGER.request_exceeded_memory(&mem_stat, Some(&tag));
                        } else {
                            CHAOS_MANAGER.release_memory(Some(&tag));
                        }

                        std::thread::sleep(Duration::from_millis(rand::random::<u64>() % 10));
                        drop(mem_stat);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let state = CHAOS_MANAGER.get_state();
        assert!(
            state.resources.is_empty(),
            "All resources should be released, found: {:?}",
            state.resources.keys()
        );
        assert!(
            state.groups.values().all(|g| g.is_empty()),
            "All groups should be empty"
        );
    }

    #[test]
    fn test_chaotic_gc_handling() {
        static CHAOS_GLOBAL_MEM: MemStat = MemStat::global(&CHAOS_MANAGER);
        static CHAOS_MANAGER: QueriesMemoryManager = QueriesMemoryManager::create();

        let gc_counter = Arc::new(AtomicU64::new(0));
        CHAOS_MANAGER.set_gc_handle({
            let gc_counter = gc_counter.clone();
            move |_tag, force| {
                std::thread::sleep(Duration::from_millis(rand::random::<u64>() % 50));
                gc_counter.fetch_add(1, Ordering::Relaxed);
                force
            }
        });

        mock_exceeded_memory(&CHAOS_GLOBAL_MEM, false);
        let counter = Arc::new(AtomicU64::new(0));
        let handles: Vec<_> = (0..CONCURRENT_THREADS)
            .map(|_| {
                Thread::spawn({
                    let counter = counter.clone();
                    move || {
                        for _i in 0..OPERATIONS_PER_THREAD {
                            let tag = format!("gc-{}", counter.fetch_add(1, Ordering::SeqCst));
                            let mem_stat = mock_mem_stat(
                                tag.clone(),
                                1,
                                ParentMemStat::StaticRef(&CHAOS_GLOBAL_MEM),
                                &CHAOS_MANAGER,
                            );

                            CHAOS_MANAGER.request_exceeded_memory(&mem_stat, Some(&tag));

                            let _ = CHAOS_MANAGER.wait_memory(
                                &mem_stat,
                                Some(&tag),
                                OutOfLimit::new(100, 50),
                            );
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert!(
            gc_counter.load(Ordering::Relaxed) >= CONCURRENT_THREADS as u64,
            "GC handler should be called multiple times"
        );
    }
}
