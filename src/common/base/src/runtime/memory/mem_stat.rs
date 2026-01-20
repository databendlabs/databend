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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

use bytesize::ByteSize;
use databend_base::uniq_id::GlobalSeq;

use crate::runtime::LimitMemGuard;
use crate::runtime::memory::memory_manager::GLOBAL_QUERIES_MANAGER;
use crate::runtime::memory::memory_manager::QueriesMemoryManager;

/// The program mem stat
///
/// Every alloc/dealloc stat will be fed to this mem stat.
pub static GLOBAL_MEM_STAT: MemStat = MemStat::global(&GLOBAL_QUERIES_MANAGER);

const MINIMUM_MEMORY_LIMIT: i64 = 256 * 1024 * 1024;

#[derive(Default, Debug)]
struct MemoryLimit {
    limit: AtomicI64,
    set_limit: AtomicI64,
    water_height: AtomicI64,
}

impl MemoryLimit {
    pub const fn new() -> MemoryLimit {
        MemoryLimit {
            limit: AtomicI64::new(0),
            set_limit: AtomicI64::new(0),
            water_height: AtomicI64::new(0),
        }
    }
}

pub enum ParentMemStat {
    Root,
    StaticRef(&'static MemStat),
    Normal(Arc<MemStat>),
}

/// Memory allocation stat.
///
/// - A MemStat have child MemStat.
/// - Every stat that is fed to a child is also fed to its parent.
/// - A MemStat has at most one parent.
pub struct MemStat {
    pub id: usize,

    pub(crate) used: AtomicI64,
    pub(crate) peak_used: AtomicI64,
    pub exceeded_mutex: Mutex<bool>,

    memory_limit: MemoryLimit,
    exceeded_memory: AtomicBool,

    memory_requester: Option<String>,
    pub(crate) queries_memory_manager: &'static QueriesMemoryManager,

    pub(crate) priority: usize,

    pub(crate) parent_memory_stat: ParentMemStat,
}

impl MemStat {
    pub const fn global(queries_memory_manager: &'static QueriesMemoryManager) -> Self {
        Self {
            id: 0,
            queries_memory_manager,
            used: AtomicI64::new(0),
            peak_used: AtomicI64::new(0),
            parent_memory_stat: ParentMemStat::Root,
            priority: 0,
            exceeded_mutex: Mutex::new(false),
            memory_limit: MemoryLimit::new(),
            exceeded_memory: AtomicBool::new(false),
            memory_requester: None,
        }
    }

    pub fn create(name: String) -> Arc<MemStat> {
        MemStat::create_child(Some(name), 0, ParentMemStat::StaticRef(&GLOBAL_MEM_STAT))
    }

    pub fn create_workload_group() -> Arc<MemStat> {
        MemStat::create_child(None, 0, ParentMemStat::StaticRef(&GLOBAL_MEM_STAT))
    }

    pub fn create_child(
        name: Option<String>,
        priority: usize,
        parent_memory_stat: ParentMemStat,
    ) -> Arc<MemStat> {
        let id = match GlobalSeq::next() {
            0 => GlobalSeq::next(),
            id => id,
        };

        Arc::new(MemStat {
            id,
            priority,
            parent_memory_stat,
            used: AtomicI64::new(0),
            peak_used: AtomicI64::new(0),
            exceeded_mutex: Mutex::new(false),
            memory_limit: MemoryLimit::default(),
            exceeded_memory: AtomicBool::new(false),
            memory_requester: name,
            queries_memory_manager: &GLOBAL_QUERIES_MANAGER,
        })
    }

    pub fn set_limit(&self, mut size: i64, set_water_height: bool) {
        // It may cause the process unable to run if memory limit is too low.
        if size > 0 && size < MINIMUM_MEMORY_LIMIT {
            size = MINIMUM_MEMORY_LIMIT;
        }

        if set_water_height {
            let water_height = size / 10 * 4;
            self.memory_limit
                .water_height
                .store(water_height, Ordering::Relaxed);
        }

        let used = self.used.load(Ordering::Relaxed);
        self.memory_limit.limit.store(size, Ordering::Relaxed);
        self.memory_limit.set_limit.store(size, Ordering::Relaxed);

        #[allow(clippy::collapsible_if)]
        if used < size && self.memory_requester.is_some() {
            if self.exceeded_memory.fetch_and(false, Ordering::SeqCst) {
                let _guard = LimitMemGuard::enter_unlimited();

                let mutex = self.exceeded_mutex.lock();
                let mut mutex = mutex.unwrap_or_else(PoisonError::into_inner);

                if *mutex {
                    *mutex = false;
                    self.queries_memory_manager
                        .release_memory(self.memory_requester.as_ref());
                }
            }
        }
    }

    pub fn get_limit(&self) -> i64 {
        self.memory_limit.limit.load(Ordering::Relaxed)
    }

    /// Feed memory usage stat to MemStat and return if it exceeds the limit.
    ///
    /// It feeds `state` to the this tracker and all of its ancestors, including GLOBAL_TRACKER.
    pub fn record_memory<const NEED_ROLLBACK: bool>(
        &self,
        alloc_memory: i64,
        rollback_memory: i64,
    ) -> Result<(), OutOfLimit> {
        if let Err(cause) = self.record_memory_impl::<false>(alloc_memory, 0, None) {
            if !cause.export_error {
                return Ok(());
            }

            if NEED_ROLLBACK {
                self.rollback(rollback_memory);
            }

            return Err(cause);
        }

        Ok(())
    }

    fn parent_record_memory<const NEED_ROLLBACK: bool>(
        &self,
        alloc_memory: i64,
        rollback_memory: i64,
        memory_requester: Option<&String>,
    ) -> Result<(), OutOfLimit> {
        match &self.parent_memory_stat {
            ParentMemStat::Root => Ok(()),
            ParentMemStat::StaticRef(global) => global.record_memory_impl::<NEED_ROLLBACK>(
                alloc_memory,
                rollback_memory,
                memory_requester,
            ),
            ParentMemStat::Normal(mem_stat) => mem_stat.record_memory_impl::<NEED_ROLLBACK>(
                alloc_memory,
                rollback_memory,
                memory_requester,
            ),
        }
    }

    #[inline]
    fn record_memory_impl<const NEED_ROLLBACK: bool>(
        &self,
        alloc_memory: i64,
        rollback_memory: i64,
        memory_requester: Option<&String>,
    ) -> Result<(), OutOfLimit> {
        let mut used = self.used.fetch_add(alloc_memory, Ordering::Relaxed);

        used += alloc_memory;
        self.peak_used.fetch_max(used, Ordering::Relaxed);

        // Bottom-up search to find the requester closest to the actual allocated memory location.
        let memory_requester = match memory_requester {
            Some(v) => Some(v),
            None => self.memory_requester.as_ref(),
        };

        if let Err(cause) =
            self.parent_record_memory::<false>(alloc_memory, rollback_memory, memory_requester)
        {
            // neighbor may exceeded limit, wait release memory
            if alloc_memory > 0
                && let Err(mut cause) = self.wait_memory(memory_requester, cause)
            {
                if NEED_ROLLBACK {
                    // We only roll back the memory that alloc failed
                    self.rollback(rollback_memory);
                }

                cause.export_error |= self.memory_requester.is_some();
                return Err(cause);
            }
        }

        if let Err(cause) = self.check_limit(used) {
            // parent has memory free, try exceeding limit.
            if let Err(mut cause) = self.exceeding_limit(used, memory_requester, cause) {
                if NEED_ROLLBACK {
                    // NOTE: we cannot rollback peak_used of parent mem stat in this case
                    // self.peak_used.store(peak_used, Ordering::Relaxed);
                    self.rollback(rollback_memory);
                }

                cause.export_error |= self.memory_requester.is_some();
                return Err(cause);
            }
        }

        Ok(())
    }

    pub fn wait_memory(&self, id: Option<&String>, oom: OutOfLimit) -> Result<(), OutOfLimit> {
        if std::thread::panicking() || LimitMemGuard::is_unlimited() {
            return Ok(());
        }

        let _guard = LimitMemGuard::enter_unlimited();

        if id.is_some() {
            self.queries_memory_manager.wait_memory(self, id, oom)?;
            return Ok(());
        }

        Err(oom)
    }

    fn exceeding_limit(
        &self,
        used: i64,
        tag: Option<&String>,
        oom: OutOfLimit,
    ) -> Result<(), OutOfLimit> {
        if std::thread::panicking() || LimitMemGuard::is_unlimited() {
            return Ok(());
        }

        if !oom.allow_exceeded_limit {
            return Err(oom);
        }

        let _guard = LimitMemGuard::enter_unlimited();
        let exceeded_mutex = self.exceeded_mutex.lock();
        let mut exceeded_mutex = exceeded_mutex.unwrap_or_else(PoisonError::into_inner);

        let parent_memory_limit = match &self.parent_memory_stat {
            ParentMemStat::Root => &GLOBAL_MEM_STAT.memory_limit,
            ParentMemStat::StaticRef(global) => &global.memory_limit,
            ParentMemStat::Normal(mem_stat) => &mem_stat.memory_limit,
        };

        let parent_limit = parent_memory_limit.limit.load(Ordering::Relaxed);

        if parent_limit >= oom.limit {
            self.memory_limit
                .limit
                .store(parent_limit, Ordering::Relaxed);

            if !*exceeded_mutex {
                *exceeded_mutex = true;
                self.exceeded_memory.store(true, Ordering::SeqCst);
                self.queries_memory_manager
                    .request_exceeded_memory(self, tag);
            }
        }

        // recheck limit
        match self.check_limit(used) {
            Ok(_) => Ok(()),
            Err(mut out_of_limit) => {
                out_of_limit.allow_exceeded_limit = false;
                Err(out_of_limit)
            }
        }
    }

    pub fn rollback(&self, memory_usage: i64) {
        self.used.fetch_sub(memory_usage, Ordering::Relaxed);

        match &self.parent_memory_stat {
            ParentMemStat::Root => {}
            ParentMemStat::StaticRef(global) => global.rollback(memory_usage),
            ParentMemStat::Normal(mem_stat) => mem_stat.rollback(memory_usage),
        };
    }

    /// Check if used memory is out of the limit.
    #[inline]
    fn check_limit(&self, used: i64) -> Result<(), OutOfLimit> {
        let limit = self.memory_limit.limit.load(Ordering::Relaxed);
        let water_height = self.memory_limit.water_height.load(Ordering::Relaxed);

        if limit != 0 && used > limit {
            let mut out_of_limit = OutOfLimit::new(used, limit);
            out_of_limit.allow_exceeded_limit = water_height != 0;
            return Err(out_of_limit);
        }

        #[allow(clippy::collapsible_if)]
        if water_height != 0 && used <= water_height && self.memory_requester.is_some() {
            if self.exceeded_memory.fetch_and(false, Ordering::SeqCst) {
                let _guard = LimitMemGuard::enter_unlimited();
                let revert_limit = self.memory_limit.set_limit.load(Ordering::Relaxed);

                let mutex = self.exceeded_mutex.lock();
                let mut mutex = mutex.unwrap_or_else(PoisonError::into_inner);

                if *mutex {
                    *mutex = false;
                    self.queries_memory_manager
                        .release_memory(self.memory_requester.as_ref());
                    self.memory_limit
                        .limit
                        .store(revert_limit, Ordering::Relaxed);
                }
            }
        }

        Ok(())
    }

    pub(crate) fn recheck_limit(&self) -> Result<(), OutOfLimit> {
        self.check_limit(self.used.load(Ordering::Relaxed))
    }

    #[inline]
    pub fn get_memory_usage(&self) -> usize {
        std::cmp::max(self.used.load(Ordering::Relaxed), 0) as usize
    }

    #[inline]
    pub fn get_peak_memory_usage(&self) -> i64 {
        self.peak_used.load(Ordering::Relaxed)
    }
}

/// Error of exceeding limit.
#[derive(Clone)]
pub struct OutOfLimit<V = i64> {
    pub value: V,
    pub limit: V,
    pub export_error: bool,
    pub allow_exceeded_limit: bool,
}

impl<V> OutOfLimit<V> {
    pub fn new(value: V, limit: V) -> Self {
        let _guard = LimitMemGuard::enter_unlimited();
        Self {
            value,
            limit,
            export_error: false,
            allow_exceeded_limit: false,
        }
    }
}

impl Debug for OutOfLimit<i64> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "memory usage {}({}) exceeds limit {}({})",
            ByteSize::b(self.value as u64),
            self.value,
            ByteSize::b(self.limit as u64),
            self.limit,
        )
    }
}

impl Drop for MemStat {
    fn drop(&mut self) {
        self.queries_memory_manager
            .drop_memory_stat(self.memory_requester.as_ref());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use databend_common_exception::Result;

    use crate::runtime::MemStat;
    use crate::runtime::memory::mem_stat::MINIMUM_MEMORY_LIMIT;
    use crate::runtime::memory::mem_stat::ParentMemStat;

    #[test]
    fn test_single_level_mem_stat() -> Result<()> {
        let mem_stat = MemStat::create("TEST".to_string());

        mem_stat.record_memory::<false>(1, 1).unwrap();
        mem_stat.record_memory::<false>(2, 2).unwrap();
        mem_stat.record_memory::<false>(-1, -1).unwrap();

        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 2);

        Ok(())
    }

    #[test]
    fn test_single_level_mem_stat_with_check_limit() -> Result<()> {
        let mem_stat = MemStat::create("TEST".to_string());
        mem_stat.set_limit(MINIMUM_MEMORY_LIMIT, false);

        mem_stat.record_memory::<false>(1, 1).unwrap();
        assert!(
            mem_stat
                .record_memory::<false>(MINIMUM_MEMORY_LIMIT, MINIMUM_MEMORY_LIMIT)
                .is_err()
        );
        assert_eq!(
            mem_stat.used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT
        );

        assert!(mem_stat.record_memory::<false>(1, 1).is_err());
        assert_eq!(
            mem_stat.used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT + 1
        );

        assert!(mem_stat.record_memory::<true>(1, 1).is_err());
        assert_eq!(
            mem_stat.used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT + 1
        );

        assert!(mem_stat.record_memory::<true>(-1, -1).is_err());
        assert_eq!(
            mem_stat.used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT + 1
        );

        assert!(mem_stat.record_memory::<false>(-1, -1).is_err());
        assert_eq!(
            mem_stat.used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT
        );

        Ok(())
    }

    #[test]
    fn test_multiple_level_mem_stat() -> Result<()> {
        let mem_stat = MemStat::create("TEST".to_string());
        let child_mem_stat = MemStat::create_child(
            Some("TEST_CHILD".to_string()),
            0,
            ParentMemStat::Normal(mem_stat.clone()),
        );

        mem_stat.record_memory::<false>(1, 1).unwrap();
        mem_stat.record_memory::<false>(2, 2).unwrap();
        mem_stat.record_memory::<false>(-1, -1).unwrap();

        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 2);
        assert_eq!(child_mem_stat.used.load(Ordering::Relaxed), 0);

        child_mem_stat.record_memory::<false>(1, 1).unwrap();
        child_mem_stat.record_memory::<false>(2, 2).unwrap();
        child_mem_stat.record_memory::<false>(-1, -1).unwrap();

        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 4);
        assert_eq!(child_mem_stat.used.load(Ordering::Relaxed), 2);

        Ok(())
    }

    #[test]
    fn test_multiple_level_mem_stat_with_check_limit() -> Result<()> {
        let mem_stat = MemStat::create("TEST".to_string());
        mem_stat.set_limit(MINIMUM_MEMORY_LIMIT * 2, false);
        let child_mem_stat = MemStat::create_child(
            Some("TEST_CHILD".to_string()),
            0,
            ParentMemStat::Normal(mem_stat.clone()),
        );
        child_mem_stat.set_limit(MINIMUM_MEMORY_LIMIT, false);

        mem_stat.record_memory::<false>(1, 1).unwrap();
        assert!(
            mem_stat
                .record_memory::<false>(MINIMUM_MEMORY_LIMIT, MINIMUM_MEMORY_LIMIT)
                .is_ok()
        );
        assert_eq!(
            mem_stat.used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT
        );
        assert_eq!(child_mem_stat.used.load(Ordering::Relaxed), 0);

        child_mem_stat.record_memory::<false>(1, 1).unwrap();
        assert!(
            child_mem_stat
                .record_memory::<false>(MINIMUM_MEMORY_LIMIT, MINIMUM_MEMORY_LIMIT)
                .is_err()
        );
        assert_eq!(
            mem_stat.used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT + 1 + MINIMUM_MEMORY_LIMIT
        );
        assert_eq!(
            child_mem_stat.used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT
        );

        // parent failure
        let mem_stat = MemStat::create("TEST".to_string());
        mem_stat.set_limit(MINIMUM_MEMORY_LIMIT, false);
        let child_mem_stat = MemStat::create_child(
            Some("TEST_CHILD".to_string()),
            0,
            ParentMemStat::Normal(mem_stat.clone()),
        );
        child_mem_stat.set_limit(MINIMUM_MEMORY_LIMIT * 2, false);

        assert!(
            child_mem_stat
                .record_memory::<true>(1 + MINIMUM_MEMORY_LIMIT, 1 + MINIMUM_MEMORY_LIMIT)
                .is_err()
        );
        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
        assert_eq!(child_mem_stat.used.load(Ordering::Relaxed), 0);

        // child failure
        let mem_stat = MemStat::create("TEST".to_string());
        mem_stat.set_limit(MINIMUM_MEMORY_LIMIT * 2, false);
        let child_mem_stat = MemStat::create_child(
            Some("TEST_CHILD".to_string()),
            0,
            ParentMemStat::Normal(mem_stat.clone()),
        );
        child_mem_stat.set_limit(MINIMUM_MEMORY_LIMIT, false);

        assert!(
            child_mem_stat
                .record_memory::<true>(1 + MINIMUM_MEMORY_LIMIT, 1 + MINIMUM_MEMORY_LIMIT)
                .is_err()
        );
        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
        // assert_eq!(mem_stat.peak_used.load(Ordering::Relaxed), 0);
        assert_eq!(child_mem_stat.used.load(Ordering::Relaxed), 0);

        Ok(())
    }
}
