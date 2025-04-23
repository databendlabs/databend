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
use std::sync::atomic::AtomicI128;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use bytesize::ByteSize;

use crate::base::GlobalSequence;
use crate::runtime::memory::memory_manager::QueriesMemoryManager;
use crate::runtime::memory::memory_manager::GLOBAL_QUERIES_MANAGER;
use crate::runtime::LimitMemGuard;

/// The program mem stat
///
/// Every alloc/dealloc stat will be fed to this mem stat.
pub static GLOBAL_MEM_STAT: MemStat = MemStat::global(&GLOBAL_QUERIES_MANAGER);
const MINIMUM_MEMORY_LIMIT: i64 = 256 * 1024 * 1024;

/// Memory allocation stat.
///
/// - A MemStat have child MemStat.
/// - Every stat that is fed to a child is also fed to its parent.
/// - A MemStat has at most one parent.
pub struct MemStat {
    pub id: usize,
    #[allow(unused)]
    pub name: Option<String>,

    pub(crate) used: AtomicI64,
    pub(crate) peak_used: AtomicI64,
    pub exceeded_mutex: Mutex<bool>,

    /// The limit of max used memory for this tracker.
    ///
    /// Set to 0 to disable the limit.
    limit: AtomicI64,
    water_height: AtomicI128,

    // 0: Memory limit not exceeded.
    // 1. Memory limit exceeded, but no error is reported
    // 2. Memory limit exceeded, requires error reporting
    // 3. Memory limit exceeded, reported error.
    pub(crate) exceeded_limit_state: AtomicUsize,

    resource_tag: Option<String>,
    queries_memory_manager: &'static QueriesMemoryManager,

    #[allow(dead_code)]
    priority: usize,

    pub(crate) parent_memory_stat: Option<Arc<MemStat>>,
}

pub const MEMORY_LIMIT_NOT_EXCEEDED: usize = 0;
pub const MEMORY_LIMIT_EXCEEDED_NO_ERROR: usize = 1;
pub const MEMORY_LIMIT_EXCEEDED_REPORTING_ERROR: usize = 2;
pub const MEMORY_LIMIT_EXCEEDED_REPORTED_ERROR: usize = 3;
pub const MEMORY_LIMIT_RELEASED_MEMORY: usize = 4;

impl MemStat {
    pub const fn global(queries_memory_manager: &'static QueriesMemoryManager) -> Self {
        Self {
            id: 0,
            name: None,
            queries_memory_manager,
            used: AtomicI64::new(0),
            peak_used: AtomicI64::new(0),
            water_height: AtomicI128::new(0),
            parent_memory_stat: None,
            exceeded_limit_state: AtomicUsize::new(MEMORY_LIMIT_NOT_EXCEEDED),
            priority: 0,
            exceeded_mutex: Mutex::new(false),
            limit: AtomicI64::new(0),
            resource_tag: None,
        }
    }

    pub fn create(name: String) -> Arc<MemStat> {
        MemStat::create_child(name, None, 0, None)
    }

    pub fn create_with_resource_tag(name: String, tag: String) -> Arc<MemStat> {
        MemStat::create_child(name, None, 0, Some(tag))
    }

    pub fn create_child(
        name: String,
        parent_memory_stat: Option<Arc<MemStat>>,
        priority: usize,
        resource_tag: Option<String>,
    ) -> Arc<MemStat> {
        let id = match GlobalSequence::next() {
            0 => GlobalSequence::next(),
            id => id,
        };

        Arc::new(MemStat {
            id,
            priority,
            parent_memory_stat,
            name: Some(name),
            used: AtomicI64::new(0),
            peak_used: AtomicI64::new(0),
            water_height: AtomicI128::new(0),
            exceeded_mutex: Mutex::new(false),
            queries_memory_manager: &GLOBAL_QUERIES_MANAGER,
            exceeded_limit_state: AtomicUsize::new(MEMORY_LIMIT_NOT_EXCEEDED),
            limit: AtomicI64::new(0),
            resource_tag: resource_tag,
        })
    }

    pub fn get_limit(&self) -> i64 {
        let limit = self.water_height.load(Ordering::Relaxed);
        Self::split_i128_to_i64s(limit).1
    }

    pub fn set_limit(&self, mut size: i64, allow_exceeded: bool) -> i64 {
        // It may cause the process unable to run if memory limit is too low.
        if size > 0 && size < MINIMUM_MEMORY_LIMIT {
            size = MINIMUM_MEMORY_LIMIT;
        }

        if allow_exceeded && size > MINIMUM_MEMORY_LIMIT {
            let red_line = size / 10 * 7;

            let old_value = self.water_height.swap(
                (std::cmp::max(red_line, MINIMUM_MEMORY_LIMIT) as i128) << 64
                    | (size as u64) as i128,
                Ordering::Relaxed,
            );

            return Self::split_i128_to_i64s(old_value).1;
        }

        let mut expected = 0;
        let mut desired = size as i128;
        while let Err(new_expected) = self.water_height.compare_exchange_weak(
            expected,
            desired,
            Ordering::SeqCst,
            Ordering::Relaxed,
        ) {
            expected = new_expected;
            if allow_exceeded {
                desired = (new_expected >> 64) << 64 | desired;
            }
        }

        Self::split_i128_to_i64s(expected).1
    }

    /// Feed memory usage stat to MemStat and return if it exceeds the limit.
    ///
    /// It feeds `state` to the this tracker and all of its ancestors, including GLOBAL_TRACKER.
    #[inline]
    pub fn record_memory<const NEED_ROLLBACK: bool>(
        &self,
        batch_memory_used: i64,
        current_memory_alloc: i64,
        root_cause: Option<OutOfLimit>,
    ) -> Result<(), OutOfLimit> {
        if let Err(cause) = self.record_memory_impl::<NEED_ROLLBACK>(
            batch_memory_used,
            current_memory_alloc,
            self.resource_tag.as_ref(),
            root_cause,
        ) {
            if cause.allow_exceeded_limit {
                return Err(cause);
            }
        }

        Ok(())
    }

    #[inline]
    pub fn record_memory_impl<const NEED_ROLLBACK: bool>(
        &self,
        batch_memory_used: i64,
        current_memory_alloc: i64,
        tag: Option<&String>,
        root_cause: Option<OutOfLimit>,
    ) -> Result<(), OutOfLimit> {
        let mut used = self.used.fetch_add(batch_memory_used, Ordering::Relaxed);

        used += batch_memory_used;
        self.peak_used.fetch_max(used, Ordering::Relaxed);

        let tag = match self.resource_tag.as_ref() {
            None => tag,
            Some(v) => Some(v),
        };

        let parent_error = match self.parent_memory_stat.as_ref() {
            None => root_cause,
            Some(parent_memory_stat) => parent_memory_stat
                .record_memory_impl::<false>(batch_memory_used, 0, tag, root_cause)
                .err(),
        };

        if let Some(cause) = parent_error {
            // neighbor may exceeded limit, wait release memory
            if batch_memory_used > 0
                && let Err(cause) = self.wait_memory(tag, cause)
            {
                if NEED_ROLLBACK {
                    // We only roll back the memory that alloc failed
                    self.rollback(current_memory_alloc);
                }

                return Err(cause);
            }
        }

        if let Err(cause) = self.check_limit(used, tag) {
            // parent has memory free, try exceeding limit.
            if let Err(cause) = self.exceeding_limit(used, tag, cause) {
                if NEED_ROLLBACK {
                    // NOTE: we cannot rollback peak_used of parent mem stat in this case
                    // self.peak_used.store(peak_used, Ordering::Relaxed);
                    self.rollback(current_memory_alloc);
                }

                return Err(cause);
            }
        }

        Ok(())
    }

    pub fn wait_memory(&self, tag: Option<&String>, oom: OutOfLimit) -> Result<(), OutOfLimit> {
        if std::thread::panicking() || LimitMemGuard::is_unlimited() {
            return Ok(());
        }

        let _guard = LimitMemGuard::enter_unlimited();

        let (water_line, _) = Self::split_i128_to_i64s(self.water_height.load(Ordering::Relaxed));

        if water_line != 0 {
            if let Err(mut cause) = self.queries_memory_manager.wait_memory(self, tag, oom) {
                cause.allow_exceeded_limit = true;
                return Err(cause);
            }

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

        // first exceeded limit.
        if !*exceeded_mutex {
            *exceeded_mutex = true;

            let new_limit = match self.parent_memory_stat.as_ref() {
                None => GLOBAL_MEM_STAT.get_limit(),
                Some(parent) => parent.get_limit(),
            };

            self.set_limit(new_limit, false);
            self.queries_memory_manager
                .request_exceeded_memory(self, tag);
        }

        // recheck limit
        self.check_limit(used, tag)
    }

    pub fn rollback(&self, memory_usage: i64) {
        self.used.fetch_sub(memory_usage, Ordering::Relaxed);

        if let Some(parent_memory_stat) = &self.parent_memory_stat {
            parent_memory_stat.rollback(memory_usage);
        }
    }

    fn split_i128_to_i64s(value: i128) -> (i64, i64) {
        let high = (value >> 64) as i64;
        let low = value as i64;
        (high, low)
    }

    /// Check if used memory is out of the limit.
    #[inline]
    fn check_limit(&self, used: i64, tag: Option<&String>) -> Result<(), OutOfLimit> {
        let (water_weight, limit) =
            Self::split_i128_to_i64s(self.water_height.load(Ordering::Relaxed));

        if limit != 0 && used > limit {
            let mut out_of_limit = OutOfLimit::new(used, limit);
            out_of_limit.allow_exceeded_limit = water_weight != 0;
            return Err(out_of_limit);
        }

        if water_weight != 0 && used <= water_weight {
            let excepted_limit = self.limit.load(Ordering::Relaxed);

            if excepted_limit == 0 || excepted_limit != limit {
                // TODO: exceeded memory
                let mutex = self.exceeded_mutex.lock();
                let mut mutex = mutex.unwrap_or_else(PoisonError::into_inner);

                if *mutex {
                    *mutex = false;
                    self.set_limit(excepted_limit, false);
                    self.queries_memory_manager.release_memory(self, tag);
                }
            }
        }

        Ok(())
    }

    pub(crate) fn recheck_limit(&self) -> Result<(), OutOfLimit> {
        self.check_limit(self.used.load(Ordering::Relaxed), None)
    }

    #[inline]
    pub fn get_memory_usage(&self) -> usize {
        std::cmp::max(self.used.load(Ordering::Relaxed), 0) as usize
    }

    #[inline]
    pub fn get_peak_memory_usage(&self) -> i64 {
        self.peak_used.load(Ordering::Relaxed)
    }

    pub fn release_memory(&self) {
        // if self
        // let exceeded_limit_mode = self
        //     .exceeded_limit_state
        //     .get_or_init(|| Arc::new(AtomicUsize::new(0)));

        // if self.allow_exceeded_limit.load(Ordering::Relaxed)
        //     && exceeded_limit_mode.load(Ordering::SeqCst) != MEMORY_LIMIT_NOT_EXCEEDED
        // {
        //     self.queries_memory_manager.release_memory(self);
        // }
    }
}

impl Drop for MemStat {
    fn drop(&mut self) {
        self.release_memory();
    }
}

/// Error of exceeding limit.
#[derive(Clone)]
pub struct OutOfLimit<V = i64> {
    pub value: V,
    pub limit: V,
    pub allow_exceeded_limit: bool,
}

impl<V> OutOfLimit<V> {
    pub const fn new(value: V, limit: V) -> Self {
        Self {
            value,
            limit,
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use databend_common_exception::Result;

    use crate::runtime::memory::mem_stat::MINIMUM_MEMORY_LIMIT;
    use crate::runtime::MemStat;

    #[test]
    fn test_single_level_mem_stat() -> Result<()> {
        let mem_stat = MemStat::create("TEST".to_string());

        mem_stat.record_memory::<false>(1, 1, None).unwrap();
        mem_stat.record_memory::<false>(2, 2, None).unwrap();
        mem_stat.record_memory::<false>(-1, -1, None).unwrap();

        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 2);

        Ok(())
    }

    #[test]
    fn test_single_level_mem_stat_with_check_limit() -> Result<()> {
        let mem_stat = MemStat::create("TEST".to_string());
        mem_stat.set_limit(MINIMUM_MEMORY_LIMIT, false);

        mem_stat.record_memory::<false>(1, 1, None).unwrap();
        assert!(mem_stat
            .record_memory::<false>(MINIMUM_MEMORY_LIMIT, MINIMUM_MEMORY_LIMIT, None)
            .is_err());
        assert_eq!(
            mem_stat.used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT
        );

        assert!(mem_stat.record_memory::<false>(1, 1, None).is_err());
        assert_eq!(
            mem_stat.used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT + 1
        );

        assert!(mem_stat.record_memory::<true>(1, 1, None).is_err());
        assert_eq!(
            mem_stat.used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT + 1
        );

        assert!(mem_stat.record_memory::<true>(-1, -1, None).is_err());
        assert_eq!(
            mem_stat.used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT + 1
        );

        assert!(mem_stat.record_memory::<false>(-1, -1, None).is_err());
        assert_eq!(
            mem_stat.used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT
        );

        Ok(())
    }

    #[test]
    fn test_multiple_level_mem_stat() -> Result<()> {
        let mem_stat = MemStat::create("TEST".to_string());
        let child_mem_stat =
            MemStat::create_child("TEST_CHILD".to_string(), Some(mem_stat.clone()), 0, None);

        mem_stat.record_memory::<false>(1, 1, None).unwrap();
        mem_stat.record_memory::<false>(2, 2, None).unwrap();
        mem_stat.record_memory::<false>(-1, -1, None).unwrap();

        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 2);
        assert_eq!(child_mem_stat.used.load(Ordering::Relaxed), 0);

        child_mem_stat.record_memory::<false>(1, 1, None).unwrap();
        child_mem_stat.record_memory::<false>(2, 2, None).unwrap();
        child_mem_stat.record_memory::<false>(-1, -1, None).unwrap();

        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 4);
        assert_eq!(child_mem_stat.used.load(Ordering::Relaxed), 2);

        Ok(())
    }

    #[test]
    fn test_multiple_level_mem_stat_with_check_limit() -> Result<()> {
        let mem_stat = MemStat::create("TEST".to_string());
        mem_stat.set_limit(MINIMUM_MEMORY_LIMIT * 2, false);
        let child_mem_stat =
            MemStat::create_child("TEST_CHILD".to_string(), Some(mem_stat.clone()), 0, None);
        child_mem_stat.set_limit(MINIMUM_MEMORY_LIMIT, false);

        mem_stat.record_memory::<false>(1, 1, None).unwrap();
        assert!(mem_stat
            .record_memory::<false>(MINIMUM_MEMORY_LIMIT, MINIMUM_MEMORY_LIMIT, None)
            .is_ok());
        assert_eq!(
            mem_stat.used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT
        );
        assert_eq!(child_mem_stat.used.load(Ordering::Relaxed), 0);

        child_mem_stat.record_memory::<false>(1, 1, None).unwrap();
        assert!(child_mem_stat
            .record_memory::<false>(MINIMUM_MEMORY_LIMIT, MINIMUM_MEMORY_LIMIT, None)
            .is_err());
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
        let child_mem_stat =
            MemStat::create_child("TEST_CHILD".to_string(), Some(mem_stat.clone()), 0, None);
        child_mem_stat.set_limit(MINIMUM_MEMORY_LIMIT * 2, false);

        assert!(child_mem_stat
            .record_memory::<true>(1 + MINIMUM_MEMORY_LIMIT, 1 + MINIMUM_MEMORY_LIMIT, None)
            .is_err());
        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
        assert_eq!(child_mem_stat.used.load(Ordering::Relaxed), 0);

        // child failure
        let mem_stat = MemStat::create("TEST".to_string());
        mem_stat.set_limit(MINIMUM_MEMORY_LIMIT * 2, false);
        let child_mem_stat =
            MemStat::create_child("TEST_CHILD".to_string(), Some(mem_stat.clone()), 0, None);
        child_mem_stat.set_limit(MINIMUM_MEMORY_LIMIT, false);

        assert!(child_mem_stat
            .record_memory::<true>(1 + MINIMUM_MEMORY_LIMIT, 1 + MINIMUM_MEMORY_LIMIT, None)
            .is_err());
        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
        // assert_eq!(mem_stat.peak_used.load(Ordering::Relaxed), 0);
        assert_eq!(child_mem_stat.used.load(Ordering::Relaxed), 0);

        Ok(())
    }
}
