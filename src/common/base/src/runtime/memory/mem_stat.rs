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
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::OnceLock;

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
    name: Option<String>,

    pub(crate) used: AtomicI64,
    pub(crate) peak_used: AtomicI64,

    /// The limit of max used memory for this tracker.
    ///
    /// Set to 0 to disable the limit.
    limit: AtomicI64,
    allow_exceeded_limit: bool,
    // 0: Memory limit not exceeded.
    // 1. Memory limit exceeded, but no error is reported
    // 2. Memory limit exceeded, requires error reporting
    // 3. Memory limit exceeded, reported error.
    exceeded_limit_mode: OnceLock<Arc<AtomicUsize>>,

    queries_memory_manager: &'static QueriesMemoryManager,

    priority: usize,

    parent_memory_stat: Option<Arc<MemStat>>,
}

const MEMORY_LIMIT_NOT_EXCEEDED: usize = 0;
const MEMORY_LIMIT_EXCEEDED_NO_ERROR: usize = 1;
const MEMORY_LIMIT_EXCEEDED_REPORTING_ERROR: usize = 2;
const MEMORY_LIMIT_EXCEEDED_REPORTED_ERROR: usize = 3;

impl MemStat {
    pub const fn global(queries_memory_manager: &'static QueriesMemoryManager) -> Self {
        Self {
            id: 0,
            name: None,
            queries_memory_manager,
            used: AtomicI64::new(0),
            peak_used: AtomicI64::new(0),
            limit: AtomicI64::new(0),
            parent_memory_stat: None,
            allow_exceeded_limit: false,
            exceeded_limit_mode: OnceLock::new(),
            priority: 0,
        }
    }

    pub fn create(name: String) -> Arc<MemStat> {
        MemStat::create_child(name, None)
    }

    pub fn create_child(name: String, parent_memory_stat: Option<Arc<MemStat>>) -> Arc<MemStat> {
        let id = match GlobalSequence::next() {
            0 => GlobalSequence::next(),
            id => id,
        };

        Arc::new(MemStat {
            id,
            name: Some(name),
            used: AtomicI64::new(0),
            peak_used: AtomicI64::new(0),
            limit: AtomicI64::new(0),
            parent_memory_stat,
            allow_exceeded_limit: false,
            exceeded_limit_mode: OnceLock::new(),
            queries_memory_manager: &GLOBAL_QUERIES_MANAGER,
            priority: 0,
        })
    }

    pub fn set_limit(&self, mut size: i64) {
        // It may cause the process unable to run if memory limit is too low.
        if size > 0 && size < MINIMUM_MEMORY_LIMIT {
            size = MINIMUM_MEMORY_LIMIT;
        }

        self.limit.store(size, Ordering::Relaxed);
    }

    /// Feed memory usage stat to MemStat and return if it exceeds the limit.
    ///
    /// It feeds `state` to the this tracker and all of its ancestors, including GLOBAL_TRACKER.
    #[inline]
    pub fn record_memory<const NEED_ROLLBACK: bool>(
        &self,
        batch_memory_used: i64,
        current_memory_alloc: i64,
    ) -> Result<(), OutOfLimit> {
        let mut used = self.used.fetch_add(batch_memory_used, Ordering::Relaxed);

        used += batch_memory_used;
        self.peak_used.fetch_max(used, Ordering::Relaxed);

        if let Some(parent_memory_stat) = self.parent_memory_stat.as_ref() {
            if let Err(cause) = parent_memory_stat.record_memory::<false>(batch_memory_used, 0) {
                if let Err(_cause) = self.check_limit(used) {
                    if NEED_ROLLBACK {
                        // We only roll back the memory that alloc failed
                        self.rollback(current_memory_alloc);
                    }

                    return Err(cause);
                }

                // neighbor may exceeded limit, wait release memory
                return self.try_wait_memory(cause);
            }
        }

        if let Err(cause) = self.check_limit(used) {
            // parent has memory free, try exceeding limit.
            if self.allow_exceeded_limit {
                return self.try_exceeding_limit(cause);
            }

            if NEED_ROLLBACK {
                // NOTE: we cannot rollback peak_used of parent mem stat in this case
                // self.peak_used.store(peak_used, Ordering::Relaxed);
                self.rollback(current_memory_alloc);
            }

            return Err(cause);
        }

        Ok(())
    }

    fn try_wait_memory(&self, out_of_limit: OutOfLimit) -> Result<(), OutOfLimit> {
        self.queries_memory_manager
            .wait_release_memory(self.id, out_of_limit)
    }

    pub fn try_exceeding_limit(&self, out_of_limit: OutOfLimit) -> Result<(), OutOfLimit> {
        if !self.allow_exceeded_limit {
            return Err(out_of_limit);
        }

        let _guard = LimitMemGuard::enter_unlimited();
        let exceeded_limit_mode = self
            .exceeded_limit_mode
            .get_or_init(|| Arc::new(AtomicUsize::new(0)));

        let mode = exceeded_limit_mode.load(Ordering::SeqCst);

        // Memory limit exceeded, but no error is reported
        if mode == MEMORY_LIMIT_EXCEEDED_NO_ERROR {
            return Ok(());
        }

        //
        if mode == MEMORY_LIMIT_NOT_EXCEEDED {
            let fetch_value = exceeded_limit_mode.compare_exchange(
                MEMORY_LIMIT_NOT_EXCEEDED,
                MEMORY_LIMIT_EXCEEDED_NO_ERROR,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );

            let fetch_value = fetch_value.unwrap_or_else(|x| x);

            if fetch_value == MEMORY_LIMIT_NOT_EXCEEDED {
                self.queries_memory_manager.request_exceeded_memory(
                    self.id,
                    self.priority,
                    exceeded_limit_mode.clone(),
                );

                return Ok(());
            } else if fetch_value == MEMORY_LIMIT_EXCEEDED_NO_ERROR {
                // exceeded limit is safely
                return Ok(());
            }
        }

        // notify release memory usage
        if mode == MEMORY_LIMIT_EXCEEDED_REPORTING_ERROR {
            let _fetch_value = exceeded_limit_mode.compare_exchange(
                MEMORY_LIMIT_EXCEEDED_REPORTING_ERROR,
                MEMORY_LIMIT_EXCEEDED_REPORTED_ERROR,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );

            // if fetch_value.unwrap_or_else(|x| x) == MEMORY_LIMIT_EXCEEDED_REPORTING_ERROR {
            //     let release_memory = std::cmp::max(0, out_of_limit.value - out_of_limit.limit);
            //     self.queries_memory_manager
            //         .release_memory(release_memory as u64);
            // }
        }

        Err(out_of_limit)
    }

    pub fn rollback(&self, memory_usage: i64) {
        self.used.fetch_sub(memory_usage, Ordering::Relaxed);

        if let Some(parent_memory_stat) = &self.parent_memory_stat {
            parent_memory_stat.rollback(memory_usage);
        }
    }

    /// Check if used memory is out of the limit.
    #[inline]
    fn check_limit(&self, used: i64) -> Result<(), OutOfLimit> {
        let limit = self.limit.load(Ordering::Relaxed);

        // No limit
        if limit == 0 {
            return Ok(());
        }

        if used <= limit {
            return Ok(());
        }

        Err(OutOfLimit::new(used, limit))
    }

    #[inline]
    pub fn get_memory_usage(&self) -> usize {
        std::cmp::max(self.used.load(Ordering::Relaxed), 0) as usize
    }

    #[inline]
    pub fn get_peek_memory_usage(&self) -> i64 {
        self.peak_used.load(Ordering::Relaxed)
    }
}

impl Drop for MemStat {
    fn drop(&mut self) {
        if self.allow_exceeded_limit {
            self.queries_memory_manager.release_memory(self.id);
        }
    }
}

/// Error of exceeding limit.
#[derive(Clone)]
pub struct OutOfLimit<V = i64> {
    pub value: V,
    pub limit: V,
}

impl<V> OutOfLimit<V> {
    pub const fn new(value: V, limit: V) -> Self {
        Self { value, limit }
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

        mem_stat.record_memory::<false>(1, 1).unwrap();
        mem_stat.record_memory::<false>(2, 2).unwrap();
        mem_stat.record_memory::<false>(-1, -1).unwrap();

        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 2);

        Ok(())
    }

    #[test]
    fn test_single_level_mem_stat_with_check_limit() -> Result<()> {
        let mem_stat = MemStat::create("TEST".to_string());
        mem_stat.set_limit(MINIMUM_MEMORY_LIMIT);

        mem_stat.record_memory::<false>(1, 1).unwrap();
        assert!(mem_stat
            .record_memory::<false>(MINIMUM_MEMORY_LIMIT, MINIMUM_MEMORY_LIMIT)
            .is_err());
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
        let child_mem_stat =
            MemStat::create_child("TEST_CHILD".to_string(), Some(mem_stat.clone()));

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
        mem_stat.set_limit(MINIMUM_MEMORY_LIMIT * 2);
        let child_mem_stat =
            MemStat::create_child("TEST_CHILD".to_string(), Some(mem_stat.clone()));
        child_mem_stat.set_limit(MINIMUM_MEMORY_LIMIT);

        mem_stat.record_memory::<false>(1, 1).unwrap();
        assert!(mem_stat
            .record_memory::<false>(MINIMUM_MEMORY_LIMIT, MINIMUM_MEMORY_LIMIT)
            .is_ok());
        assert_eq!(
            mem_stat.used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT
        );
        assert_eq!(child_mem_stat.used.load(Ordering::Relaxed), 0);

        child_mem_stat.record_memory::<false>(1, 1).unwrap();
        assert!(child_mem_stat
            .record_memory::<false>(MINIMUM_MEMORY_LIMIT, MINIMUM_MEMORY_LIMIT)
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
        mem_stat.set_limit(MINIMUM_MEMORY_LIMIT);
        let child_mem_stat =
            MemStat::create_child("TEST_CHILD".to_string(), Some(mem_stat.clone()));
        child_mem_stat.set_limit(MINIMUM_MEMORY_LIMIT * 2);

        assert!(child_mem_stat
            .record_memory::<true>(1 + MINIMUM_MEMORY_LIMIT, 1 + MINIMUM_MEMORY_LIMIT)
            .is_err());
        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
        assert_eq!(child_mem_stat.used.load(Ordering::Relaxed), 0);

        // child failure
        let mem_stat = MemStat::create("TEST".to_string());
        mem_stat.set_limit(MINIMUM_MEMORY_LIMIT * 2);
        let child_mem_stat =
            MemStat::create_child("TEST_CHILD".to_string(), Some(mem_stat.clone()));
        child_mem_stat.set_limit(MINIMUM_MEMORY_LIMIT);

        assert!(child_mem_stat
            .record_memory::<true>(1 + MINIMUM_MEMORY_LIMIT, 1 + MINIMUM_MEMORY_LIMIT)
            .is_err());
        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
        // assert_eq!(mem_stat.peak_used.load(Ordering::Relaxed), 0);
        assert_eq!(child_mem_stat.used.load(Ordering::Relaxed), 0);

        Ok(())
    }
}
