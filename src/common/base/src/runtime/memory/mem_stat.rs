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
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytesize::ByteSize;
use log::info;

/// The program mem stat
///
/// Every alloc/dealloc stat will be fed to this mem stat.
pub static GLOBAL_MEM_STAT: MemStat = MemStat::global();

const MINIMUM_MEMORY_LIMIT: i64 = 256 * 1024 * 1024;

/// Memory allocation stat.
///
/// - A MemStat have child MemStat.
/// - Every stat that is fed to a child is also fed to its parent.
/// - A MemStat has at most one parent.
pub struct MemStat {
    name: Option<String>,

    pub(crate) used: AtomicI64,

    pub(crate) peak_used: AtomicI64,

    /// The limit of max used memory for this tracker.
    ///
    /// Set to 0 to disable the limit.
    limit: AtomicI64,

    parent_memory_stat: Vec<Arc<MemStat>>,
}

impl MemStat {
    pub const fn global() -> Self {
        Self {
            name: None,
            used: AtomicI64::new(0),
            limit: AtomicI64::new(0),
            peak_used: AtomicI64::new(0),
            parent_memory_stat: vec![],
        }
    }

    pub fn create(name: String) -> Arc<MemStat> {
        MemStat::create_child(name, vec![])
    }

    pub fn create_child(name: String, parent_memory_stat: Vec<Arc<MemStat>>) -> Arc<MemStat> {
        Arc::new(MemStat {
            name: Some(name),
            used: AtomicI64::new(0),
            limit: AtomicI64::new(0),
            peak_used: AtomicI64::new(0),
            parent_memory_stat,
        })
    }

    pub fn get_parent_memory_stat(&self) -> Vec<Arc<MemStat>> {
        self.parent_memory_stat.clone()
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
        let old_peak_used = self.peak_used.fetch_max(used, Ordering::Relaxed);

        for (idx, parent_memory_stat) in self.parent_memory_stat.iter().enumerate() {
            if let Err(cause) = parent_memory_stat
                .record_memory::<NEED_ROLLBACK>(batch_memory_used, current_memory_alloc)
            {
                if NEED_ROLLBACK {
                    // We only roll back the memory that alloc failed
                    self.used.fetch_sub(current_memory_alloc, Ordering::Relaxed);

                    if used > old_peak_used {
                        self.peak_used
                            .fetch_sub(current_memory_alloc, Ordering::Relaxed);
                    }

                    for index in 0..idx {
                        self.parent_memory_stat[index].rollback(current_memory_alloc);
                    }
                }

                return Err(cause);
            }
        }

        if let Err(cause) = self.check_limit(used) {
            if NEED_ROLLBACK {
                if used > old_peak_used {
                    self.peak_used
                        .fetch_sub(current_memory_alloc, Ordering::Relaxed);
                }

                // NOTE: we cannot rollback peak_used of parent mem stat in this case
                // self.peak_used.store(peak_used, Ordering::Relaxed);
                self.rollback(current_memory_alloc);
            }

            return Err(cause);
        }

        Ok(())
    }

    pub fn rollback(&self, memory_usage: i64) {
        self.used.fetch_sub(memory_usage, Ordering::Relaxed);

        for parent_memory_stat in &self.parent_memory_stat {
            parent_memory_stat.rollback(memory_usage)
        }
    }

    pub fn movein_memory(&self, size: i64) {
        let used = self.used.fetch_add(size, Ordering::Relaxed);
        self.peak_used.fetch_max(used + size, Ordering::Relaxed);
    }

    pub fn moveout_memory(&self, size: i64) {
        self.used.fetch_sub(size, Ordering::Relaxed);
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
    pub fn get_memory_usage(&self) -> i64 {
        self.used.load(Ordering::Relaxed)
    }

    #[inline]
    #[allow(unused)]
    pub fn get_peak_memory_usage(&self) -> i64 {
        self.peak_used.load(Ordering::Relaxed)
    }

    #[allow(unused)]
    pub fn log_memory_usage(&self) {
        let name = self.name.clone().unwrap_or_else(|| String::from("global"));
        let memory_usage = self.used.load(Ordering::Relaxed);
        let memory_usage = std::cmp::max(0, memory_usage) as u64;
        info!(
            "Current memory usage({}): {}.",
            name,
            ByteSize::b(memory_usage)
        );
    }

    #[allow(unused)]
    pub fn log_peek_memory_usage(&self) {
        let name = self.name.clone().unwrap_or_else(|| String::from("global"));
        let peak_memory_usage = self.peak_used.load(Ordering::Relaxed);
        let peak_memory_usage = std::cmp::max(0, peak_memory_usage) as u64;
        info!(
            "Peak memory usage({}): {}.",
            name,
            ByteSize::b(peak_memory_usage)
        );
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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
        assert_eq!(mem_stat.peak_used.load(Ordering::Relaxed), 3);

        Ok(())
    }

    #[test]
    fn test_single_level_mem_stat_with_check_limit() -> Result<()> {
        let mem_stat = MemStat::create("TEST".to_string());
        mem_stat.set_limit(MINIMUM_MEMORY_LIMIT);

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
        assert_eq!(
            mem_stat.peak_used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT
        );

        assert!(mem_stat.record_memory::<false>(1, 1).is_err());
        assert_eq!(
            mem_stat.used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT + 1
        );
        assert_eq!(
            mem_stat.peak_used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT + 1
        );

        assert!(mem_stat.record_memory::<true>(1, 1).is_err());
        assert_eq!(
            mem_stat.used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT + 1
        );
        assert_eq!(
            mem_stat.peak_used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT + 1
        );

        assert!(mem_stat.record_memory::<true>(-1, -1).is_err());
        assert_eq!(
            mem_stat.used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT + 1
        );
        assert_eq!(
            mem_stat.peak_used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT + 1
        );

        assert!(mem_stat.record_memory::<false>(-1, -1).is_err());
        assert_eq!(
            mem_stat.used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT
        );
        assert_eq!(
            mem_stat.peak_used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT + 1
        );

        Ok(())
    }

    #[test]
    fn test_multiple_level_mem_stat() -> Result<()> {
        let mem_stat = MemStat::create("TEST".to_string());
        let child_mem_stat =
            MemStat::create_child("TEST_CHILD".to_string(), vec![mem_stat.clone()]);

        mem_stat.record_memory::<false>(1, 1).unwrap();
        mem_stat.record_memory::<false>(2, 2).unwrap();
        mem_stat.record_memory::<false>(-1, -1).unwrap();

        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 2);
        assert_eq!(mem_stat.peak_used.load(Ordering::Relaxed), 3);
        assert_eq!(child_mem_stat.used.load(Ordering::Relaxed), 0);
        assert_eq!(child_mem_stat.peak_used.load(Ordering::Relaxed), 0);

        child_mem_stat.record_memory::<false>(1, 1).unwrap();
        child_mem_stat.record_memory::<false>(2, 2).unwrap();
        child_mem_stat.record_memory::<false>(-1, -1).unwrap();

        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 4);
        assert_eq!(mem_stat.peak_used.load(Ordering::Relaxed), 5);
        assert_eq!(child_mem_stat.used.load(Ordering::Relaxed), 2);
        assert_eq!(child_mem_stat.peak_used.load(Ordering::Relaxed), 3);

        Ok(())
    }

    #[test]
    fn test_multiple_level_mem_stat_with_check_limit() -> Result<()> {
        let mem_stat = MemStat::create("TEST".to_string());
        mem_stat.set_limit(MINIMUM_MEMORY_LIMIT * 2);
        let child_mem_stat =
            MemStat::create_child("TEST_CHILD".to_string(), vec![mem_stat.clone()]);
        child_mem_stat.set_limit(MINIMUM_MEMORY_LIMIT);

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
        assert_eq!(
            mem_stat.peak_used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT
        );
        assert_eq!(child_mem_stat.used.load(Ordering::Relaxed), 0);
        assert_eq!(child_mem_stat.peak_used.load(Ordering::Relaxed), 0);

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
            mem_stat.peak_used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT + 1 + MINIMUM_MEMORY_LIMIT
        );
        assert_eq!(
            child_mem_stat.used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT
        );
        assert_eq!(
            child_mem_stat.peak_used.load(Ordering::Relaxed),
            1 + MINIMUM_MEMORY_LIMIT
        );

        // parent failure
        let mem_stat = MemStat::create("TEST".to_string());
        mem_stat.set_limit(MINIMUM_MEMORY_LIMIT);
        let child_mem_stat =
            MemStat::create_child("TEST_CHILD".to_string(), vec![mem_stat.clone()]);
        child_mem_stat.set_limit(MINIMUM_MEMORY_LIMIT * 2);

        assert!(
            child_mem_stat
                .record_memory::<true>(1 + MINIMUM_MEMORY_LIMIT, 1 + MINIMUM_MEMORY_LIMIT)
                .is_err()
        );
        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
        assert_eq!(mem_stat.peak_used.load(Ordering::Relaxed), 0);
        assert_eq!(child_mem_stat.used.load(Ordering::Relaxed), 0);
        assert_eq!(child_mem_stat.peak_used.load(Ordering::Relaxed), 0);

        // child failure
        let mem_stat = MemStat::create("TEST".to_string());
        mem_stat.set_limit(MINIMUM_MEMORY_LIMIT * 2);
        let child_mem_stat =
            MemStat::create_child("TEST_CHILD".to_string(), vec![mem_stat.clone()]);
        child_mem_stat.set_limit(MINIMUM_MEMORY_LIMIT);

        assert!(
            child_mem_stat
                .record_memory::<true>(1 + MINIMUM_MEMORY_LIMIT, 1 + MINIMUM_MEMORY_LIMIT)
                .is_err()
        );
        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
        // assert_eq!(mem_stat.peak_used.load(Ordering::Relaxed), 0);
        assert_eq!(child_mem_stat.used.load(Ordering::Relaxed), 0);
        assert_eq!(child_mem_stat.peak_used.load(Ordering::Relaxed), 0);

        Ok(())
    }
}
