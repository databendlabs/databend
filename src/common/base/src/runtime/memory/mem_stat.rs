use std::cell::RefCell;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytesize::ByteSize;
use log::info;

use crate::runtime::runtime_tracker::OutOfLimit;
use crate::runtime::ThreadTracker;
use crate::runtime::GLOBAL_MEM_STAT;

/// Memory allocation stat.
///
/// - A MemStat have child MemStat.
/// - Every stat that is fed to a child is also fed to its parent.
/// - A MemStat has at most one parent.
pub struct MemStat {
    global: bool,

    name: Option<String>,

    pub used: AtomicI64,

    pub peak_used: AtomicI64,

    /// The limit of max used memory for this tracker.
    ///
    /// Set to 0 to disable the limit.
    limit: AtomicI64,

    parent_memory_stat: Option<Arc<MemStat>>,
}

impl MemStat {
    pub const fn global() -> Self {
        Self {
            name: None,
            global: true,
            used: AtomicI64::new(0),
            limit: AtomicI64::new(0),
            peak_used: AtomicI64::new(0),
            parent_memory_stat: None,
        }
    }

    pub fn create(name: String) -> Arc<MemStat> {
        MemStat::create_child(name, None)
    }

    pub fn create_child(name: String, parent_memory_stat: Option<Arc<MemStat>>) -> Arc<MemStat> {
        Arc::new(MemStat {
            global: false,
            name: Some(name),
            used: AtomicI64::new(0),
            limit: AtomicI64::new(0),
            peak_used: AtomicI64::new(0),
            parent_memory_stat,
        })
    }

    pub fn set_limit(&self, mut size: i64) {
        // It may cause the process unable to run if memory limit is too low.
        const LOWEST: i64 = 256 * 1024 * 1024;

        if size > 0 && size < LOWEST {
            size = LOWEST;
        }

        self.limit.store(size, Ordering::Relaxed);
    }

    /// Feed memory usage stat to MemStat and return if it exceeds the limit.
    ///
    /// It feeds `state` to the this tracker and all of its ancestors, including GLOBAL_TRACKER.
    #[inline]
    pub fn record_memory<const NEED_ROLLBACK: bool>(
        &self,
        memory_usage: i64,
    ) -> Result<(), OutOfLimit> {
        let mut used = self.used.fetch_add(memory_usage, Ordering::Relaxed);

        used += memory_usage;
        let peak_used = self.peak_used.fetch_max(used, Ordering::Relaxed);

        if !self.global {
            let parent_memory_stat = self
                .parent_memory_stat
                .as_deref()
                .unwrap_or(&GLOBAL_MEM_STAT);
            if let Err(cause) = parent_memory_stat.record_memory::<NEED_ROLLBACK>(memory_usage) {
                if NEED_ROLLBACK {
                    self.peak_used.store(peak_used, Ordering::Relaxed);
                    self.used.fetch_sub(memory_usage, Ordering::Relaxed);
                }

                return Err(cause);
            }
        }

        if let Err(cause) = self.check_limit(used) {
            if NEED_ROLLBACK {
                self.peak_used.store(peak_used, Ordering::Relaxed);
                self.used.fetch_sub(memory_usage, Ordering::Relaxed);
            }

            return Err(cause);
        }

        Ok(())
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
