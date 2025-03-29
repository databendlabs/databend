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

use std::alloc::AllocError;
use std::ptr::addr_of_mut;
use std::sync::atomic::Ordering;
#[cfg(test)]
use std::sync::Arc;

use crate::runtime::memory::mem_stat::OutOfLimit;
use crate::runtime::memory::MemStat;
use crate::runtime::LimitMemGuard;
use crate::runtime::ThreadTracker;
use crate::runtime::GLOBAL_MEM_STAT;

#[thread_local]
static mut GLOBAL_STAT_BUFFER: GlobalStatBuffer = GlobalStatBuffer::empty(&GLOBAL_MEM_STAT);

pub static MEM_STAT_BUFFER_SIZE: i64 = 4 * 1024 * 1024;

/// Buffering memory allocation stats.
///
/// A StatBuffer buffers stats changes in local variables, and periodically flush them to other storage such as an `Arc<T>` shared by several threads.
#[derive(Clone)]
pub struct GlobalStatBuffer {
    pub(crate) memory_usage: i64,
    // Whether to allow unlimited memory. Alloc memory will not panic if it is true.
    unlimited_flag: bool,
    pub(crate) global_mem_stat: &'static MemStat,
    destroyed_thread_local_macro: bool,
}

impl GlobalStatBuffer {
    pub const fn empty(global_mem_stat: &'static MemStat) -> Self {
        Self {
            memory_usage: 0,
            global_mem_stat,
            unlimited_flag: false,
            destroyed_thread_local_macro: false,
        }
    }

    pub fn current() -> &'static mut GlobalStatBuffer {
        unsafe { &mut *addr_of_mut!(GLOBAL_STAT_BUFFER) }
    }

    pub fn is_unlimited(&self) -> bool {
        self.unlimited_flag
    }

    pub fn set_unlimited_flag(&mut self, flag: bool) -> bool {
        let old = self.unlimited_flag;
        self.unlimited_flag = flag;
        old
    }

    pub fn incr(&mut self, bs: i64) -> i64 {
        self.memory_usage += bs;
        self.memory_usage
    }

    /// Flush buffered stat to MemStat it belongs to.
    pub fn flush<const ROLLBACK: bool>(
        &mut self,
        alloc: i64,
    ) -> std::result::Result<(), OutOfLimit> {
        match std::mem::take(&mut self.memory_usage) {
            0 => Ok(()),
            usage => self.global_mem_stat.record_memory::<ROLLBACK>(usage, alloc),
        }
    }

    pub fn alloc(&mut self, memory_usage: i64) -> std::result::Result<(), AllocError> {
        // Rust will alloc or dealloc memory after the thread local is destroyed when we using thread_local macro.
        // This is the boundary of thread exit. It may be dangerous to throw mistakes here.
        if self.destroyed_thread_local_macro {
            let used = self
                .global_mem_stat
                .used
                .fetch_add(memory_usage, Ordering::Relaxed);
            self.global_mem_stat
                .peak_used
                .fetch_max(used + memory_usage, Ordering::Relaxed);
            return Ok(());
        }

        match self.incr(memory_usage) <= MEM_STAT_BUFFER_SIZE {
            true => Ok(()),
            false => {
                match !std::thread::panicking() && !self.unlimited_flag {
                    true => {
                        if let Err(out_of_limit) = self.flush::<true>(memory_usage) {
                            let _guard = LimitMemGuard::enter_unlimited();
                            ThreadTracker::replace_error_message(Some(format!(
                                "{:?}",
                                out_of_limit
                            )));
                            return Err(AllocError);
                        }
                    }
                    false => {
                        let _ = self.flush::<false>(0);
                    }
                };

                Ok(())
            }
        }
    }

    pub fn force_alloc(&mut self, memory_usage: i64) {
        if self.destroyed_thread_local_macro {
            let used = self
                .global_mem_stat
                .used
                .fetch_add(memory_usage, Ordering::Relaxed);
            self.global_mem_stat
                .peak_used
                .fetch_max(used + memory_usage, Ordering::Relaxed);
            return;
        }

        if self.incr(memory_usage) > MEM_STAT_BUFFER_SIZE {
            let _ = self.flush::<false>(memory_usage);
        }
    }

    pub fn dealloc(&mut self, memory_usage: i64) {
        // Rust will alloc or dealloc memory after the thread local is destroyed when we using thread_local macro.
        if self.destroyed_thread_local_macro {
            self.global_mem_stat
                .used
                .fetch_add(-memory_usage, Ordering::Relaxed);
            return;
        }

        if self.incr(-memory_usage) < -MEM_STAT_BUFFER_SIZE {
            let _ = self.flush::<false>(memory_usage);
        }

        // NOTE: De-allocation does not panic
        // even when it's possible exceeding the limit
        // due to other threads sharing the same MemStat may have allocated a lot of memory.
    }

    pub fn mark_destroyed(&mut self) {
        let _guard = LimitMemGuard::enter_unlimited();
        let memory_usage = std::mem::take(&mut self.memory_usage);

        // Memory operations during destruction will be recorded to global stat.
        self.destroyed_thread_local_macro = true;
        let _ = self.global_mem_stat.record_memory::<false>(memory_usage, 0);
    }
}

#[cfg(test)]
pub struct MockGuard {
    _mem_stat: Arc<MemStat>,
    old_global_stat_buffer: GlobalStatBuffer,
}

#[cfg(test)]
impl MockGuard {
    pub fn flush(&mut self) -> Result<(), OutOfLimit> {
        GlobalStatBuffer::current().flush::<false>(0)
    }
}

#[cfg(test)]
impl Drop for MockGuard {
    fn drop(&mut self) {
        let _ = self.flush();
        std::mem::swap(
            GlobalStatBuffer::current(),
            &mut self.old_global_stat_buffer,
        );
    }
}

#[cfg(test)]
impl GlobalStatBuffer {
    pub fn mock(mem_stat: Arc<MemStat>) -> MockGuard {
        let mut mock_global_stat_buffer = Self {
            memory_usage: 0,
            global_mem_stat: unsafe { std::mem::transmute::<&_, &'static _>(mem_stat.as_ref()) },
            unlimited_flag: false,
            destroyed_thread_local_macro: false,
        };

        std::mem::swap(GlobalStatBuffer::current(), &mut mock_global_stat_buffer);
        MockGuard {
            _mem_stat: mem_stat,
            old_global_stat_buffer: mock_global_stat_buffer,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use databend_common_exception::Result;

    use crate::runtime::memory::stat_buffer_global::MEM_STAT_BUFFER_SIZE;
    use crate::runtime::memory::GlobalStatBuffer;
    use crate::runtime::memory::MemStat;

    #[test]
    fn test_alloc() -> Result<()> {
        static TEST_MEM_STATE: MemStat = MemStat::global();
        let mut buffer = GlobalStatBuffer::empty(&TEST_MEM_STATE);

        buffer.alloc(1).unwrap();
        assert_eq!(buffer.memory_usage, 1);
        assert_eq!(TEST_MEM_STATE.used.load(Ordering::Relaxed), 0);

        buffer.destroyed_thread_local_macro = true;
        buffer.alloc(2).unwrap();
        assert_eq!(buffer.memory_usage, 1);
        assert_eq!(TEST_MEM_STATE.used.load(Ordering::Relaxed), 2);

        buffer.destroyed_thread_local_macro = false;
        buffer.alloc(MEM_STAT_BUFFER_SIZE).unwrap();
        assert_eq!(buffer.memory_usage, 0);
        assert_eq!(
            TEST_MEM_STATE.used.load(Ordering::Relaxed),
            MEM_STAT_BUFFER_SIZE + 1 + 2
        );

        Ok(())
    }

    #[test]
    fn test_dealloc() -> Result<()> {
        static TEST_MEM_STATE: MemStat = MemStat::global();
        let mut buffer = GlobalStatBuffer::empty(&TEST_MEM_STATE);

        buffer.dealloc(1);
        assert_eq!(buffer.memory_usage, -1);
        assert_eq!(TEST_MEM_STATE.used.load(Ordering::Relaxed), 0);

        buffer.destroyed_thread_local_macro = true;
        buffer.dealloc(2);
        assert_eq!(buffer.memory_usage, -1);
        assert_eq!(TEST_MEM_STATE.used.load(Ordering::Relaxed), -2);

        buffer.destroyed_thread_local_macro = false;
        buffer.dealloc(MEM_STAT_BUFFER_SIZE);
        assert_eq!(buffer.memory_usage, 0);
        assert_eq!(
            TEST_MEM_STATE.used.load(Ordering::Relaxed),
            -(MEM_STAT_BUFFER_SIZE + 1 + 2)
        );

        Ok(())
    }

    #[test]
    fn test_mark_destroyed() -> Result<()> {
        static TEST_MEM_STATE: MemStat = MemStat::global();

        let mut buffer = GlobalStatBuffer::empty(&TEST_MEM_STATE);

        assert!(!buffer.destroyed_thread_local_macro);
        buffer.alloc(1).unwrap();
        assert_eq!(TEST_MEM_STATE.used.load(Ordering::Relaxed), 0);
        buffer.mark_destroyed();
        assert!(buffer.destroyed_thread_local_macro);
        assert_eq!(TEST_MEM_STATE.used.load(Ordering::Relaxed), 1);

        Ok(())
    }
}
