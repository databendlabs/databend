use std::ptr::addr_of_mut;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::runtime::memory::stat_buffer_global::MEM_STAT_BUFFER_SIZE;
use crate::runtime::memory::OutOfLimit;
use crate::runtime::LimitMemGuard;
use crate::runtime::MemStat;
use crate::runtime::GLOBAL_MEM_STAT;

#[thread_local]
static mut MEM_STAT_BUFFER: MemStatBuffer = MemStatBuffer::empty(&GLOBAL_MEM_STAT);

pub struct MemStatBuffer {
    cur_mem_stat_id: usize,
    cur_mem_stat: Option<Arc<MemStat>>,
    memory_usage: i64,
    // Whether to allow unlimited memory. Alloc memory will not panic if it is true.
    // unlimited_flag: bool,
    global_mem_stat: &'static MemStat,
    destroyed_thread_local_macro: bool,
}

impl MemStatBuffer {
    pub const fn empty(global_mem_stat: &'static MemStat) -> MemStatBuffer {
        MemStatBuffer {
            global_mem_stat,
            cur_mem_stat_id: 0,
            cur_mem_stat: None,
            memory_usage: 0,
            // unlimited_flag: false,
            destroyed_thread_local_macro: false,
        }
    }

    pub fn current() -> &'static mut MemStatBuffer {
        unsafe { &mut *addr_of_mut!(MEM_STAT_BUFFER) }
    }

    pub fn incr(&mut self, bs: i64) -> i64 {
        self.memory_usage += bs;
        self.memory_usage
    }

    pub fn flush<const FALLBACK: bool>(
        &mut self,
        memory_usage: i64,
        alloc: i64,
    ) -> Result<(), OutOfLimit> {
        if memory_usage == 0 {
            return Ok(());
        }

        self.cur_mem_stat_id = 0;
        if let Some(mem_stat) = self.cur_mem_stat.take() {
            if let Err(cause) = mem_stat.record_memory::<FALLBACK>(memory_usage, alloc) {
                let memory_usage = match FALLBACK {
                    true => memory_usage - alloc,
                    false => memory_usage,
                };

                self.global_mem_stat
                    .record_memory::<false>(memory_usage, 0)?;
                return Err(cause);
            }
        }

        self.global_mem_stat
            .record_memory::<FALLBACK>(memory_usage, alloc)
    }

    pub fn alloc(
        &mut self,
        mem_stat: &Arc<MemStat>,
        mut memory_usage: i64,
    ) -> Result<(), OutOfLimit> {
        if self.destroyed_thread_local_macro {
            mem_stat.used.fetch_add(memory_usage, Ordering::Relaxed);
            return Ok(());
        }

        if mem_stat.id != self.cur_mem_stat_id {
            std::mem::swap(&mut self.memory_usage, &mut memory_usage);
            let flush_res = self.flush::<false>(memory_usage, 0);

            self.cur_mem_stat = Some(mem_stat.clone());
            self.cur_mem_stat_id = mem_stat.id;
            return flush_res;
        }

        if self.incr(memory_usage) >= MEM_STAT_BUFFER_SIZE {
            let alloc = memory_usage;
            let memory_usage = std::mem::take(&mut self.memory_usage);
            self.flush::<true>(memory_usage, alloc)?;
        }

        Ok(())
    }

    pub fn dealloc(&mut self, mem_stat: &Arc<MemStat>, memory_usage: i64) {
        let mut memory_usage = -memory_usage;

        if self.destroyed_thread_local_macro {
            mem_stat.used.fetch_add(memory_usage, Ordering::Relaxed);
            return;
        }

        debug_assert_eq!(Arc::weak_count(mem_stat), 0);

        if mem_stat.id != self.cur_mem_stat_id {
            if Arc::strong_count(mem_stat) == 1 {
                mem_stat.used.fetch_add(memory_usage, Ordering::Relaxed);
                self.global_mem_stat
                    .used
                    .fetch_add(memory_usage, Ordering::Relaxed);
                return;
            }

            std::mem::swap(&mut self.memory_usage, &mut memory_usage);
            let _ = self.flush::<false>(memory_usage, 0);

            self.cur_mem_stat = Some(mem_stat.clone());
            self.cur_mem_stat_id = mem_stat.id;
            return;
        }

        if self.incr(memory_usage) <= -MEM_STAT_BUFFER_SIZE || Arc::strong_count(mem_stat) == 1 {
            let alloc = memory_usage;
            let memory_usage = std::mem::take(&mut self.memory_usage);
            let _ = self.flush::<false>(memory_usage, alloc);
        }

        // NOTE: De-allocation does not panic
        // even when it's possible exceeding the limit
        // due to other threads sharing the same MemStat may have allocated a lot of memory.
    }

    pub fn mark_destroyed(&mut self) {
        let _guard = LimitMemGuard::enter_unlimited();
        let memory_usage = std::mem::take(&mut self.memory_usage);

        self.destroyed_thread_local_macro = true;
        let _ = self.flush::<false>(memory_usage, 0);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use crate::runtime::memory::stat_buffer_global::MEM_STAT_BUFFER_SIZE;
    use crate::runtime::memory::stat_buffer_mem_stat::MemStatBuffer;
    use crate::runtime::memory::OutOfLimit;
    use crate::runtime::MemStat;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_alloc_with_same_allocator() -> Result<(), OutOfLimit> {
        static TEST_GLOBAL: MemStat = MemStat::global();

        let mut buffer = MemStatBuffer::empty(&TEST_GLOBAL);

        let mem_stat = MemStat::create(String::from("test"));
        buffer.alloc(&mem_stat, 1)?;
        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
        assert_eq!(TEST_GLOBAL.used.load(Ordering::Relaxed), 0);

        buffer.alloc(&mem_stat, MEM_STAT_BUFFER_SIZE - 2)?;
        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
        assert_eq!(TEST_GLOBAL.used.load(Ordering::Relaxed), 0);

        buffer.alloc(&mem_stat, 1)?;
        assert_eq!(mem_stat.used.load(Ordering::Relaxed), MEM_STAT_BUFFER_SIZE);
        assert_eq!(
            TEST_GLOBAL.used.load(Ordering::Relaxed),
            MEM_STAT_BUFFER_SIZE
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_alloc_with_diff_allocator() -> Result<(), OutOfLimit> {
        static TEST_GLOBAL: MemStat = MemStat::global();

        let mut buffer = MemStatBuffer::empty(&TEST_GLOBAL);

        let mem_stat_1 = MemStat::create(String::from("test"));
        let mem_stat_2 = MemStat::create(String::from("test"));
        buffer.alloc(&mem_stat_1, 1)?;
        assert_eq!(mem_stat_1.used.load(Ordering::Relaxed), 0);
        assert_eq!(mem_stat_2.used.load(Ordering::Relaxed), 0);
        assert_eq!(TEST_GLOBAL.used.load(Ordering::Relaxed), 0);
        buffer.alloc(&mem_stat_2, 1)?;
        assert_eq!(mem_stat_1.used.load(Ordering::Relaxed), 1);
        assert_eq!(mem_stat_2.used.load(Ordering::Relaxed), 0);
        assert_eq!(TEST_GLOBAL.used.load(Ordering::Relaxed), 1);

        buffer.alloc(&mem_stat_1, 1)?;
        assert_eq!(mem_stat_1.used.load(Ordering::Relaxed), 1);
        assert_eq!(mem_stat_2.used.load(Ordering::Relaxed), 1);
        assert_eq!(TEST_GLOBAL.used.load(Ordering::Relaxed), 2);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_dealloc_with_same_allocator() -> Result<(), OutOfLimit> {
        static TEST_GLOBAL: MemStat = MemStat::global();

        let mut buffer = MemStatBuffer::empty(&TEST_GLOBAL);

        let mem_stat = MemStat::create(String::from("test"));
        let _shared = mem_stat.clone();

        buffer.dealloc(&mem_stat, 1);
        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
        assert_eq!(TEST_GLOBAL.used.load(Ordering::Relaxed), 0);

        buffer.dealloc(&mem_stat, MEM_STAT_BUFFER_SIZE - 2);
        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
        assert_eq!(TEST_GLOBAL.used.load(Ordering::Relaxed), 0);

        buffer.dealloc(&mem_stat, 1);
        assert_eq!(mem_stat.used.load(Ordering::Relaxed), -MEM_STAT_BUFFER_SIZE);
        assert_eq!(
            TEST_GLOBAL.used.load(Ordering::Relaxed),
            -MEM_STAT_BUFFER_SIZE
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_dealloc_with_diff_allocator() -> Result<(), OutOfLimit> {
        static TEST_GLOBAL: MemStat = MemStat::global();

        let mut buffer = MemStatBuffer::empty(&TEST_GLOBAL);

        let mem_stat_1 = MemStat::create(String::from("test"));
        let mem_stat_2 = MemStat::create(String::from("test"));
        let _shared = (mem_stat_1.clone(), mem_stat_2.clone());

        buffer.dealloc(&mem_stat_1, 1);
        assert_eq!(mem_stat_1.used.load(Ordering::Relaxed), 0);
        assert_eq!(mem_stat_2.used.load(Ordering::Relaxed), 0);
        assert_eq!(TEST_GLOBAL.used.load(Ordering::Relaxed), 0);
        buffer.dealloc(&mem_stat_2, 1);
        assert_eq!(mem_stat_1.used.load(Ordering::Relaxed), -1);
        assert_eq!(mem_stat_2.used.load(Ordering::Relaxed), 0);
        assert_eq!(TEST_GLOBAL.used.load(Ordering::Relaxed), -1);

        buffer.dealloc(&mem_stat_1, 1);
        assert_eq!(mem_stat_1.used.load(Ordering::Relaxed), -1);
        assert_eq!(mem_stat_2.used.load(Ordering::Relaxed), -1);
        assert_eq!(TEST_GLOBAL.used.load(Ordering::Relaxed), -2);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_dealloc_with_unique_allocator() -> Result<(), OutOfLimit> {
        static TEST_GLOBAL: MemStat = MemStat::global();

        let mut buffer = MemStatBuffer::empty(&TEST_GLOBAL);

        let mem_stat = MemStat::create(String::from("test"));

        buffer.dealloc(&mem_stat, 1);
        assert_eq!(mem_stat.used.load(Ordering::Relaxed), -1);
        assert_eq!(TEST_GLOBAL.used.load(Ordering::Relaxed), -1);

        buffer.dealloc(&mem_stat, MEM_STAT_BUFFER_SIZE - 2);
        assert_eq!(
            mem_stat.used.load(Ordering::Relaxed),
            -(MEM_STAT_BUFFER_SIZE - 1)
        );
        assert_eq!(
            TEST_GLOBAL.used.load(Ordering::Relaxed),
            -(MEM_STAT_BUFFER_SIZE - 1)
        );

        buffer.dealloc(&mem_stat, 1);
        assert_eq!(mem_stat.used.load(Ordering::Relaxed), -MEM_STAT_BUFFER_SIZE);
        assert_eq!(
            TEST_GLOBAL.used.load(Ordering::Relaxed),
            -MEM_STAT_BUFFER_SIZE
        );

        Ok(())
    }
}
