// Copyright 2021 Datafuse Labs.
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

use std::future::Future;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::FutureExt;

#[thread_local]
static mut TRACKER: Option<ThreadTracker> = None;

/// Thread local memory allocation stat buffer for fast update.
///
/// The stat will be periodically flush to global stat tracker `GLOBAL_TRACKER`.
#[thread_local]
static mut MEM_STAT_BUFFER: StatBuffer = StatBuffer::empty();

/// Limit process-wise memory usage by checking if `GLOBAL_TRACKER.memory_usage` exceeds the limit defined in this struct.
#[thread_local]
static mut GLOBAL_LIMIT: GlobalMemoryLimit = GlobalMemoryLimit::empty();

/// Global memory allocation stat tracker.
pub static GLOBAL_TRACKER: MemoryTracker = MemoryTracker {
    memory_usage: AtomicI64::new(0),

    parent_memory_tracker: None,
};

static UNTRACKED_MEMORY_LIMIT: i64 = 4 * 1024 * 1024;

pub fn get_memory_usage() -> i64 {
    GLOBAL_TRACKER.memory_usage.load(Ordering::Relaxed)
}

pub fn is_allow_to_alloc(size: i64) -> bool {
    debug_assert!(size >= 0);

    let x = unsafe { &mut GLOBAL_LIMIT };
    x.check_alloc_allowed(size)
}

pub fn set_memory_limit(mut size: i64) {
    // It may cause the process unable to run if memory limit is too low.
    const LOWEST: i64 = 256 * 1024 * 1024;

    if size > 0 && size < LOWEST {
        size = LOWEST;
    }

    unsafe {
        GLOBAL_LIMIT.limit.store(size, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct ThreadTracker {
    mem_tracker: Arc<MemoryTracker>,

    // Buffered memory allocation stats is not reported to MemoryTracker and can not be seen.
    buffer: StatBuffer,
}

/// A guard that restores the thread local tracker to `old` when being dropped.
pub struct TrackerGuard<'a> {
    old: &'a mut Option<ThreadTracker>,
}

impl<'a> Drop for TrackerGuard<'a> {
    fn drop(&mut self) {
        *self.old = ThreadTracker::attach_thread_tracker(self.old.take());
    }
}

impl ThreadTracker {
    pub fn create(mem_tracker: Arc<MemoryTracker>) -> ThreadTracker {
        ThreadTracker {
            mem_tracker,
            buffer: Default::default(),
        }
    }

    #[inline]
    pub fn current() -> &'static ThreadTracker {
        unsafe { TRACKER.as_ref().unwrap() }
    }

    pub fn fork() -> Option<ThreadTracker> {
        unsafe { TRACKER.as_ref().cloned() }
    }

    pub fn attach_thread_tracker(tracker: Option<ThreadTracker>) -> Option<ThreadTracker> {
        unsafe { std::mem::replace(&mut TRACKER, tracker) }
    }

    /// Enters the context that use tracker `p` and returns a guard that restores the previous tracker when being dropped.
    pub fn enter(tracker: &mut Option<ThreadTracker>) -> TrackerGuard {
        *tracker = ThreadTracker::attach_thread_tracker(tracker.take());

        TrackerGuard { old: tracker }
    }

    #[inline]
    pub fn current_mem_tracker() -> Option<Arc<MemoryTracker>> {
        unsafe { TRACKER.as_ref().map(|tracker| tracker.mem_tracker.clone()) }
    }

    /// Accumulate allocated memory.
    ///
    /// `size` is the positive number of allocated bytes.
    /// `p` is the pointer to the allocated memory.
    #[inline]
    pub fn alloc_memory<T: ?Sized>(size: i64, p: &NonNull<T>) {
        let _ = p;

        unsafe {
            // Update global tracker
            if MEM_STAT_BUFFER.incr(size) > UNTRACKED_MEMORY_LIMIT {
                MEM_STAT_BUFFER.flush_to(&GLOBAL_TRACKER.memory_usage);
            }

            if let Some(tracker) = &mut TRACKER {
                tracker.buffer.incr(size);

                if tracker.buffer.memory_usage > UNTRACKED_MEMORY_LIMIT {
                    tracker.mem_tracker.record_memory(&tracker.buffer);
                    tracker.buffer.reset();
                }
            }
        }
    }

    /// Accumulate deallocated memory.
    ///
    /// `size` is positive number of bytes of the memory to deallocate.
    /// `p` is the pointer to the memory to deallocate.
    #[inline]
    pub fn dealloc_memory<T>(size: i64, p: &NonNull<T>) {
        // size > 0
        let _ = p;

        unsafe {
            // Update global tracker
            if MEM_STAT_BUFFER.incr(-size) < -UNTRACKED_MEMORY_LIMIT {
                MEM_STAT_BUFFER.flush_to(&GLOBAL_TRACKER.memory_usage);
            }

            if let Some(tracker) = &mut TRACKER {
                tracker.buffer.decr(size);

                if tracker.buffer.memory_usage < -UNTRACKED_MEMORY_LIMIT {
                    tracker.mem_tracker.record_memory(&tracker.buffer);
                    tracker.buffer.reset();
                }
            }
        }
    }
}

pub struct MemoryTracker {
    memory_usage: AtomicI64,

    parent_memory_tracker: Option<Arc<MemoryTracker>>,
}

pub struct GlobalMemoryLimit {
    /// The rate to check memory usage.
    ///
    /// Checking is expensive thus we do not check it for every memory allocation request.
    check_rate_limit: RateLimiter,

    /// The limit of used memory globally.
    ///
    /// Set to 0 to disable the limit.
    limit: AtomicI64,
}

impl GlobalMemoryLimit {
    pub const fn empty() -> Self {
        Self {
            check_rate_limit: RateLimiter::empty(),
            limit: AtomicI64::new(0),
        }
    }

    /// Check if allocated memory exceeds the limit.
    ///
    /// It does NOT do the check for every call.
    fn check_alloc_allowed(&mut self, size: i64) -> bool {
        // Check max memory threshold for every `CHECK_INTERVAL` MB allocated memory.
        const CHECK_INTERVAL: i64 = 64 * 1024 * 1024;

        let rate_lim = &mut self.check_rate_limit;

        rate_lim.counter += size;

        if rate_lim.counter > rate_lim.next_check {
            rate_lim.next_check = rate_lim.counter + CHECK_INTERVAL;

            let limit = self.limit.load(Ordering::Relaxed);
            if limit == 0 {
                // no limit
                return true;
            }

            let mem_used = GLOBAL_TRACKER.memory_usage.load(Ordering::Relaxed);

            return mem_used < limit;
        }

        true
    }
}

/// Set a rate cap on an action that is less frequent than once per `n` allocated bytes.
#[derive(Clone, Debug, Default)]
pub struct RateLimiter {
    /// This counter is used to set a rate cap on an action that is less frequent than once per `n` allocated bytes.
    counter: i64,

    /// The counter value at which the next check will be done.
    next_check: i64,
}

impl RateLimiter {
    pub const fn empty() -> Self {
        Self {
            counter: 0,
            next_check: 0,
        }
    }
}

/// Buffering memory allocation stats.
///
/// A StatBuffer buffers stats changes in local variables, and periodically flush them to other storage such as an `Arc<T>` shared by several threads.
#[derive(Clone, Debug, Default)]
pub struct StatBuffer {
    memory_usage: i64,
}

impl StatBuffer {
    pub const fn empty() -> Self {
        Self { memory_usage: 0 }
    }

    pub fn incr(&mut self, bs: i64) -> i64 {
        self.memory_usage += bs;
        self.memory_usage
    }

    pub fn decr(&mut self, bs: i64) -> i64 {
        self.memory_usage -= bs;
        self.memory_usage
    }

    pub fn reset(&mut self) {
        self.memory_usage = 0;
    }

    pub fn flush_to(&mut self, st: &AtomicI64) {
        st.fetch_add(self.memory_usage, Ordering::Relaxed);
        self.reset();
    }
}

impl MemoryTracker {
    pub fn create() -> Arc<MemoryTracker> {
        let parent = MemoryTracker::current();
        MemoryTracker::create_sub_tracker(parent)
    }

    pub fn create_sub_tracker(
        parent_memory_tracker: Option<Arc<MemoryTracker>>,
    ) -> Arc<MemoryTracker> {
        Arc::new(MemoryTracker {
            parent_memory_tracker,
            memory_usage: AtomicI64::new(0),
        })
    }

    #[inline]
    pub fn record_memory(&self, state: &StatBuffer) {
        self.memory_usage
            .fetch_add(state.memory_usage, Ordering::Relaxed);

        if let Some(parent_memory_tracker) = &self.parent_memory_tracker {
            parent_memory_tracker.record_memory(state);
        }
    }

    #[inline]
    pub fn current() -> Option<Arc<MemoryTracker>> {
        unsafe { TRACKER.as_ref().map(|tracker| tracker.mem_tracker.clone()) }
    }

    #[inline]
    pub fn get_memory_usage(&self) -> i64 {
        self.memory_usage.load(Ordering::Relaxed)
    }
}

impl MemoryTracker {
    pub fn on_stop_thread(self: &Arc<Self>) -> impl Fn() {
        move || unsafe {
            if let Some(thread_tracker) = TRACKER.take() {
                thread_tracker
                    .mem_tracker
                    .record_memory(&thread_tracker.buffer);
            }
        }
    }

    pub fn on_start_thread(self: &Arc<Self>) -> impl Fn() {
        // TODO: log::info("thread {}-{} started", thread_id, thread_name);
        let mem_tracker = self.clone();

        move || {
            let thread_tracker = ThreadTracker::create(mem_tracker.clone());
            ThreadTracker::attach_thread_tracker(Some(thread_tracker));
        }
    }
}

/// A [`Future`] that enters its thread tracker when being polled.
pub struct AsyncThreadTracker<T: Future> {
    inner: Pin<Box<T>>,
    thread_tracker: Option<ThreadTracker>,
    old_thread_tracker: Option<ThreadTracker>,
}

unsafe impl<T: Future + Send> Send for AsyncThreadTracker<T> {}

impl<T: Future> AsyncThreadTracker<T> {
    pub fn create(tracker: Option<ThreadTracker>, inner: T) -> AsyncThreadTracker<T> {
        AsyncThreadTracker::<T> {
            inner: Box::pin(inner),
            thread_tracker: tracker,
            old_thread_tracker: None,
        }
    }
}

impl<T: Future> Future for AsyncThreadTracker<T> {
    type Output = T::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.old_thread_tracker = ThreadTracker::attach_thread_tracker(self.thread_tracker.take());
        let res = self.inner.poll_unpin(cx);
        self.thread_tracker = ThreadTracker::attach_thread_tracker(self.old_thread_tracker.take());
        res
    }
}

impl<T: Future> Drop for AsyncThreadTracker<T> {
    fn drop(&mut self) {
        if self.old_thread_tracker.is_some() {
            self.thread_tracker =
                ThreadTracker::attach_thread_tracker(self.old_thread_tracker.take());
        }
    }
}
