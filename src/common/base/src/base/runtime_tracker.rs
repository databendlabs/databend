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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::FutureExt;

#[thread_local]
static mut TRACKER: Option<ThreadTracker> = None;

/// A flag indicating if current thread panicked due to exceeding memory limit.
///
/// While panicking, building a backtrace allocates a few more memory, these allocation should not trigger panicking again.
#[thread_local]
static PANICKING: AtomicBool = AtomicBool::new(false);

static UNTRACKED_MEMORY_LIMIT: i64 = 4 * 1024 * 1024;

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

    /// The limit of max used memory for this tracker.
    ///
    /// Set to 0 to disable the limit.
    limit: AtomicI64,

    parent_memory_tracker: Option<Arc<MemoryTracker>>,
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
            memory_usage: AtomicI64::new(0),
            limit: AtomicI64::new(0),
            parent_memory_tracker,
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

    /// Accumulate memory usage and check if it exceeds the limit.
    #[inline]
    pub fn record_memory(&self, state: &StatBuffer) {
        if PANICKING.load(Ordering::Relaxed) {
            // This thread already panicked, do not panic again, do not push event to parent tracker either.
            return;
        }

        let mut used = self
            .memory_usage
            .fetch_add(state.memory_usage, Ordering::Relaxed);

        used += state.memory_usage;

        let limit = self.limit.load(Ordering::Relaxed);

        if limit > 0 && used > limit {
            // Before panicking, disable limit checking. Otherwise it will panic again when building a backtrace.
            PANICKING.store(true, Ordering::Relaxed);

            panic!(
                "memory usage({}) exceeds user defined limit({})",
                used, limit
            );
        }

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
