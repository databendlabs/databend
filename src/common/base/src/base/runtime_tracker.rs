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

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::future::Future;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::FutureExt;

use crate::mem_allocator::GlobalAllocator;

#[thread_local]
static mut TRACKER: *mut ThreadTracker = std::ptr::null_mut();

/// Thread local memory allocation stat buffer for fast update.
///
/// The stat will be periodically flush to global stat tracker `GLOBAL_TRACKER`.
#[thread_local]
static mut MEM_STAT_BUFFER: StatBuffer = StatBuffer::empty();

/// Global memory allocation stat tracker.
pub static GLOBAL_TRACKER: MemoryTracker = MemoryTracker {
    memory_usage: AtomicI64::new(0),

    parent_memory_tracker: None,
};

static UNTRACKED_MEMORY_LIMIT: i64 = 4 * 1024 * 1024;

pub struct ThreadTracker {
    mem_tracker: Arc<MemoryTracker>,

    // Buffered memory allocation stats is not reported to MemoryTracker and can not be seen.
    buffer: StatBuffer,
}

/// A guard that restores the thread local tracker to `old` when being dropped.
pub struct TrackerGuard {
    old: *mut ThreadTracker,
}

impl Drop for TrackerGuard {
    fn drop(&mut self) {
        ThreadTracker::attach_thread_tracker(self.old);
    }
}

impl ThreadTracker {
    pub fn create(mem_tracker: Arc<MemoryTracker>) -> *mut ThreadTracker {
        unsafe {
            TRACKER = Box::into_raw(Box::new(ThreadTracker {
                mem_tracker,
                buffer: Default::default(),
            }));

            TRACKER
        }
    }

    #[inline]
    pub fn current() -> *mut ThreadTracker {
        unsafe { TRACKER }
    }

    pub fn attach_thread_tracker(tracker: *mut ThreadTracker) {
        unsafe {
            TRACKER = tracker;
        }
    }

    /// Enters the context that use tracker `p` and returns a guard that restores the previous tracker when being dropped.
    pub fn enter(tracker: *mut ThreadTracker) -> TrackerGuard {
        let g = TrackerGuard {
            old: ThreadTracker::current(),
        };
        Self::attach_thread_tracker(tracker);
        g
    }

    #[inline]
    pub fn current_mem_tracker() -> Option<Arc<MemoryTracker>> {
        unsafe {
            match TRACKER.is_null() {
                true => None,
                false => Some((*TRACKER).mem_tracker.clone()),
            }
        }
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

            if TRACKER.is_null() {
                return;
            }

            (*TRACKER).buffer.incr(size);

            if (*TRACKER).buffer.memory_usage > UNTRACKED_MEMORY_LIMIT {
                (*TRACKER).mem_tracker.record_memory(&(*TRACKER).buffer);
                (*TRACKER).buffer.reset();
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

            if TRACKER.is_null() {
                return;
            }

            (*TRACKER).buffer.decr(size);

            if (*TRACKER).buffer.memory_usage < -UNTRACKED_MEMORY_LIMIT {
                (*TRACKER).mem_tracker.record_memory(&(*TRACKER).buffer);
                (*TRACKER).buffer.reset();
            }
        }
    }
}

pub struct MemoryTracker {
    memory_usage: AtomicI64,

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
        unsafe {
            let thread_tracker = ThreadTracker::current();
            match thread_tracker.is_null() {
                true => None,
                false => Some((*thread_tracker).mem_tracker.clone()),
            }
        }
    }

    #[inline]
    pub fn get_memory_usage(&self) -> i64 {
        self.memory_usage.load(Ordering::Relaxed)
    }
}

impl MemoryTracker {
    pub fn on_stop_thread(self: &Arc<Self>) -> impl Fn() {
        move || unsafe {
            let thread_tracker = std::mem::replace(&mut TRACKER, std::ptr::null_mut());

            (*thread_tracker)
                .mem_tracker
                .record_memory(&(*thread_tracker).buffer);
            std::ptr::drop_in_place(thread_tracker as usize as *mut ThreadTracker);
            GlobalAllocator.dealloc(thread_tracker as *mut u8, Layout::new::<ThreadTracker>())
        }
    }

    pub fn on_start_thread(self: &Arc<Self>) -> impl Fn() {
        // TODO: log::info("thread {}-{} started", thread_id, thread_name);
        let mem_tracker = self.clone();

        move || {
            ThreadTracker::create(mem_tracker.clone());
        }
    }
}

/// A [`Future`] that enters its thread tracker when being polled.
///
/// [`Future`]: std::future::Future
pub struct AsyncThreadTracker<T: Future> {
    inner: Pin<Box<T>>,
    thread_tracker: *mut ThreadTracker,
}

unsafe impl<T: Future + Send> Send for AsyncThreadTracker<T> {}

impl<T: Future> AsyncThreadTracker<T> {
    pub fn create(tracker: *mut ThreadTracker, inner: T) -> AsyncThreadTracker<T> {
        AsyncThreadTracker::<T> {
            inner: Box::pin(inner),
            thread_tracker: tracker,
        }
    }
}

impl<T: Future> Future for AsyncThreadTracker<T> {
    type Output = T::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Start using this tracker and restore to the old tracker when `_g` is dropped.
        let _g = ThreadTracker::enter(self.thread_tracker);
        self.inner.poll_unpin(cx)
    }
}
