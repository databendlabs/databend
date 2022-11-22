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

static UNTRACKED_MEMORY_LIMIT: i64 = 4 * 1024 * 1024;

pub struct ThreadTracker {
    mem_tracker: Arc<MemoryTracker>,

    // Buffered memory allocation stats is not reported to MemoryTracker and can not be seen.
    alloc_buf: StatBuffer,
    dealloc_buf: StatBuffer,
}

impl ThreadTracker {
    pub fn create(mem_tracker: Arc<MemoryTracker>) -> *mut ThreadTracker {
        unsafe {
            TRACKER = Box::into_raw(Box::new(ThreadTracker {
                mem_tracker,
                alloc_buf: Default::default(),
                dealloc_buf: Default::default(),
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
            if TRACKER.is_null() {
                return;
            }

            (*TRACKER).alloc_buf.incr(size);

            if (*TRACKER).alloc_buf.bytes > UNTRACKED_MEMORY_LIMIT {
                (*TRACKER)
                    .mem_tracker
                    .alloc_memory((*TRACKER).alloc_buf.n, (*TRACKER).alloc_buf.bytes);
                (*TRACKER).alloc_buf.reset();
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
            if TRACKER.is_null() {
                return;
            }

            (*TRACKER).dealloc_buf.incr(size);

            if (*TRACKER).dealloc_buf.bytes > UNTRACKED_MEMORY_LIMIT {
                (*TRACKER)
                    .mem_tracker
                    .dealloc_memory((*TRACKER).dealloc_buf.n, (*TRACKER).dealloc_buf.bytes);
                (*TRACKER).dealloc_buf.reset();
            }
        }
    }
}

pub struct MemoryTracker {
    /// Count of calls to `alloc`.
    n_alloc: AtomicI64,

    /// Number of allocated bytes.
    bytes_alloc: AtomicI64,

    /// Count of calls to `dealloc`.
    n_dealloc: AtomicI64,

    /// Number of deallocated bytes.
    bytes_dealloc: AtomicI64,

    parent_memory_tracker: Option<Arc<MemoryTracker>>,
}

/// Buffering memory allocation stats.
///
/// A StatBuffer buffers stats changes in local variables, and periodically flush them to other storage such as an `Arc<T>` shared by several threads.
#[derive(Clone, Debug, Default)]
pub struct StatBuffer {
    n: i64,
    bytes: i64,
}

impl StatBuffer {
    pub fn incr(&mut self, bs: i64) {
        self.n += 1;
        self.bytes += bs;
    }

    pub fn reset(&mut self) {
        self.n = 0;
        self.bytes = 0;
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
            n_alloc: AtomicI64::new(0),
            bytes_alloc: AtomicI64::new(0),
            n_dealloc: AtomicI64::new(0),
            bytes_dealloc: AtomicI64::new(0),
        })
    }

    #[inline]
    pub fn alloc_memory(&self, n: i64, size: i64) {
        self.bytes_alloc.fetch_add(size, Ordering::Relaxed);
        self.n_alloc.fetch_add(n, Ordering::Relaxed);

        if let Some(parent_memory_tracker) = &self.parent_memory_tracker {
            parent_memory_tracker.alloc_memory(n, size);
        }
    }

    #[inline]
    pub fn dealloc_memory(&self, n: i64, size: i64) {
        self.bytes_dealloc.fetch_add(size, Ordering::Relaxed);
        self.n_dealloc.fetch_add(n, Ordering::Relaxed);

        if let Some(parent_memory_tracker) = &self.parent_memory_tracker {
            parent_memory_tracker.dealloc_memory(n, size);
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
        self.bytes_alloc.load(Ordering::Relaxed) - self.bytes_dealloc.load(Ordering::Relaxed)
    }
}

impl MemoryTracker {
    pub fn on_stop_thread(self: &Arc<Self>) -> impl Fn() {
        move || unsafe {
            let thread_tracker = std::mem::replace(&mut TRACKER, std::ptr::null_mut());

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

pub struct AsyncThreadTracker<T: Future> {
    inner: Pin<Box<T>>,
    thread_tracker: *mut ThreadTracker,
    old_thread_tracker: Option<*mut ThreadTracker>,
}

unsafe impl<T: Future + Send> Send for AsyncThreadTracker<T> {}

impl<T: Future> AsyncThreadTracker<T> {
    pub fn create(tracker: *mut ThreadTracker, inner: T) -> AsyncThreadTracker<T> {
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
        self.old_thread_tracker = Some(ThreadTracker::current());
        let new_tracker = self.thread_tracker;
        ThreadTracker::attach_thread_tracker(new_tracker);
        let res = self.inner.poll_unpin(cx);
        ThreadTracker::attach_thread_tracker(self.old_thread_tracker.take().unwrap());
        res
    }
}

impl<T: Future> Drop for AsyncThreadTracker<T> {
    fn drop(&mut self) {
        if let Some(old_thread_tracker) = self.old_thread_tracker.take() {
            ThreadTracker::attach_thread_tracker(old_thread_tracker);
        }
    }
}
