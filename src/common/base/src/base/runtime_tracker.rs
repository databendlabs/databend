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

//! Memory allocation stat tracker.
//!
//! Every thread has a thread local  `ThreadTracker` instance, inside which, there is a reference
//! to a `MemoryTracker`.  A `MemoryTracker` can be shared by multiple `ThreadTracker`.  A
//! `ThreadTracker` buffers allocation stat and flushes it to its `MemoryTracker` when necessary.
//!
//! `MemoryTracker` is organized in a hierarchical structure: when allocation stat is flushed to a
//! `MemoryTracker`, it will then report the stat to its parent, and so on.  Finally if its parent
//! is `None`, it flushes stat to `GLOBAL_TRACKER`, which is the root of the `MemoryTracker` tree.
//!
//! A reporting path could be `T3 -> M4 -> M2 -> G`, or `T1 -> G`:
//!
//! ```text
//! GLOBAL_TRACKER(G) <--- ThreadTracker(T1)
//! ^     ^     ^
//! |     |     `------ ThreadTracker(T2)
//! |     `------------.
//! |                  MemoryTracker(M2)
//! MemoryTracker(M1)     ^     ^
//!                       |     `------------.
//!                       |                  MemoryTracker(M4) <--- ThreadTracker(T3)
//!                       MemoryTracker(M3)           ^
//!                                                    `------- ThreadTracker(T4)
//! ```
//!
//! A ThreadTracker that points to `GLOBAL_TRACKER` is installed automatically for every thread,
//! unless an application replaced it via `ThreadTracker::swap_with()`.
//!
//! An `AsyncThreadTracker` has a embedeed `ThreadTracker` installed for its inner `Future`.
//! When `AsyncThreadTracker` is `poll()`ed, its `ThreadTracker` is installed to the running thread
//! and will be restored when `poll()` returns.

use std::fmt::Debug;
use std::fmt::Formatter;
use std::future::Future;
use std::mem::take;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use bytesize::ByteSize;
use pin_project_lite::pin_project;

/// The root tracker.
///
/// Every alloc/dealloc stat will be fed to this tracker.
pub static GLOBAL_TRACKER: MemoryTracker = MemoryTracker::empty();

#[thread_local]
static mut TRACKER: ThreadTracker = ThreadTracker::empty();

/// Flag indicating an exceeding limit panic is happening and allocating memory by panic handler is allowed.
///
/// Flag will be reset when `panic!()` returns.
#[thread_local]
static PANICKING: AtomicBool = AtomicBool::new(false);

static UNTRACKED_MEMORY_LIMIT: i64 = 4 * 1024 * 1024;

/// A guard that restores the thread local tracker to the `saved` when dropped.
pub struct Entered<'a> {
    /// Saved tracker for restoring
    saved: &'a mut ThreadTracker,
}

impl<'a> Drop for Entered<'a> {
    fn drop(&mut self) {
        ThreadTracker::swap_with(self.saved);
    }
}

/// A guard that resets the `PANICKING` flag when dropped.
pub struct Panicking;

impl Drop for Panicking {
    fn drop(&mut self) {
        PANICKING.store(false, Ordering::Relaxed);
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

/// A per-thread tracker that tracks memory usage stat.
///
/// Disable `Clone` to prevent accidentally duplicating the buffer.
#[derive(Default)]
pub struct ThreadTracker {
    mem_tracker: Option<Arc<MemoryTracker>>,

    /// Buffered memory allocation stat that is yet not reported to `mem_tracker` and can not be seen.
    buffer: StatBuffer,
}

impl Drop for ThreadTracker {
    fn drop(&mut self) {
        let buf = take(&mut self.buffer);
        let _ = MemoryTracker::record_memory(&self.mem_tracker, buf);
    }
}

/// A memory stat tracker with buffer.
///
/// Every ThreadTracker belongs to one MemoryTracker.
/// A MemoryTracker might receive memory stat from more than one ThreadTracker.
impl ThreadTracker {
    pub const fn empty() -> Self {
        Self {
            mem_tracker: None,
            buffer: StatBuffer::empty(),
        }
    }

    pub fn create(mem_tracker: Option<Arc<MemoryTracker>>) -> ThreadTracker {
        ThreadTracker {
            mem_tracker,
            buffer: Default::default(),
        }
    }

    /// Create a ThreadTracker sharing the same internal MemoryTracker with the current thread.
    pub fn fork() -> ThreadTracker {
        let mt = unsafe { TRACKER.mem_tracker.clone() };
        ThreadTracker::create(mt)
    }

    /// Swap the `tracker` with the current thread's.
    pub fn swap_with(tracker: &mut ThreadTracker) {
        unsafe { std::mem::swap(&mut TRACKER, tracker) }
    }

    /// Enters a context in which it reports memory stats to `tracker` and returns a guard that restores the previous tracker when being dropped.
    ///
    /// When entered, `tracker` is swapped with the thread local tracker `TRACKER`.
    pub fn enter(tracker: &mut ThreadTracker) -> Entered {
        ThreadTracker::swap_with(tracker);

        Entered { saved: tracker }
    }

    #[inline]
    pub fn current_mem_tracker() -> Option<Arc<MemoryTracker>> {
        unsafe { TRACKER.mem_tracker.clone() }
    }

    /// Accumulate stat about allocated memory.
    ///
    /// `size` is the positive number of allocated bytes.
    #[inline]
    pub fn alloc_memory(size: i64) {
        let tracker = unsafe { &mut TRACKER };

        let used = tracker.buffer.incr(size);

        if used <= UNTRACKED_MEMORY_LIMIT {
            return;
        }

        let res = tracker.flush();

        if let Err(out_of_limit) = res {
            // NOTE: `PANICKING` only allows allocation inside the following `panic!()`.
            //       If a `Drop` is called when unwinding, the `Drop` may panic again if it allocates memory over the limit.
            if PANICKING.load(Ordering::Relaxed) {
                return;
            }

            PANICKING.store(true, Ordering::Relaxed);

            // Reset PANICKING when dropped.
            let _p = Panicking;

            panic!("{:?}", out_of_limit);
        }
    }

    /// Accumulate deallocated memory.
    ///
    /// `size` is positive number of bytes of the memory to deallocate.
    #[inline]
    pub fn dealloc_memory(size: i64) {
        // size > 0

        let tracker = unsafe { &mut TRACKER };

        let used = tracker.buffer.incr(-size);

        if used >= -UNTRACKED_MEMORY_LIMIT {
            return;
        }

        let _ = tracker.flush();

        // NOTE: De-allocation does not panic
        // even when it's possible exceeding the limit
        // due to other threads sharing the same MemoryTracker may have allocated a lot of memory.
    }

    /// Flush buffered stat to MemoryTracker it belongs to.
    pub fn flush(&mut self) -> Result<(), OutOfLimit> {
        let buf = take(&mut self.buffer);
        MemoryTracker::record_memory(&self.mem_tracker, buf)
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
}

/// A tracking unit.
///
/// - A MemoryTracker have child MemoryTracker.
/// - Every stat that is fed to a child is also fed to its parent.
/// - A MemoryTracker has at most one parent.
pub struct MemoryTracker {
    memory_usage: AtomicI64,

    /// The limit of max used memory for this tracker.
    ///
    /// Set to 0 to disable the limit.
    limit: AtomicI64,

    parent_memory_tracker: Option<Arc<MemoryTracker>>,
}

impl MemoryTracker {
    pub const fn empty() -> Self {
        Self {
            memory_usage: AtomicI64::new(0),
            limit: AtomicI64::new(0),
            parent_memory_tracker: None,
        }
    }

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

    /// Feed memory usage stat to MemoryTracker and return if it exceeds the limit.
    ///
    /// It feeds `state` to the this tracker and all of its ancestors, including GLOBAL_TRACKER.
    #[inline]
    pub fn record_memory(
        tracker: &Option<Arc<MemoryTracker>>,
        state: StatBuffer,
    ) -> Result<(), OutOfLimit> {
        let mut is_root = false;

        let mem_tracker = match tracker {
            Some(x) => x,
            None => {
                // No parent, report to GLOBAL_TRACKER
                is_root = true;
                &GLOBAL_TRACKER
            }
        };

        let mut used = mem_tracker
            .memory_usage
            .fetch_add(state.memory_usage, Ordering::Relaxed);

        used += state.memory_usage;

        if !is_root {
            Self::record_memory(&mem_tracker.parent_memory_tracker, state)?;
        }

        mem_tracker.check_limit(used)
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
    pub fn current() -> Option<Arc<MemoryTracker>> {
        unsafe { TRACKER.mem_tracker.clone() }
    }

    #[inline]
    pub fn get_memory_usage(&self) -> i64 {
        self.memory_usage.load(Ordering::Relaxed)
    }

    pub fn on_start_thread(self: &Arc<Self>) -> impl Fn() {
        let mem_tracker = self.clone();

        move || {
            let mut thread_tracker = ThreadTracker::create(Some(mem_tracker.clone()));
            ThreadTracker::swap_with(&mut thread_tracker);

            debug_assert!(
                thread_tracker.mem_tracker.is_none(),
                "a new thread must have no tracker"
            );
        }
    }
}

pin_project! {
    /// A [`Future`] that enters its thread tracker when being polled.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct AsyncThreadTracker<T> {
        #[pin]
        inner: T,

        thread_tracker: ThreadTracker,
    }
}

impl<T> AsyncThreadTracker<T> {
    pub fn create(tracker: ThreadTracker, inner: T) -> AsyncThreadTracker<T> {
        AsyncThreadTracker::<T> {
            inner,
            thread_tracker: tracker,
        }
    }
}

impl<T: Future> Future for AsyncThreadTracker<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let _g = ThreadTracker::enter(this.thread_tracker);
        this.inner.poll(cx)
    }
}

#[cfg(test)]
mod tests {

    mod async_thread_tracker {
        use std::future::Future;
        use std::pin::Pin;
        use std::sync::Arc;
        use std::task::Context;
        use std::task::Poll;

        use crate::base::runtime_tracker::TRACKER;
        use crate::base::AsyncThreadTracker;
        use crate::base::MemoryTracker;
        use crate::base::ThreadTracker;

        struct Foo {
            i: usize,
        }

        impl Future for Foo {
            type Output = Vec<u8>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let _ = cx;
                let v = Vec::with_capacity(self.i * 1024 * 1024);

                Poll::Ready(v)
            }
        }

        #[test]
        fn test_async_thread_tracker_normal_quit() -> anyhow::Result<()> {
            // A future alloc memory and it should be tracked.
            // The memory is passed out and is de-allocated outside the future and should not be tracked.

            let mem_tracker = Arc::new(MemoryTracker::empty());
            let tracker = ThreadTracker::create(Some(mem_tracker.clone()));

            let f = Foo { i: 3 };
            let f = AsyncThreadTracker::create(tracker, f);

            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;

            let v = rt.block_on(f);

            let used = mem_tracker.get_memory_usage();
            assert_eq!(
                3 * 1024 * 1024,
                used,
                "when future dropped, mem stat buffer is flushed"
            );

            drop(v);
            unsafe { &mut TRACKER.flush() };

            let used = mem_tracker.get_memory_usage();
            assert_eq!(
                3 * 1024 * 1024,
                used,
                "can not see mem dropped outside the future"
            );

            Ok(())
        }
    }

    mod async_thread_tracker_panic {
        use std::future::Future;
        use std::pin::Pin;
        use std::sync::Arc;
        use std::task::Context;
        use std::task::Poll;

        use crate::base::AsyncThreadTracker;
        use crate::base::MemoryTracker;
        use crate::base::ThreadTracker;

        struct Foo {
            i: usize,
        }

        impl Future for Foo {
            type Output = Vec<u8>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let _ = cx;
                let _v: Vec<u8> = Vec::with_capacity(self.i * 1024 * 1024);
                panic!("foo");
            }
        }

        #[test]
        fn test_async_thread_tracker_panic() -> anyhow::Result<()> {
            // A future alloc memory then panic.
            // The memory stat should revert to 0.
            //
            // But it looks panicking allocates some memory.
            // The used memory after the first panic stays stable.

            // Run a future in a one-shot runtime, return the used memory.
            fn run_fut_in_rt(mem_tracker: &Arc<MemoryTracker>) -> i64 {
                let tracker = ThreadTracker::create(Some(mem_tracker.clone()));

                let f = Foo { i: 8 };
                let f = AsyncThreadTracker::create(tracker, f);

                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(5)
                    .enable_all()
                    .build()
                    .unwrap();

                rt.block_on(async {
                    let h = tokio::spawn(f);
                    let res = h.await;
                    assert!(res.is_err(), "panicked");
                });
                mem_tracker.get_memory_usage()
            }

            let mem_tracker = Arc::new(MemoryTracker::empty());

            let used0 = run_fut_in_rt(&mem_tracker);
            let used1 = run_fut_in_rt(&mem_tracker);

            // The constantly used memory is about 1MB.
            assert!(used1 - used0 < 1024 * 1024);
            assert!(used0 - used1 < 1024 * 1024);

            Ok(())
        }
    }
}
