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

//! Memory allocation stat tracker.
//!
//! Every thread has a thread local  `ThreadTracker` instance, inside which, there is a reference
//! to a `MemStat`.  A `MemStat` can be shared by multiple `ThreadTracker`.  A
//! `ThreadTracker` buffers allocation stat and flushes it to its `MemStat` when necessary.
//!
//! `MemStat` is organized in a hierarchical structure: when allocation stat is flushed to a
//! `MemStat`, it will then report the stat to its parent, and so on.  Finally if its parent
//! is `None`, it flushes stat to `GLOBAL_TRACKER`, which is the root of the `MemStat` tree.
//!
//! A reporting path could be `T3 -> M4 -> M2 -> G`, or `T1 -> G`:
//!
//! ```text
//! GLOBAL_TRACKER(G) <--- ThreadTracker(T1)
//! ^     ^      ^
//! |     |      '-------- ThreadTracker(T2)
//! |     `--------MemStat(M2)
//! |                 ^     ^
//! MemStat(M1)       |     '-----MemStat(M4) <--- ThreadTracker(T3)
//!                   |                    ^
//!                   MemStat(M3)          '------ ThreadTracker(T4)
//! ```
//!
//! A ThreadTracker that points to `GLOBAL_TRACKER` is installed automatically for every thread,
//! unless an application replaced it via `ThreadTracker::swap_with()`.
//!
//! An `TrackedFuture` has a embedded `ThreadTracker` installed for its inner `Future`.
//! When `TrackedFuture` is `poll()`ed, its `ThreadTracker` is installed to the running thread
//! and will be restored when `poll()` returns.

use std::alloc::AllocError;
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use pin_project_lite::pin_project;

use crate::runtime::memory::MemStat;
use crate::runtime::memory::OutOfLimit;
use crate::runtime::memory::StatBuffer;
use crate::runtime::memory::GLOBAL_MEM_STAT;

// For implemented and needs to call drop, we cannot use the attribute tag thread local.
// https://play.rust-lang.org/?version=nightly&mode=debug&edition=2021&gist=ea33533387d401e86423df1a764b5609
thread_local! {
    static TRACKER: RefCell<ThreadTracker> = const { RefCell::new(ThreadTracker::empty()) };
}

/// A guard that restores the thread local tracker to the `saved` when dropped.
pub struct Entered {
    /// Saved tracker for restoring
    saved: Option<Arc<MemStat>>,
}

impl Drop for Entered {
    fn drop(&mut self) {
        let _ = StatBuffer::current().flush::<false>();
        ThreadTracker::replace_mem_stat(self.saved.take());
    }
}

pub struct LimitMemGuard {
    saved: bool,
}

impl LimitMemGuard {
    pub fn enter_unlimited() -> Self {
        Self {
            saved: StatBuffer::current().set_unlimited_flag(true),
        }
    }

    pub fn enter_limited() -> Self {
        Self {
            saved: StatBuffer::current().set_unlimited_flag(false),
        }
    }

    pub(crate) fn is_unlimited() -> bool {
        StatBuffer::current().is_unlimited()
    }
}

impl Drop for LimitMemGuard {
    fn drop(&mut self) {
        StatBuffer::current().set_unlimited_flag(self.saved);
    }
}

/// A per-thread tracker that tracks memory usage stat.
#[derive(Default)]
pub struct ThreadTracker {
    mem_stat: Option<Arc<MemStat>>,
    out_of_limit_desc: Option<String>,
}

impl Drop for ThreadTracker {
    fn drop(&mut self) {
        StatBuffer::current().mark_destroyed();
    }
}

/// A memory stat tracker with buffer.
///
/// Every ThreadTracker belongs to one MemStat.
/// A MemStat might receive memory stat from more than one ThreadTracker.
impl ThreadTracker {
    pub(crate) const fn empty() -> Self {
        Self {
            mem_stat: None,
            out_of_limit_desc: None,
        }
    }

    /// Replace the `mem_stat` with the current thread's.
    pub fn replace_mem_stat(mem_state: Option<Arc<MemStat>>) -> Option<Arc<MemStat>> {
        TRACKER.with(|v: &RefCell<ThreadTracker>| {
            let mut borrow_mut = v.borrow_mut();
            let old = borrow_mut.mem_stat.take();
            borrow_mut.mem_stat = mem_state;
            old
        })
    }

    /// Replace the `out_of_limit_desc` with the current thread's.
    pub fn replace_error_message(desc: Option<String>) -> Option<String> {
        TRACKER.with(|v: &RefCell<ThreadTracker>| {
            let mut borrow_mut = v.borrow_mut();
            let old = borrow_mut.out_of_limit_desc.take();
            borrow_mut.out_of_limit_desc = desc;
            old
        })
    }

    /// Enters a context in which it reports memory stats to `mem stat` and returns a guard that restores the previous mem stat when being dropped.
    pub fn enter(mem_state: Option<Arc<MemStat>>) -> Entered {
        let _ = StatBuffer::current().flush::<false>();
        Entered {
            saved: ThreadTracker::replace_mem_stat(mem_state),
        }
    }

    /// Accumulate stat about allocated memory.
    ///
    /// `size` is the positive number of allocated bytes.
    #[inline]
    pub fn alloc(size: i64) -> Result<(), AllocError> {
        if let Err(out_of_limit) = StatBuffer::current().alloc(size) {
            // https://play.rust-lang.org/?version=stable&mode=debug&edition=2021&gist=03d21a15e52c7c0356fca04ece283cf9
            if !std::thread::panicking() && !LimitMemGuard::is_unlimited() {
                let _guard = LimitMemGuard::enter_unlimited();
                ThreadTracker::replace_error_message(Some(format!("{:?}", out_of_limit)));
                return Err(AllocError);
            }
        }

        Ok(())
    }

    /// Accumulate deallocated memory.
    ///
    /// `size` is positive number of bytes of the memory to deallocate.
    #[inline]
    pub fn dealloc(size: i64) {
        StatBuffer::current().dealloc(size)
    }

    pub fn record_memory<const NEED_ROLLBACK: bool>(memory_usage: i64) -> Result<(), OutOfLimit> {
        let has_thread_local = TRACKER.try_with(|tracker: &RefCell<ThreadTracker>| {
            // We need to ensure no heap memory alloc or dealloc. it will cause panic of borrow recursive call.
            let tracker = tracker.borrow_mut();
            let mem_stat = tracker.mem_stat.as_deref().unwrap_or(&GLOBAL_MEM_STAT);
            mem_stat.record_memory::<NEED_ROLLBACK>(memory_usage)
        });

        match has_thread_local {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(oom)) => Err(oom),
            Err(_access_error) => GLOBAL_MEM_STAT.record_memory::<NEED_ROLLBACK>(memory_usage),
        }
    }
}

pin_project! {
    /// A [`Future`] that enters its thread tracker when being polled.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct TrackedFuture<T> {
        #[pin]
        inner: T,

        mem_stat: Option<Arc<MemStat>>,
    }
}

impl<T> TrackedFuture<T> {
    pub fn create(inner: T) -> TrackedFuture<T> {
        Self::create_with_mem_stat(None, inner)
    }

    pub fn create_with_mem_stat(mem_stat: Option<Arc<MemStat>>, inner: T) -> Self {
        Self { inner, mem_stat }
    }
}

impl<T: Future> Future for TrackedFuture<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let _g = ThreadTracker::enter(this.mem_stat.clone());
        this.inner.poll(cx)
    }
}

pin_project! {
    /// A [`Future`] that enters its thread tracker when being polled.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct UnlimitedFuture<T> {
        #[pin]
        inner: T,
    }
}

impl<T> UnlimitedFuture<T> {
    pub fn create(inner: T) -> UnlimitedFuture<T> {
        UnlimitedFuture::<T> { inner }
    }
}

impl<T: Future> Future for UnlimitedFuture<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _guard = LimitMemGuard::enter_unlimited();

        let this = self.project();
        this.inner.poll(cx)
    }
}

#[cfg(test)]
mod tests {
    mod async_thread_tracker {
        use std::future::Future;
        use std::pin::Pin;
        use std::task::Context;
        use std::task::Poll;

        use crate::runtime::memory::MemStat;
        use crate::runtime::memory::StatBuffer;
        use crate::runtime::TrackedFuture;

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

            let mem_stat = MemStat::create("test_async_thread_tracker_normal_quit".to_string());

            let f = Foo { i: 3 };
            let f = TrackedFuture::create_with_mem_stat(Some(mem_stat.clone()), f);

            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;

            let v = rt.block_on(f);

            let used = mem_stat.get_memory_usage();
            assert_eq!(
                3 * 1024 * 1024,
                used,
                "when future dropped, mem stat buffer is flushed"
            );

            drop(v);

            let _ = StatBuffer::current().flush::<false>();

            let used = mem_stat.get_memory_usage();
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

        use crate::runtime::memory::MemStat;
        use crate::runtime::TrackedFuture;

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
            fn run_fut_in_rt(mem_stat: &Arc<MemStat>) -> i64 {
                let f = Foo { i: 8 };
                let f = TrackedFuture::create_with_mem_stat(Some(mem_stat.clone()), f);

                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(5)
                    .enable_all()
                    .build()
                    .unwrap();

                rt.block_on(async {
                    let h = crate::runtime::spawn(f);
                    let res = h.await;
                    assert!(res.is_err(), "panicked");
                });
                mem_stat.get_memory_usage()
            }

            let mem_stat = MemStat::create("test_async_thread_tracker_panic".to_string());

            let used0 = run_fut_in_rt(&mem_stat);
            let used1 = run_fut_in_rt(&mem_stat);

            // The constantly used memory is about 1MB.
            assert!(used1 - used0 < 1024 * 1024);
            assert!(used0 - used1 < 1024 * 1024);

            Ok(())
        }
    }
}
