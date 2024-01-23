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
use std::fmt::Debug;
use std::fmt::Formatter;
use std::future::Future;
use std::mem::take;
use std::pin::Pin;
use std::ptr::addr_of_mut;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use bytesize::ByteSize;
use log::info;
use pin_project_lite::pin_project;

/// The root tracker.
///
/// Every alloc/dealloc stat will be fed to this tracker.
pub static GLOBAL_MEM_STAT: MemStat = MemStat::global();

// For implemented and needs to call drop, we cannot use the attribute tag thread local.
// https://play.rust-lang.org/?version=nightly&mode=debug&edition=2021&gist=ea33533387d401e86423df1a764b5609
thread_local! {
    static TRACKER: RefCell<ThreadTracker> = const { RefCell::new(ThreadTracker::empty()) };
}

#[thread_local]
static mut STAT_BUFFER: StatBuffer = StatBuffer::empty();

static MEM_STAT_BUFFER_SIZE: i64 = 4 * 1024 * 1024;

pub fn set_alloc_error_hook() {
    std::alloc::set_alloc_error_hook(|layout| {
        let _guard = LimitMemGuard::enter_unlimited();

        let out_of_limit_desc = ThreadTracker::replace_error_message(None);

        panic!(
            "{}",
            out_of_limit_desc
                .unwrap_or_else(|| format!("memory allocation of {} bytes failed", layout.size()))
        );
    })
}

/// A guard that restores the thread local tracker to the `saved` when dropped.
pub struct Entered {
    /// Saved tracker for restoring
    saved: Option<Arc<MemStat>>,
}

impl Drop for Entered {
    fn drop(&mut self) {
        unsafe {
            let _ = STAT_BUFFER.flush::<false>();
            ThreadTracker::replace_mem_stat(self.saved.take());
        }
    }
}

pub struct LimitMemGuard {
    saved: bool,
}

impl LimitMemGuard {
    pub fn enter_unlimited() -> Self {
        unsafe {
            let saved = STAT_BUFFER.unlimited_flag;
            STAT_BUFFER.unlimited_flag = true;
            Self { saved }
        }
    }

    pub fn enter_limited() -> Self {
        unsafe {
            let saved = STAT_BUFFER.unlimited_flag;
            STAT_BUFFER.unlimited_flag = false;
            Self { saved }
        }
    }

    pub(crate) fn is_unlimited() -> bool {
        unsafe { STAT_BUFFER.unlimited_flag }
    }
}

impl Drop for LimitMemGuard {
    fn drop(&mut self) {
        unsafe {
            STAT_BUFFER.unlimited_flag = self.saved;
        }
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
#[derive(Default)]
pub struct ThreadTracker {
    mem_stat: Option<Arc<MemStat>>,
    out_of_limit_desc: Option<String>,
}

impl Drop for ThreadTracker {
    fn drop(&mut self) {
        unsafe {
            let _guard = LimitMemGuard::enter_unlimited();
            let memory_usage = take(&mut STAT_BUFFER.memory_usage);

            // Memory operations during destruction will be recorded to global stat.
            STAT_BUFFER.destroyed_thread_local_macro = true;
            let _ = MemStat::record_memory::<false>(&None, memory_usage);
        }
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
        unsafe {
            let _ = STAT_BUFFER.flush::<false>();
            Entered {
                saved: ThreadTracker::replace_mem_stat(mem_state),
            }
        }
    }

    /// Accumulate stat about allocated memory.
    ///
    /// `size` is the positive number of allocated bytes.
    #[inline]
    pub fn alloc(size: i64) -> Result<(), AllocError> {
        let state_buffer = unsafe { addr_of_mut!(STAT_BUFFER) };

        // Rust will alloc or dealloc memory after the thread local is destroyed when we using thread_local macro.
        // This is the boundary of thread exit. It may be dangerous to throw mistakes here.
        if unsafe { &mut *state_buffer }.destroyed_thread_local_macro {
            let used = GLOBAL_MEM_STAT.used.fetch_add(size, Ordering::Relaxed);
            GLOBAL_MEM_STAT
                .peak_used
                .fetch_max(used + size, Ordering::Relaxed);
            return Ok(());
        }

        let has_oom = match unsafe { &mut *state_buffer }.incr(size) <= MEM_STAT_BUFFER_SIZE {
            true => Ok(()),
            false => unsafe { &mut *state_buffer }.flush::<true>(),
        };

        if let Err(out_of_limit) = has_oom {
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
        let state_buffer = unsafe { addr_of_mut!(STAT_BUFFER) };

        // Rust will alloc or dealloc memory after the thread local is destroyed when we using thread_local macro.
        if unsafe { &mut *state_buffer }.destroyed_thread_local_macro {
            GLOBAL_MEM_STAT.used.fetch_add(-size, Ordering::Relaxed);
            return;
        }

        if unsafe { &mut *state_buffer }.incr(-size) < -MEM_STAT_BUFFER_SIZE {
            let _ = unsafe { &mut *state_buffer }.flush::<false>();
        }

        // NOTE: De-allocation does not panic
        // even when it's possible exceeding the limit
        // due to other threads sharing the same MemStat may have allocated a lot of memory.
    }
}

/// Buffering memory allocation stats.
///
/// A StatBuffer buffers stats changes in local variables, and periodically flush them to other storage such as an `Arc<T>` shared by several threads.
#[derive(Clone, Debug, Default)]
pub struct StatBuffer {
    memory_usage: i64,
    // Whether to allow unlimited memory. Alloc memory will not panic if it is true.
    unlimited_flag: bool,
    destroyed_thread_local_macro: bool,
}

impl StatBuffer {
    pub const fn empty() -> Self {
        Self {
            memory_usage: 0,
            unlimited_flag: false,
            destroyed_thread_local_macro: false,
        }
    }

    pub fn incr(&mut self, bs: i64) -> i64 {
        self.memory_usage += bs;
        self.memory_usage
    }

    /// Flush buffered stat to MemStat it belongs to.
    pub fn flush<const NEED_ROLLBACK: bool>(&mut self) -> Result<(), OutOfLimit> {
        let memory_usage = take(&mut self.memory_usage);
        let has_thread_local = TRACKER.try_with(|tracker: &RefCell<ThreadTracker>| {
            // We need to ensure no heap memory alloc or dealloc. it will cause panic of borrow recursive call.
            MemStat::record_memory::<NEED_ROLLBACK>(&tracker.borrow().mem_stat, memory_usage)
        });

        match has_thread_local {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(oom)) => Err(oom),
            Err(_access_error) => MemStat::record_memory::<NEED_ROLLBACK>(&None, memory_usage),
        }
    }
}

/// Memory allocation stat.
///
/// - A MemStat have child MemStat.
/// - Every stat that is fed to a child is also fed to its parent.
/// - A MemStat has at most one parent.
pub struct MemStat {
    name: Option<String>,

    used: AtomicI64,

    peak_used: AtomicI64,

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
            used: AtomicI64::new(0),
            limit: AtomicI64::new(0),
            peak_used: AtomicI64::new(0),
            parent_memory_stat: None,
        }
    }

    pub fn create(name: String) -> Arc<MemStat> {
        let parent = MemStat::current();
        MemStat::create_child(name, parent)
    }

    pub fn create_child(name: String, parent_memory_stat: Option<Arc<MemStat>>) -> Arc<MemStat> {
        Arc::new(MemStat {
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
        mem_stat: &Option<Arc<MemStat>>,
        memory_usage: i64,
    ) -> Result<(), OutOfLimit> {
        let mut is_root = false;

        let mem_stat = match mem_stat {
            Some(x) => x,
            None => {
                // No parent, report to GLOBAL_TRACKER
                is_root = true;
                &GLOBAL_MEM_STAT
            }
        };

        let mut used = mem_stat.used.fetch_add(memory_usage, Ordering::Relaxed);

        used += memory_usage;
        mem_stat.peak_used.fetch_max(used, Ordering::Relaxed);

        if !is_root {
            if let Err(cause) =
                Self::record_memory::<NEED_ROLLBACK>(&mem_stat.parent_memory_stat, memory_usage)
            {
                if NEED_ROLLBACK {
                    let used = mem_stat.used.fetch_sub(memory_usage, Ordering::Relaxed);
                    mem_stat
                        .peak_used
                        .fetch_max(used - memory_usage, Ordering::Relaxed);
                }

                return Err(cause);
            }
        }

        if let Err(cause) = mem_stat.check_limit(used) {
            if NEED_ROLLBACK {
                let used = mem_stat.used.fetch_sub(memory_usage, Ordering::Relaxed);
                mem_stat
                    .peak_used
                    .fetch_max(used - memory_usage, Ordering::Relaxed);
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
    pub fn current() -> Option<Arc<MemStat>> {
        TRACKER.with(|f: &RefCell<ThreadTracker>| f.borrow().mem_stat.clone())
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

    pub fn on_start_thread(self: &Arc<Self>) -> impl Fn() {
        let mem_stat = self.clone();

        move || {
            let s = ThreadTracker::replace_mem_stat(Some(mem_stat.clone()));

            debug_assert!(s.is_none(), "a new thread must have no tracker");
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
        Self::create_with_mem_stat(MemStat::current(), inner)
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

        use crate::runtime::runtime_tracker::STAT_BUFFER;
        use crate::runtime::MemStat;
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

            unsafe {
                let _ = STAT_BUFFER.flush::<false>();
            }

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

        use crate::runtime::MemStat;
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
                    let h = tokio::spawn(async_backtrace::location!().frame(f));
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
