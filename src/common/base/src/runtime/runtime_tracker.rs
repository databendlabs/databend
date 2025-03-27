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

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use pin_project_lite::pin_project;

use crate::runtime::memory::GlobalStatBuffer;
use crate::runtime::memory::MemStat;
use crate::runtime::metrics::ScopedRegistry;
use crate::runtime::profile::Profile;
use crate::runtime::MemStatBuffer;

// For implemented and needs to call drop, we cannot use the attribute tag thread local.
// https://play.rust-lang.org/?version=nightly&mode=debug&edition=2021&gist=ea33533387d401e86423df1a764b5609
thread_local! {
    static TRACKER: RefCell<ThreadTracker> = const { RefCell::new(ThreadTracker::empty()) };
}

pub struct LimitMemGuard {
    global_saved: bool,
    mem_stat_saved: bool,
}

impl LimitMemGuard {
    pub fn enter_unlimited() -> Self {
        Self {
            global_saved: GlobalStatBuffer::current().set_unlimited_flag(true),
            mem_stat_saved: MemStatBuffer::current().set_unlimited_flag(true),
        }
    }

    pub fn enter_limited() -> Self {
        Self {
            global_saved: GlobalStatBuffer::current().set_unlimited_flag(false),
            mem_stat_saved: MemStatBuffer::current().set_unlimited_flag(false),
        }
    }
}

impl Drop for LimitMemGuard {
    fn drop(&mut self) {
        MemStatBuffer::current().set_unlimited_flag(self.mem_stat_saved);
        GlobalStatBuffer::current().set_unlimited_flag(self.global_saved);
    }
}

/// A per-thread tracker that tracks memory usage stat.
pub struct ThreadTracker {
    out_of_limit_desc: Option<String>,
    pub(crate) payload: TrackingPayload,
}

#[derive(Clone)]
pub struct TrackingPayload {
    pub query_id: Option<String>,
    pub profile: Option<Arc<Profile>>,
    pub mem_stat: Option<Arc<MemStat>>,
    pub metrics: Option<Arc<ScopedRegistry>>,
}

pub struct TrackingGuard {
    saved: TrackingPayload,
}

impl Drop for TrackingGuard {
    fn drop(&mut self) {
        let _ = GlobalStatBuffer::current().flush::<false>(0);

        TRACKER.with(|x| {
            let mut thread_tracker = x.borrow_mut();
            std::mem::swap(&mut thread_tracker.payload, &mut self.saved);
        });
    }
}

pub struct TrackingFuture<T: Future> {
    inner: Pin<Box<T>>,
    tracking_payload: TrackingPayload,
}

impl<T: Future> TrackingFuture<T> {
    pub fn create(inner: T, tracking_payload: TrackingPayload) -> TrackingFuture<T> {
        TrackingFuture {
            inner: Box::pin(inner),
            tracking_payload,
        }
    }
}

impl<T: Future> Future for TrackingFuture<T> {
    type Output = T::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _guard = ThreadTracker::tracking(self.tracking_payload.clone());
        self.inner.as_mut().poll(cx)
    }
}

impl Drop for ThreadTracker {
    fn drop(&mut self) {
        MemStatBuffer::current().mark_destroyed();
        GlobalStatBuffer::current().mark_destroyed();
    }
}

/// A memory stat tracker with buffer.
///
/// Every ThreadTracker belongs to one MemStat.
/// A MemStat might receive memory stat from more than one ThreadTracker.
impl ThreadTracker {
    pub(crate) const fn empty() -> Self {
        Self {
            out_of_limit_desc: None,
            payload: TrackingPayload {
                profile: None,
                metrics: None,
                mem_stat: None,
                query_id: None,
            },
        }
    }

    // rust style thread local is always lazy init.
    // need to be called immediately after the threads start
    pub fn init() {
        TRACKER.with(|x| {
            let _ = x.borrow_mut();
        })
    }

    pub(crate) fn with<F, R>(f: F) -> R
    where F: FnOnce(&RefCell<ThreadTracker>) -> R {
        TRACKER.with(f)
    }

    pub fn tracking(tracking_payload: TrackingPayload) -> TrackingGuard {
        let mut guard = TrackingGuard {
            saved: tracking_payload,
        };
        let _ = MemStatBuffer::current().flush::<false>(0);
        let _ = GlobalStatBuffer::current().flush::<false>(0);

        TRACKER.with(move |x| {
            let mut thread_tracker = x.borrow_mut();
            std::mem::swap(&mut thread_tracker.payload, &mut guard.saved);

            guard
        })
    }

    pub fn tracking_future<T: Future>(future: T) -> TrackingFuture<T> {
        TRACKER.with(move |x| TrackingFuture::create(future, x.borrow().payload.clone()))
    }

    pub fn tracking_function<F, T>(f: F) -> impl FnOnce() -> T
    where F: FnOnce() -> T {
        TRACKER.with(move |x| {
            let payload = x.borrow().payload.clone();
            move || {
                let _guard = ThreadTracker::tracking(payload);
                f()
            }
        })
    }

    pub fn new_tracking_payload() -> TrackingPayload {
        TRACKER.with(|x| x.borrow().payload.clone())
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

    pub fn mem_stat() -> Option<&'static Arc<MemStat>> {
        TRACKER
            .try_with(|tracker| {
                let tracker = tracker.borrow();
                unsafe { std::mem::transmute(tracker.payload.mem_stat.as_ref()) }
            })
            .unwrap_or(None)
    }

    pub fn query_id() -> Option<&'static String> {
        TRACKER.with(|tracker| {
            tracker
                .borrow()
                .payload
                .query_id
                .as_ref()
                .map(|query_id| unsafe { &*(query_id as *const String) })
        })
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
