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
use std::sync::atomic::AtomicUsize;
use std::task::Context;
use std::task::Poll;

use concurrent_queue::ConcurrentQueue;
use log::LevelFilter;
use pin_project_lite::pin_project;

use crate::runtime::MemStatBuffer;
use crate::runtime::OutOfLimit;
use crate::runtime::QueryPerf;
use crate::runtime::TimeSeriesProfiles;
use crate::runtime::memory::GlobalStatBuffer;
use crate::runtime::memory::MemStat;
use crate::runtime::metrics::ScopedRegistry;
use crate::runtime::profile::Profile;
use crate::runtime::time_series::QueryTimeSeriesProfile;
use crate::runtime::workload_group::WorkloadGroupResource;

// For implemented and needs to call drop, we cannot use the attribute tag thread local.
// https://play.rust-lang.org/?version=nightly&mode=debug&edition=2021&gist=ea33533387d401e86423df1a764b5609
thread_local! {
    static TRACKER: RefCell<ThreadTracker> = RefCell::new(ThreadTracker::empty());
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

    pub fn is_unlimited() -> bool {
        GlobalStatBuffer::current().is_unlimited() || MemStatBuffer::current().unlimited_flag
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
    pub(crate) payload: Arc<TrackingPayload>,
}

pub struct CaptureLogSettings {
    pub level: LevelFilter,
    pub queue: Option<Arc<ConcurrentQueue<String>>>,
}

impl CaptureLogSettings {
    pub fn capture_off() -> Arc<CaptureLogSettings> {
        Arc::new(CaptureLogSettings {
            level: LevelFilter::Off,
            queue: None,
        })
    }

    pub fn capture_query(
        level: LevelFilter,
        queue: Arc<ConcurrentQueue<String>>,
    ) -> Arc<CaptureLogSettings> {
        Arc::new(CaptureLogSettings {
            level,
            queue: Some(queue),
        })
    }
}

pub struct TrackingPayload {
    pub query_id: Option<String>,
    pub profile: Option<Arc<Profile>>,
    pub mem_stat: Option<Arc<MemStat>>,
    pub metrics: Option<Arc<ScopedRegistry>>,
    pub capture_log_settings: Option<Arc<CaptureLogSettings>>,
    pub time_series_profile: Option<Arc<QueryTimeSeriesProfile>>,
    pub local_time_series_profile: Option<Arc<TimeSeriesProfiles>>,
    pub workload_group_resource: Option<Arc<WorkloadGroupResource>>,
    pub perf_enabled: bool,
    pub process_rows: AtomicUsize,
}

pub trait TrackingPayloadExt {
    fn tracking<F>(self, future: F) -> TrackingFuture<F>
    where F: Future;
}

impl TrackingPayloadExt for Option<Arc<TrackingPayload>> {
    fn tracking<F>(self, future: F) -> TrackingFuture<F>
    where F: Future {
        ThreadTracker::tracking_future_with_payload(future, self)
    }
}

impl TrackingPayloadExt for Arc<TrackingPayload> {
    fn tracking<F>(self, future: F) -> TrackingFuture<F>
    where F: Future {
        ThreadTracker::tracking_future_with_payload(future, Some(self))
    }
}

impl TrackingPayloadExt for TrackingPayload {
    fn tracking<F>(self, future: F) -> TrackingFuture<F>
    where F: Future {
        ThreadTracker::tracking_future_with_payload(future, Some(Arc::new(self)))
    }
}

impl TrackingPayloadExt for Option<TrackingPayload> {
    fn tracking<F>(self, future: F) -> TrackingFuture<F>
    where F: Future {
        ThreadTracker::tracking_future_with_payload(future, self.map(Arc::new))
    }
}

impl TrackingPayload {}

impl Clone for TrackingPayload {
    fn clone(&self) -> Self {
        TrackingPayload {
            query_id: self.query_id.clone(),
            profile: self.profile.clone(),
            mem_stat: self.mem_stat.clone(),
            metrics: self.metrics.clone(),
            capture_log_settings: self.capture_log_settings.clone(),
            time_series_profile: self.time_series_profile.clone(),
            local_time_series_profile: self.local_time_series_profile.clone(),
            workload_group_resource: self.workload_group_resource.clone(),
            perf_enabled: self.perf_enabled,
            process_rows: AtomicUsize::new(
                self.process_rows.load(std::sync::atomic::Ordering::SeqCst),
            ),
        }
    }
}

pub struct TrackingGuard {
    saved: Arc<TrackingPayload>,
}

impl TrackingGuard {
    pub fn flush(&self) -> Result<(), OutOfLimit> {
        if let Err(out_of_memory) = MemStatBuffer::current().flush::<false>(0) {
            let _ = GlobalStatBuffer::current().flush::<false>(0);
            return Err(out_of_memory);
        }

        GlobalStatBuffer::current().flush::<false>(0)
    }
}

impl Drop for TrackingGuard {
    fn drop(&mut self) {
        let _ = GlobalStatBuffer::current().flush::<false>(0);

        TRACKER.with(|x| {
            let mut thread_tracker = x.borrow_mut();
            std::mem::swap(&mut thread_tracker.payload, &mut self.saved);

            // Sync perf flag when restoring the previous payload
            QueryPerf::sync_from_payload(thread_tracker.payload.perf_enabled);
        });
    }
}

pub struct TrackingFuture<T: Future> {
    inner: Pin<Box<T>>,
    tracking_payload: Arc<TrackingPayload>,
}

impl<T: Future> TrackingFuture<T> {
    pub fn create(inner: T, tracking_payload: Arc<TrackingPayload>) -> TrackingFuture<T> {
        TrackingFuture {
            inner: Box::pin(inner),
            tracking_payload,
        }
    }
}

impl<T: Future> Future for TrackingFuture<T> {
    type Output = T::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _guard = ThreadTracker::tracking_inner(self.tracking_payload.clone());
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
    pub(crate) fn empty() -> Self {
        Self {
            out_of_limit_desc: None,
            payload: Arc::new(TrackingPayload {
                profile: None,
                metrics: None,
                mem_stat: None,
                query_id: None,
                capture_log_settings: None,
                time_series_profile: None,
                local_time_series_profile: None,
                workload_group_resource: None,
                perf_enabled: false,
                process_rows: AtomicUsize::new(0),
            }),
        }
    }

    // rust style thread local is always lazy init.
    // need to be called immediately after the threads start
    pub fn init() {
        TRACKER.with(|x| {
            let _ = x.borrow_mut();
        });
    }

    pub(crate) fn with<F, R>(f: F) -> R
    where F: FnOnce(&RefCell<ThreadTracker>) -> R {
        TRACKER.with(f)
    }

    pub fn tracking_inner(tracking_payload: Arc<TrackingPayload>) -> TrackingGuard {
        let mut guard = TrackingGuard {
            saved: tracking_payload,
        };

        let _guard = LimitMemGuard::enter_unlimited();
        let _ = MemStatBuffer::current().flush::<false>(0);
        let _ = GlobalStatBuffer::current().flush::<false>(0);

        TRACKER.with(move |x| {
            let mut thread_tracker = x.borrow_mut();
            std::mem::swap(&mut thread_tracker.payload, &mut guard.saved);

            // Sync perf flag from the new payload to thread_local PERF_FLAG
            QueryPerf::sync_from_payload(thread_tracker.payload.perf_enabled);

            guard
        })
    }

    pub fn tracking(tracking_payload: TrackingPayload) -> TrackingGuard {
        Self::tracking_inner(Arc::new(tracking_payload))
    }

    pub fn tracking_future<T: Future>(future: T) -> TrackingFuture<T> {
        TRACKER.with(move |x| TrackingFuture::create(future, x.borrow().payload.clone()))
    }

    pub fn tracking_future_with_payload<T: Future>(
        future: T,
        payload: Option<Arc<TrackingPayload>>,
    ) -> TrackingFuture<T> {
        TRACKER.with(move |x| {
            TrackingFuture::create(
                future,
                payload.unwrap_or_else(|| x.borrow().payload.clone()),
            )
        })
    }

    pub fn tracking_function<F, T>(f: F) -> impl FnOnce() -> T
    where F: FnOnce() -> T {
        TRACKER.with(move |x| {
            let payload = x.borrow().payload.clone();
            move || {
                let _guard = ThreadTracker::tracking_inner(payload);
                f()
            }
        })
    }

    pub fn new_tracking_payload() -> TrackingPayload {
        TRACKER.with(|x| x.borrow().payload.as_ref().clone())
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

    pub fn workload_group() -> Option<&'static Arc<WorkloadGroupResource>> {
        TRACKER
            .try_with(|tracker| {
                let tracker = tracker.borrow();
                unsafe { std::mem::transmute(tracker.payload.workload_group_resource.as_ref()) }
            })
            .unwrap_or(None)
    }

    pub fn query_id() -> Option<&'static String> {
        TRACKER
            .try_with(|tracker| {
                tracker
                    .borrow()
                    .payload
                    .query_id
                    .as_ref()
                    .map(|query_id| unsafe { &*(query_id as *const String) })
            })
            .unwrap_or(None)
    }

    pub fn capture_log_settings() -> Option<&'static Arc<CaptureLogSettings>> {
        TRACKER
            .try_with(|tracker| {
                tracker
                    .borrow()
                    .payload
                    .capture_log_settings
                    .as_ref()
                    .map(|v| unsafe { std::mem::transmute(v) })
            })
            .ok()
            .and_then(|x| x)
    }

    pub fn process_rows() -> usize {
        TRACKER
            .try_with(|tracker| {
                tracker
                    .borrow()
                    .payload
                    .process_rows
                    .load(std::sync::atomic::Ordering::SeqCst)
            })
            .unwrap_or(0)
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
