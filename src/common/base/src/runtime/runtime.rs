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

use std::backtrace::Backtrace;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ResultExt;
use futures::future;
use log::warn;
use tokio::runtime::Builder;
use tokio::runtime::Handle;
use tokio::runtime::Id;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use super::watchdog::WatchdogEvent;
use super::watchdog::run_watchdog_loop;
use crate::runtime::Thread;
use crate::runtime::ThreadJoinHandle;
use crate::runtime::ThreadTracker;
use crate::runtime::catch_unwind::CatchUnwindFuture;
use crate::runtime::drop_guard;

/// Tokio Runtime wrapper.
/// If a runtime is in an asynchronous context, shutdown it first.
pub struct Runtime {
    /// Runtime handle.
    handle: Handle,

    /// Prefix injected into task frames and used to filter task dumps.
    task_marker: String,

    /// Use to receive a drop signal when dropper is dropped.
    _dropper: Dropper,
}

impl Runtime {
    fn create(name: Option<String>, builder: &mut Builder) -> Result<Self> {
        let runtime = builder
            .build()
            .map_err(|tokio_error| ErrorCode::TokioError(tokio_error.to_string()))?;

        let (watchdog_tx, watchdog_rx) = std::sync::mpsc::channel::<WatchdogEvent>();

        let handle = runtime.handle().clone();
        let runtime_name = name.clone().unwrap_or_else(|| "unnamed".to_string());
        let runtime_id = handle.id();
        let runtime_label = format!("{runtime_name} id={runtime_id}");
        let task_marker = format!("[{runtime_label}]");

        let n = name.clone();
        let watchdog_handle = handle.clone();
        let watchdog_marker = task_marker.clone();
        let watchdog_event_tx = watchdog_tx.clone();
        // The wait-to-drop thread blocks until the runtime is dropped, and in
        // the meantime periodically probes the runtime to check that tasks are
        // still being scheduled in a timely manner.
        let join_handler =
            Thread::named_spawn(n.as_ref().map(|n| format!("wait-to-drop-{n}")), move || {
                run_watchdog_loop(
                    &watchdog_handle,
                    &watchdog_marker,
                    &watchdog_event_tx,
                    &watchdog_rx,
                );

                if cfg!(debug_assertions) {
                    let instant = Instant::now();
                    // In debug builds, bound the Tokio runtime shutdown wait to 3 seconds.
                    // The dropper joining this wait-to-drop thread is still an unbounded wait.
                    runtime.shutdown_timeout(Duration::from_secs(3));
                    instant.elapsed() >= Duration::from_secs(3)
                } else {
                    false
                }
            });

        Ok(Runtime {
            handle,
            task_marker,
            _dropper: Dropper {
                name,
                runtime_id,
                close: Some(watchdog_tx),
                join_handler: Some(join_handler),
            },
        })
    }

    /// Spawns a new tokio runtime with a default thread count on a background
    /// thread and returns a `Handle` which can be used to spawn tasks via
    /// its executor.
    pub fn with_default_worker_threads(thread_name: Option<String>) -> Result<Self> {
        Self::create_runtime(None, thread_name)
    }

    pub fn with_worker_threads(workers: usize, thread_name: Option<String>) -> Result<Self> {
        Self::create_runtime(Some(workers), thread_name)
    }

    /// Creates a new multi-thread runtime with optional worker count and thread name.
    ///
    /// In debug builds with UNIT_TEST=TRUE, overrides thread_name with current thread name
    /// and sets a larger stack size (20MB) for debugging.
    #[allow(unused_mut)]
    fn create_runtime(workers: Option<usize>, mut thread_name: Option<String>) -> Result<Self> {
        let mut builder = Builder::new_multi_thread();

        #[cfg(debug_assertions)]
        {
            // In unit tests, use the test thread name for better debugging
            if matches!(std::env::var("UNIT_TEST"), Ok(v) if v == "TRUE")
                && let Some(name) = std::thread::current().name()
            {
                thread_name = Some(name.to_string());
            }
            builder.thread_stack_size(20 * 1024 * 1024);
        }

        if let Some(ref name) = thread_name {
            builder.name(name.clone());
            builder.thread_name(name.clone());
        }

        if let Some(n) = workers {
            builder.worker_threads(n);
        }

        Self::create(
            thread_name,
            builder.enable_all().on_thread_start(ThreadTracker::init),
        )
    }

    pub fn inner(&self) -> tokio::runtime::Handle {
        self.handle.clone()
    }

    /// Returns whether the caller is running in this runtime.
    #[inline]
    pub fn is_current(&self) -> bool {
        is_current_runtime(self.handle.id())
    }

    fn task_location_name(&self, location_name: String) -> String {
        format!("{} {}", self.task_marker, location_name)
    }

    #[track_caller]
    pub fn block_on<T, C, F>(&self, future: F) -> F::Output
    where F: Future<Output = Result<T, C>> {
        // Call location_future before closure since #[track_caller] doesn't propagate through closures
        let future = location_future(CatchUnwindFuture::create(future), None);
        #[allow(clippy::disallowed_methods)]
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(future)
                .with_context(|| "failed to block on future".to_string())
                .flatten()
        })
    }

    // For each future of `futures`, before being executed
    // a permit will be acquired from the semaphore, and released when it is done
    pub async fn try_spawn_batch<Fut>(
        &self,
        semaphore: Semaphore,
        futures: impl IntoIterator<Item = Fut>,
    ) -> Result<Vec<JoinHandle<Fut::Output>>>
    where
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let semaphore = Arc::new(semaphore);
        let iter = futures.into_iter().map(|v| {
            |permit| async {
                let r = v.await;
                drop(permit);
                r
            }
        });
        self.try_spawn_batch_with_owned_semaphore(semaphore, iter)
            .await
    }

    // For each future of `futures`, before being executed
    // a permit will be acquired from the semaphore, and released when it is done

    // Please take care using the `semaphore`.
    // If sub task may be spawned in the `futures`, and uses the
    // clone of semaphore to acquire permits, please release the permits on time,
    // or give sufficient(but not abundant, of course) permits, to tolerant the
    // maximum degree of parallelism, otherwise, it may lead to deadlock.
    pub async fn try_spawn_batch_with_owned_semaphore<F, Fut>(
        &self,
        semaphore: Arc<Semaphore>,
        futures: impl IntoIterator<Item = F>,
    ) -> Result<Vec<JoinHandle<Fut::Output>>>
    where
        F: FnOnce(OwnedSemaphorePermit) -> Fut + Send + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let iter = futures.into_iter();
        let (lower, upper) = iter.size_hint();
        let mut handlers = Vec::with_capacity(upper.unwrap_or(lower));
        for fut in iter {
            let semaphore = semaphore.clone();
            // Although async task is rather lightweight, it do consumes resources,
            // so we acquire a permit BEFORE spawn.
            // Thus, the `futures` passed into this method is NOT suggested to be "materialized"
            // iterator, e.g. Vec<..>
            let permit = semaphore.acquire_owned().await.map_err(|e| {
                ErrorCode::Internal(format!("semaphore closed, acquire permit failure. {}", e))
            })?;
            let task_location_name =
                self.task_location_name("Runtime::try_spawn_batch task".to_string());
            #[expect(clippy::disallowed_methods)]
            let handler = self.handle.spawn(ThreadTracker::tracking_future(
                async_backtrace::location!(task_location_name).frame(async move {
                    // take the ownership of the permit, (implicitly) drop it when task is done
                    fut(permit).await
                }),
            ));
            handlers.push(handler)
        }

        Ok(handlers)
    }

    // TODO(Winter): remove
    // Please do not use this method(it's temporary)
    #[async_backtrace::framed]
    pub async fn spawn_blocking<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce() -> Result<R> + Send + 'static,
        R: Send + 'static,
    {
        #[allow(clippy::disallowed_methods)]
        self.handle
            .spawn_blocking(ThreadTracker::tracking_function(f))
            .await?
    }
}

impl Runtime {
    /// Spawns a new asynchronous task with a specific name, returning a JoinHandle for it.
    #[track_caller]
    pub fn spawn_named<T>(&self, task: T, name: impl Into<String>) -> JoinHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.spawn_with_name(task, Some(name.into()))
    }

    /// Spawns a new asynchronous task, returning a JoinHandle for it.
    #[track_caller]
    pub fn spawn<T>(&self, task: T) -> JoinHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.spawn_with_name(task, None)
    }

    /// Spawns a new asynchronous task with an optional name, returning a JoinHandle for it.
    ///
    /// If `name` is `None`, generates a default name based on the current query context.
    #[track_caller]
    fn spawn_with_name<T>(&self, task: T, name: Option<String>) -> JoinHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let location_name = name.unwrap_or_else(|| match ThreadTracker::query_id() {
            None => String::from(GLOBAL_TASK_DESC),
            Some(query_id) => format!("Running query {} spawn task", query_id),
        });

        let task = location_future(task, Some(self.task_location_name(location_name)));

        #[expect(clippy::disallowed_methods)]
        self.handle.spawn(task)
    }
}

/// Dropping the dropper will cause runtime to shutdown.
pub struct Dropper {
    name: Option<String>,
    runtime_id: Id,
    close: Option<std::sync::mpsc::Sender<WatchdogEvent>>,
    join_handler: Option<ThreadJoinHandle<bool>>,
}

impl Drop for Dropper {
    fn drop(&mut self) {
        drop_guard(move || {
            // Send a signal to say i am dropping.
            if let Some(close_sender) = self.close.take()
                && close_sender.send(WatchdogEvent::Stop).is_ok()
            {
                if is_current_runtime(self.runtime_id) {
                    // The wait-to-drop thread owns the Tokio runtime and will shut it down
                    // after observing the stop signal. Joining it from one of the same
                    // runtime's workers would deadlock: shutdown waits for this worker to
                    // exit, while this worker waits for shutdown to finish.
                    drop(self.join_handler.take());
                    return;
                }

                match self.join_handler.take().unwrap().join() {
                    Err(e) => warn!("Runtime dropper panic, {:?}", e),
                    Ok(true) => {
                        // If the debug shutdown timeout is fully consumed, log the
                        // drop-site backtrace to help diagnose blocked runtime tasks.
                        warn!(
                            "Runtime dropper is blocked 3 seconds, runtime name: {:?}, drop backtrace: {:?}",
                            self.name,
                            Backtrace::capture()
                        );
                    }
                    _ => {}
                };
            }
        })
    }
}

#[inline]
fn is_current_runtime(runtime_id: Id) -> bool {
    match Handle::try_current() {
        Ok(handle) => handle.id() == runtime_id,
        Err(_) => false,
    }
}

/// Run multiple futures parallel
/// using a semaphore to limit the parallelism number, and a specified thread pool to run the futures.
/// It waits for all futures to complete and returns their results.
pub async fn execute_futures_in_parallel<Fut>(
    futures: impl IntoIterator<Item = Fut>,
    thread_nums: usize,
    permit_nums: usize,
    thread_name: String,
) -> Result<Vec<Fut::Output>>
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    // 1. build the runtime.
    let semaphore = Semaphore::new(permit_nums);
    let runtime = Arc::new(Runtime::with_worker_threads(
        thread_nums,
        Some(thread_name),
    )?);

    // 2. spawn all the tasks to the runtime with semaphore.
    let join_handlers = runtime.try_spawn_batch(semaphore, futures).await?;

    // 3. get all the result.
    Ok(future::try_join_all(join_handlers).await?)
}

pub const GLOBAL_TASK: &str = "Zxv39PlwG1ahbF0APRUf03";
pub const GLOBAL_TASK_DESC: &str = "Global spawn task";

#[track_caller]
pub fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let name = current_runtime_location_name::<F>(None);
    #[expect(clippy::disallowed_methods)]
    tokio::spawn(location_future(future, Some(name)))
}

#[track_caller]
pub fn spawn_named<F>(future: F, name: String) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let name = current_runtime_location_name::<F>(Some(name));
    #[expect(clippy::disallowed_methods)]
    tokio::spawn(location_future(future, Some(name)))
}

#[track_caller]
pub fn spawn_local<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let name = current_runtime_location_name::<F>(None);
    #[expect(clippy::disallowed_methods)]
    tokio::task::spawn_local(location_future(future, Some(name)))
}

#[track_caller]
pub fn spawn_blocking<F, R>(f: F) -> tokio::task::JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    #[expect(clippy::disallowed_methods)]
    tokio::runtime::Handle::current().spawn_blocking(ThreadTracker::tracking_function(f))
}

#[track_caller]
pub fn try_spawn_blocking<F, R>(f: F) -> std::result::Result<tokio::task::JoinHandle<R>, F>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Err(_) => Err(f),
        #[expect(clippy::disallowed_methods)]
        Ok(handler) => Ok(handler.spawn_blocking(ThreadTracker::tracking_function(f))),
    }
}

#[track_caller]
pub fn block_on<F: Future>(future: F) -> F::Output {
    // Call location_future before closure since #[track_caller] doesn't propagate through closures
    let future = location_future(future, None);
    #[expect(clippy::disallowed_methods)]
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(future))
}

#[track_caller]
pub fn block_on_with_handle<F: Future>(handle: &Handle, future: F) -> F::Output {
    // Call location_future before closure since #[track_caller] doesn't propagate through closures
    let future = location_future(future, None);
    #[expect(clippy::disallowed_methods)]
    tokio::task::block_in_place(|| handle.block_on(future))
}

#[track_caller]
pub fn try_block_on<F: Future>(future: F) -> std::result::Result<F::Output, F> {
    match tokio::runtime::Handle::try_current() {
        Err(_) => Err(future),
        Ok(handler) => {
            // Call location_future before closure since #[track_caller] doesn't propagate through closures
            let future = location_future(future, None);
            #[expect(clippy::disallowed_methods)]
            Ok(tokio::task::block_in_place(|| handler.block_on(future)))
        }
    }
}

#[track_caller]
fn location_future<F>(
    future: F,
    frame_name: Option<String>,
) -> impl Future<Output = F::Output> + use<F>
where
    F: Future,
{
    let frame_location = std::panic::Location::caller();

    // NOTE:
    // Frame name: https://play.rust-lang.org/?version=nightly&mode=debug&edition=2021&gist=689fbc84ab4be894c0cdd285bea24845
    // Frame location: https://play.rust-lang.org/?version=nightly&mode=debug&edition=2021&gist=3ae3a2295607628ce95f0a34a566847b

    // TODO: tracking payload
    let future = ThreadTracker::tracking_future(future);

    let frame_name = frame_name.unwrap_or_else(|| {
        std::any::type_name::<F>()
            .trim_end_matches("::{{closure}}")
            .to_string()
    });

    async_backtrace::location!(
        frame_name,
        frame_location.file(),
        frame_location.line(),
        frame_location.column()
    )
    .frame(future)
}

fn current_runtime_location_name<F>(frame_name: Option<String>) -> String
where F: Future {
    let frame_name = frame_name.unwrap_or_else(|| {
        std::any::type_name::<F>()
            .trim_end_matches("::{{closure}}")
            .to_string()
    });

    if let Ok(handle) = Handle::try_current() {
        return format!("{} {frame_name}", current_runtime_marker(&handle));
    }

    frame_name
}

fn current_runtime_marker(handle: &Handle) -> String {
    let runtime_name = handle.name().unwrap_or("unnamed");
    format!("[{} id={}]", runtime_name, handle.id())
}
