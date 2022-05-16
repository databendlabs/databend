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
use std::sync::Arc;
use std::thread;

use common_exception::ErrorCode;
use common_exception::Result;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::runtime_tracker::RuntimeTracker;

/// Methods to spawn tasks.
pub trait TrySpawn {
    /// Tries to spawn a new asynchronous task, returning a tokio::JoinHandle for it.
    ///
    /// It allows to return an error before spawning the task.
    fn try_spawn<T>(&self, task: T) -> Result<JoinHandle<T::Output>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static;

    /// Spawns a new asynchronous task, returning a tokio::JoinHandle for it.
    ///
    /// A default impl of this method just calls `try_spawn` and just panics if there is an error.
    fn spawn<T>(&self, task: T) -> JoinHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.try_spawn(task).unwrap()
    }
}

impl<S: TrySpawn> TrySpawn for Arc<S> {
    fn try_spawn<T>(&self, task: T) -> Result<JoinHandle<T::Output>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.as_ref().try_spawn(task)
    }

    fn spawn<T>(&self, task: T) -> JoinHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.as_ref().spawn(task)
    }
}

/// Tokio Runtime wrapper.
/// If a runtime is in an asynchronous context, shutdown it first.
pub struct Runtime {
    // Handle to runtime.
    handle: Handle,
    // Runtime tracker
    tracker: Arc<RuntimeTracker>,
    // Use to receive a drop signal when dropper is dropped.
    _dropper: Dropper,
}

impl Runtime {
    fn create(tracker: Arc<RuntimeTracker>, builder: &mut tokio::runtime::Builder) -> Result<Self> {
        let runtime = builder
            .build()
            .map_err(|tokio_error| ErrorCode::TokioError(tokio_error.to_string()))?;

        let (send_stop, recv_stop) = oneshot::channel();

        let handle = runtime.handle().clone();

        // Block the runtime to shutdown.
        let _ = thread::spawn(move || runtime.block_on(recv_stop));

        Ok(Runtime {
            handle,
            tracker,
            _dropper: Dropper {
                close: Some(send_stop),
            },
        })
    }

    fn tracker_builder(rt_tracker: Arc<RuntimeTracker>) -> tokio::runtime::Builder {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder
            .enable_all()
            .on_thread_stop(rt_tracker.on_stop_thread())
            .on_thread_start(rt_tracker.on_start_thread());

        builder
    }

    pub fn get_tracker(&self) -> Arc<RuntimeTracker> {
        self.tracker.clone()
    }

    /// Spawns a new tokio runtime with a default thread count on a background
    /// thread and returns a `Handle` which can be used to spawn tasks via
    /// its executor.
    pub fn with_default_worker_threads() -> Result<Self> {
        let tracker = RuntimeTracker::create();
        let mut runtime_builder = Self::tracker_builder(tracker.clone());
        Self::create(tracker, &mut runtime_builder)
    }

    pub fn with_worker_threads(workers: usize, thread_name: Option<String>) -> Result<Self> {
        let tracker = RuntimeTracker::create();
        let mut runtime_builder = Self::tracker_builder(tracker.clone());
        if let Some(v) = thread_name {
            runtime_builder.thread_name(v);
        }
        Self::create(tracker, runtime_builder.worker_threads(workers))
    }

    pub fn inner(&self) -> tokio::runtime::Handle {
        self.handle.clone()
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.handle.block_on(future)
    }
}

impl TrySpawn for Runtime {
    fn try_spawn<T>(&self, task: T) -> Result<JoinHandle<T::Output>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        Ok(self.handle.spawn(task))
    }
}

/// Dropping the dropper will cause runtime to shutdown.
pub struct Dropper {
    close: Option<oneshot::Sender<()>>,
}

impl Drop for Dropper {
    fn drop(&mut self) {
        // Send a signal to say i am dropping.
        self.close.take().map(|v| v.send(()));
    }
}
