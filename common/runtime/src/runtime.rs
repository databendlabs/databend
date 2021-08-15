// Copyright 2020 Datafuse Labs.
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
use std::thread;

use common_exception::ErrorCode;
use common_exception::Result;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// Tokio Runtime wrapper.
/// If a runtime is in an asynchronous context, shutdown it first.
pub struct Runtime {
    // Handle to runtime.
    handle: Handle,
    // Use to receive a drop signal when dropper is dropped.
    _dropper: Dropper,
}

impl Runtime {
    fn create(builder: &mut tokio::runtime::Builder) -> Result<Self> {
        let runtime = builder
            .build()
            .map_err(|tokio_error| ErrorCode::TokioError(format!("{}", tokio_error)))?;

        let (send_stop, recv_stop) = oneshot::channel();

        let handle = runtime.handle().clone();

        // Block the runtime to shutdown.
        let _ = thread::spawn(move || runtime.block_on(recv_stop));

        Ok(Runtime {
            handle,
            _dropper: Dropper {
                close: Some(send_stop),
            },
        })
    }

    /// Spawns a new tokio runtime with a default thread count on a background
    /// thread and returns a `Handle` which can be used to spawn tasks via
    /// its executor.
    pub fn with_default_worker_threads() -> Result<Self> {
        let mut runtime = tokio::runtime::Builder::new_multi_thread();
        let builder = runtime.enable_all();
        Self::create(builder)
    }

    pub fn with_worker_threads(workers: usize) -> Result<Self> {
        let mut runtime = tokio::runtime::Builder::new_multi_thread();
        let builder = runtime.enable_all().worker_threads(workers);
        Self::create(builder)
    }

    /// Spawns a new asynchronous task, returning a tokio::JoinHandle for it.
    /// Same as tokio::runtime.spawn.
    pub fn spawn<T>(&self, task: T) -> JoinHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.handle.spawn(task)
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
