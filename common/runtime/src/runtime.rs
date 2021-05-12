// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::future::Future;
use std::sync::mpsc;
use std::thread;

use common_exception::ErrorCodes;
use common_exception::Result;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// Tokio Runtime wrapper.
/// If a runtime is in an asynchronous context, we will the shutdown it first.
#[allow(dead_code)]
pub struct Runtime {
    // Use to receive a drop signal when dropper is dropped.
    dropper: Dropper,
    // Handle to runtime.
    handle: Handle
}

impl Runtime {
    fn create(builder: &mut tokio::runtime::Builder) -> Result<Self> {
        let runtime = builder
            .build()
            .map_err(|tokio_error| ErrorCodes::TokioError(format!("{}", tokio_error)))?;

        let (send_stop, recv_stop) = oneshot::channel();
        let (tx, rx) = mpsc::channel();

        // Block the runtime to shutdown.
        let _ = thread::spawn(move || {
            tx.send(runtime.handle().clone())
                .expect("Rx is blocking upper thread.");
            runtime.block_on(async {
                tokio::select! {
                _ = recv_stop => {},
                }
            })
        });

        let handle = rx
            .recv()
            .map_err(|e| ErrorCodes::UnknownException(format!("{}", e)))?;

        Ok(Runtime {
            handle,
            dropper: Dropper {
                close: Some(send_stop)
            }
        })
    }

    /// Spawns a new tokio runtime with a default thread count on a background
    /// thread and returns a `Handle` which can be used to spawn tasks via
    /// its executor.
    pub fn with_default_worker_threads() -> Result<Self> {
        let mut runtime = tokio::runtime::Builder::new_multi_thread();
        let builder = runtime.enable_io().enable_time();
        Self::create(builder)
    }

    pub fn with_worker_threads(workers: usize) -> Result<Self> {
        let mut runtime = tokio::runtime::Builder::new_multi_thread();
        let builder = runtime.worker_threads(workers).enable_io().enable_time();
        Self::create(builder)
    }

    /// Spawns a new asynchronous task, returning a tokio::JoinHandle for it.
    /// Same as tokio::runtime.spawn.
    pub fn spawn<T>(&self, task: T) -> JoinHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static
    {
        self.handle.spawn(task)
    }

    /// Run a future to completion on this `Handle`'s associated `Runtime`.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.handle.block_on(future)
    }
}

/// Dropping the dropper will cause runtime to shutdown.
pub struct Dropper {
    close: Option<oneshot::Sender<()>>
}

impl Drop for Dropper {
    fn drop(&mut self) {
        // Send a signal to say i am dropping.
        self.close.take().map(|v| v.send(()));
    }
}
