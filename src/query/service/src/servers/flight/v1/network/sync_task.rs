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

//! [`SyncTaskSet`] / [`SyncTaskHandle`] for spawning async futures
//! from synchronous `event()` methods and polling them to completion.

use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use databend_common_pipeline::core::ExecutorWaker;
use futures_util::future::BoxFuture;
use petgraph::prelude::NodeIndex;

/// Synchronous task scheduler. Wraps an `ExecutorWaker` and provides `spawn`
/// to create [`SyncTaskHandle`]s that can be polled from `event()`.
pub struct SyncTaskSet {
    worker_id: usize,
    executor_waker: Arc<ExecutorWaker>,
}

impl SyncTaskSet {
    pub fn new(worker_id: usize, executor_waker: Arc<ExecutorWaker>) -> Self {
        Self {
            worker_id,
            executor_waker,
        }
    }

    /// Spawn an async future, returning a [`SyncTaskHandle`].
    ///
    /// The future is polled once immediately. If it completes, the value
    /// is stored in the handle. Otherwise the future is stored for later polling.
    pub fn spawn<'a, T>(&self, id: NodeIndex, future: BoxFuture<'a, T>) -> SyncTaskHandle<'a, T> {
        let waker = self.executor_waker.to_waker(id, self.worker_id);
        let mut future = future;
        let mut cx = Context::from_waker(&waker);
        match Pin::new(&mut future).poll(&mut cx) {
            Poll::Ready(value) => SyncTaskHandle {
                waker,
                future: None,
                value: Some(value),
            },
            Poll::Pending => SyncTaskHandle {
                waker,
                future: Some(future),
                value: None,
            },
        }
    }
}

/// Handle for a spawned async task. Held as `Option<SyncTaskHandle<T>>` in a
/// processor and polled from `event()`.
pub struct SyncTaskHandle<'a, T> {
    waker: Waker,
    future: Option<BoxFuture<'a, T>>,
    value: Option<T>,
}

impl<'a, T> SyncTaskHandle<'a, T> {
    /// Poll the future.
    ///
    /// - Returns `Poll::Ready(T)` when the future has completed.
    /// - Returns `Poll::Pending` when the future is still in progress.
    ///
    /// Panics if called after the value has already been consumed.
    pub fn poll(&mut self, _reset: bool) -> Poll<T> {
        if let Some(value) = self.value.take() {
            return Poll::Ready(value);
        }

        let Some(future) = &mut self.future else {
            panic!("SyncTaskHandle polled after value was consumed");
        };

        let mut cx = Context::from_waker(&self.waker);
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => {
                self.future = None;
                Poll::Ready(value)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
