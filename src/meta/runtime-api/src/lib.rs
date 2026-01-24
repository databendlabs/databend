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

mod tokio_impl;

use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

pub use tokio::task::JoinHandle;
pub use tokio_impl::TokioRuntime;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub enum TrackingData {
    QueryId(Option<String>),
}

impl TrackingData {
    pub fn new_query_id(id: Option<impl ToString>) -> Self {
        Self::QueryId(id.map(|x| x.to_string()))
    }
}

/// Spawn tasks on the current runtime context.
pub trait SpawnApi: Clone + Debug + Send + Sync + 'static {
    fn spawn<F>(future: F, name: Option<String>) -> JoinHandle<F::Output>
    where
        Self: Sized,
        F: Future + Send + 'static,
        F::Output: Send + 'static;

    fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
    where
        Self: Sized,
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static;

    /// Create a wrapped Future that has bound data.
    fn track_future<'a, T, Fut>(fut: Fut, data: Vec<TrackingData>) -> BoxFuture<'a, T>
    where
        Fut: Future<Output = T> + Send + 'a,
        T: Send + 'a;

    /// Wrap a future to bypass memory limits during execution.
    ///
    /// This is used for futures that should not be subject to memory tracking
    /// or limits, such as critical client operations that must complete.
    fn unlimited_future<'a, T, Fut>(fut: Fut) -> BoxFuture<'a, T>
    where
        Fut: Future<Output = T> + Send + 'a,
        T: Send + 'a;

    /// Inject tracing span context into a tonic request.
    ///
    /// Enables distributed tracing across gRPC calls by propagating
    /// the W3C traceparent header. Returns request unchanged if no
    /// span context is available.
    fn inject_span_to_request<T>(request: tonic::Request<T>) -> tonic::Request<T>
    where Self: Sized;
}

/// Owned runtime instance that can spawn tasks.
pub trait RuntimeApi: SpawnApi {
    /// Create a runtime for unit tests.
    ///
    /// Uses a small worker pool (4 threads) and panics on failure,
    /// which is acceptable in test code.
    fn new_testing(name: impl ToString) -> Self
    where Self: Sized {
        Self::new(Some(4), Some(name.to_string())).unwrap()
    }

    /// Create a runtime for embedded meta service (e.g., `LocalMetaService`).
    ///
    /// Uses a small worker pool (4 threads) suitable for embedded scenarios
    /// where a full production runtime is not needed.
    fn new_embedded(name: impl ToString) -> Self
    where Self: Sized {
        Self::new(Some(4), Some(name.to_string())).unwrap()
    }

    /// Create a new runtime with the specified number of worker threads.
    ///
    /// - `workers`: Number of worker threads. `None` uses tokio's default.
    /// - `name`: Optional name prefix for worker threads.
    fn new(workers: Option<usize>, name: Option<String>) -> Result<Self, String>
    where Self: Sized;

    /// Spawn a future on this runtime instance.
    fn spawn_on<F>(&self, future: F, name: Option<String>) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;
}
