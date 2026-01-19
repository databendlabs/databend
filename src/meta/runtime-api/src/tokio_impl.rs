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

use std::future::Future;

use tokio::task::JoinHandle;

use crate::BoxFuture;
use crate::RuntimeApi;
use crate::SpawnApi;
use crate::TrackingData;

/// Tokio runtime that can spawn tasks.
///
/// Can be used in two ways:
/// - As an owned runtime instance (via `RuntimeApi::new()`)
/// - As a type parameter for `SpawnApi` static methods (no instance needed)
pub struct TokioRuntime {
    inner: tokio::runtime::Runtime,
}

impl std::fmt::Debug for TokioRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokioRuntime").finish_non_exhaustive()
    }
}

#[expect(clippy::disallowed_methods)]
impl SpawnApi for TokioRuntime {
    fn spawn<F>(future: F, _name: Option<String>) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::spawn(future)
    }

    fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        tokio::runtime::Handle::current().spawn_blocking(f)
    }

    /// The default impl does not do anything.
    fn track_future<'a, T, Fut>(fut: Fut, data: Vec<TrackingData>) -> BoxFuture<'a, T>
    where
        Fut: Future<Output = T> + Send + 'a,
        T: Send + 'a,
    {
        let _ = data;
        Box::pin(fut)
    }
}

#[expect(clippy::disallowed_methods)]
impl RuntimeApi for TokioRuntime {
    fn new(workers: Option<usize>, name: Option<String>) -> Result<Self, String> {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        if let Some(n) = workers {
            builder.worker_threads(n);
        }
        if let Some(name) = name {
            builder.thread_name(name);
        }
        builder.enable_all();
        let inner = builder.build().map_err(|e| e.to_string())?;
        Ok(Self { inner })
    }

    fn spawn_on<F>(&self, future: F, _name: Option<String>) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.inner.spawn(future)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spawn_on() {
        let rt = TokioRuntime::new(Some(1), None).unwrap();
        let handle = rt.spawn_on(async { 42 }, None);
        rt.inner.block_on(async {
            assert_eq!(handle.await.unwrap(), 42);
        });
    }

    #[tokio::test]
    async fn test_spawn() {
        let handle = TokioRuntime::spawn(async { "hello" }, None);
        assert_eq!(handle.await.unwrap(), "hello");
    }

    #[tokio::test]
    async fn test_abort() {
        let handle = TokioRuntime::spawn(
            async {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            },
            None,
        );
        handle.abort();
        assert!(handle.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn test_spawn_blocking() {
        let handle = TokioRuntime::spawn_blocking(|| 42);
        assert_eq!(handle.await.unwrap(), 42);
    }
}
