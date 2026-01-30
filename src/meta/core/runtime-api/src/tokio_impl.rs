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
use std::sync::Arc;
use std::thread::JoinHandle as ThreadJoinHandle;
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Certificate;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Endpoint;

use crate::BoxFuture;
use crate::Channel;
use crate::ChannelError;
use crate::ClientMetricsApi;
use crate::RuntimeApi;
use crate::SpawnApi;
use crate::TlsConfig;
use crate::TrackingData;

/// No-op metrics implementation for lightweight/testing scenarios.
#[derive(Clone, Copy, Debug)]
pub struct NoopMetrics;

impl ClientMetricsApi for NoopMetrics {
    fn record_request_duration(_: &str, _: &str, _: f64) {}
    fn request_inflight(_: i64) {}
    fn record_request_success(_: &str, _: &str) {}
    fn record_request_failed(_: &str, _: &str, _: &str) {}
    fn record_make_client_fail(_: &str) {}
}

/// Tokio runtime that can spawn tasks.
///
/// Can be used in two ways:
/// - As an owned runtime instance (via `RuntimeApi::new()`)
/// - As a type parameter for `SpawnApi` static methods (no instance needed)
///
/// # Why the runtime lives in a separate thread
///
/// Tokio runtime cannot be dropped from within an async context - it panics with:
/// "Cannot drop a runtime in a context where blocking is not allowed."
///
/// To avoid this, we spawn a dedicated thread that owns the actual runtime.
/// The thread blocks on a shutdown signal, and when `TokioRuntime` is dropped,
/// it sends the signal and waits for the thread to complete.
///
/// This approach is simpler than alternatives like:
/// - Moving runtime to a new thread at drop time (doesn't wait for completion)
/// - Using `ManuallyDrop` with explicit shutdown (not ergonomic)
///
/// # Cloning
///
/// `TokioRuntime` is cloneable. Clones share the same underlying runtime.
/// The runtime shuts down only when the last clone is dropped.
#[derive(Clone)]
pub struct TokioRuntime {
    /// Handle to spawn tasks on the runtime.
    handle: Handle,

    /// Dropping this signals the runtime thread to shut down.
    /// Wrapped in Arc so clones share the same shutdown mechanism.
    _shutdown: Arc<Shutdown>,
}

/// Manages the shutdown of the runtime thread.
///
/// When dropped, sends a shutdown signal and waits for the thread to complete.
struct Shutdown {
    /// Send to signal the runtime thread to shut down.
    tx: Option<oneshot::Sender<()>>,

    /// The thread that owns the actual tokio runtime.
    thread: Option<ThreadJoinHandle<()>>,
}

impl Drop for Shutdown {
    fn drop(&mut self) {
        // Signal the runtime thread to shut down
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(());
        }
        // Wait for the thread to complete
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl std::fmt::Debug for TokioRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokioRuntime").finish_non_exhaustive()
    }
}

async fn build_tls_config(
    cfg: Option<&TlsConfig>,
) -> Result<Option<ClientTlsConfig>, ChannelError> {
    let Some(cfg) = cfg else { return Ok(None) };

    let pem =
        tokio::fs::read(&cfg.root_ca_cert_path)
            .await
            .map_err(|e| ChannelError::TlsConfig {
                action: format!("read '{}'", cfg.root_ca_cert_path),
                message: e.to_string(),
            })?;

    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(pem))
        .domain_name(&cfg.domain_name);

    Ok(Some(tls))
}

#[expect(clippy::disallowed_methods)]
impl SpawnApi for TokioRuntime {
    type ClientMetrics = NoopMetrics;

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

    /// No-op for TokioRuntime - just returns the future as-is.
    fn unlimited_future<'a, T, Fut>(fut: Fut) -> BoxFuture<'a, T>
    where
        Fut: Future<Output = T> + Send + 'a,
        T: Send + 'a,
    {
        Box::pin(fut)
    }

    /// No-op for TokioRuntime - returns request unchanged.
    fn prepare_request<T>(request: tonic::Request<T>) -> tonic::Request<T> {
        request
    }

    /// No-op for TokioRuntime - just calls the closure without tracing.
    fn trace_request<'a, T, F, Fut, R>(
        _name: &'static str,
        request: tonic::Request<T>,
        f: F,
    ) -> BoxFuture<'a, R>
    where
        F: FnOnce(tonic::Request<T>) -> Fut,
        Fut: Future<Output = R> + Send + 'a,
        R: Send + 'a,
    {
        Box::pin(f(request))
    }

    fn capture_tracking_context() -> Box<dyn FnOnce() -> Box<dyn std::any::Any + Send> + Send> {
        Box::new(|| Box::new(()))
    }

    /// Channel creation using tonic's built-in endpoint with optional TLS.
    ///
    /// Does not use custom DNS resolution - suitable for testing and simple deployments.
    fn connect(
        addr: String,
        timeout: Option<Duration>,
        tls_config: Option<TlsConfig>,
    ) -> BoxFuture<'static, Result<Channel, ChannelError>> {
        Box::pin(async move {
            let tls = build_tls_config(tls_config.as_ref()).await?;
            let scheme = if tls.is_some() { "https" } else { "http" };

            let mut endpoint =
                Endpoint::from_shared(format!("{scheme}://{addr}")).map_err(|e| {
                    ChannelError::InvalidUri {
                        uri: addr.clone(),
                        message: e.to_string(),
                    }
                })?;

            if let Some(t) = timeout {
                endpoint = endpoint.connect_timeout(t).timeout(t);
            }
            if let Some(tls) = tls {
                endpoint = endpoint
                    .tls_config(tls)
                    .map_err(|e| ChannelError::TlsConfig {
                        action: "apply".to_string(),
                        message: e.to_string(),
                    })?;
            }

            endpoint
                .connect()
                .await
                .map_err(|e| ChannelError::CannotConnect {
                    uri: addr,
                    message: e.to_string(),
                })
        })
    }
}

#[expect(clippy::disallowed_methods)]
impl RuntimeApi for TokioRuntime {
    fn new(workers: Option<usize>, name: Option<String>) -> Result<Self, String> {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        if let Some(n) = workers {
            builder.worker_threads(n);
        }
        if let Some(ref name) = name {
            builder.thread_name(name);
        }
        builder.enable_all();
        let runtime = builder.build().map_err(|e| e.to_string())?;
        let handle = runtime.handle().clone();

        // Create shutdown channel
        let (tx, rx) = oneshot::channel();

        // Spawn a thread that owns the runtime and waits for shutdown signal.
        // This avoids the "Cannot drop a runtime in a context where blocking is not allowed" panic.
        let thread_name = name.map(|n| format!("rt-shutdown-{}", n));
        let thread = std::thread::Builder::new()
            .name(thread_name.unwrap_or_else(|| "rt-shutdown".to_string()))
            .spawn(move || {
                // Block until shutdown signal received, then drop runtime
                let _ = runtime.block_on(rx);
                // runtime is dropped here, outside of async context
            })
            .map_err(|e| e.to_string())?;

        Ok(Self {
            handle,
            _shutdown: Arc::new(Shutdown {
                tx: Some(tx),
                thread: Some(thread),
            }),
        })
    }

    fn spawn_on<F>(&self, future: F, _name: Option<String>) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spawn_on() {
        let rt = TokioRuntime::new(Some(1), None).unwrap();
        let handle = rt.spawn_on(async { 42 }, None);

        // Use a separate runtime to block_on since we can't access the inner runtime
        #[allow(clippy::disallowed_methods)]
        let result = std::thread::spawn(move || {
            let rt2 = tokio::runtime::Runtime::new().unwrap();
            rt2.block_on(handle)
        })
        .join()
        .unwrap();

        assert_eq!(result.unwrap(), 42);
    }

    /// Test that TokioRuntime can be dropped from within an async context.
    ///
    /// Before the fix, this would panic with:
    /// "Cannot drop a runtime in a context where blocking is not allowed"
    #[tokio::test]
    async fn test_drop_from_async_context() {
        let rt = TokioRuntime::new(Some(1), Some("test-drop".to_string())).unwrap();
        let handle = rt.spawn_on(async { 42 }, None);
        assert_eq!(handle.await.unwrap(), 42);
        // rt is dropped here, inside async context - this should NOT panic
        drop(rt);
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
