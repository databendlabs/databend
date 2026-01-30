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
use std::time::Duration;

pub use tokio::task::JoinHandle;
pub use tokio_impl::NoopMetrics;
pub use tokio_impl::TokioRuntime;
pub use tonic::transport::Channel;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// TLS configuration for gRPC client connections.
#[derive(Clone, Debug)]
pub struct TlsConfig {
    /// Path to the root CA certificate file.
    pub root_ca_cert_path: String,
    /// Domain name for TLS verification.
    pub domain_name: String,
}

/// Errors that can occur when creating a gRPC channel.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ChannelError {
    /// The provided address could not be parsed as a valid URI.
    #[error("invalid uri: {uri}, error: {message}")]
    InvalidUri { uri: String, message: String },

    /// Failed to load or apply TLS configuration.
    #[error("{action} TLS config: {message}")]
    TlsConfig { action: String, message: String },

    /// Failed to establish connection to the server.
    #[error("cannot connect to {uri}: {message}")]
    CannotConnect { uri: String, message: String },
}

/// Metrics API for gRPC client operations.
pub trait ClientMetricsApi: Send + Sync + 'static {
    /// Record gRPC request duration in milliseconds.
    fn record_request_duration(endpoint: &str, request: &str, duration_ms: f64);

    /// Adjust in-flight request counter by delta (positive to increment, negative to decrement).
    fn request_inflight(delta: i64);

    /// Record a successful gRPC request.
    fn record_request_success(endpoint: &str, request: &str);

    /// Record a failed gRPC request.
    fn record_request_failed(endpoint: &str, request: &str, error_name: &str);

    /// Record a gRPC client creation failure.
    fn record_make_client_fail(endpoint: &str);
}

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
    /// Client metrics implementation for this runtime.
    type ClientMetrics: ClientMetricsApi;

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

    /// Prepare a tonic request by injecting implementation-specific metadata.
    ///
    /// This is a general-purpose hook for runtime implementations to inject
    /// any metadata they need into outgoing gRPC requests. For example:
    /// - Tracing span context (W3C traceparent header)
    /// - Query ID for request correlation
    /// - Any other implementation-specific headers
    ///
    /// `TokioRuntime` returns the request unchanged.
    /// `DatabendRuntime` injects tracing context and query ID.
    fn prepare_request<T>(request: tonic::Request<T>) -> tonic::Request<T>
    where Self: Sized;

    /// Trace an incoming remote request.
    ///
    /// Server-side counterpart to `prepare_request`:
    /// - `prepare_request`: client injects tracing context into outgoing requests
    /// - `trace_request`: server extracts tracing context and wraps handler with span
    ///
    /// The closure receives the request and returns a future. The implementation
    /// extracts any tracing context from request metadata and wraps the future
    /// with the appropriate span.
    ///
    /// `TokioRuntime` just calls the closure without tracing.
    /// `DatabendRuntime` extracts span context and wraps with `.in_span()`.
    fn trace_request<'a, T, F, Fut, R>(
        name: &'static str,
        request: tonic::Request<T>,
        f: F,
    ) -> BoxFuture<'a, R>
    where
        Self: Sized,
        F: FnOnce(tonic::Request<T>) -> Fut,
        Fut: Future<Output = R> + Send + 'a,
        R: Send + 'a;

    /// Capture the current tracking context, returning a guard creator.
    ///
    /// The tracking context is captured when this method is called. The returned
    /// closure, when invoked, enters that captured context and returns a guard.
    /// The guard must be held for the duration of the tracked operation - dropping
    /// it ends the tracking scope.
    ///
    /// `TokioRuntime` returns a no-op closure.
    /// `DatabendRuntime` captures ThreadTracker payload and returns tracking guard.
    fn capture_tracking_context() -> Box<dyn FnOnce() -> Box<dyn std::any::Any + Send> + Send>
    where Self: Sized;

    /// Create a gRPC channel to the specified address.
    ///
    /// Establishes a connection with optional TLS and timeout configuration.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address to connect to (e.g., "127.0.0.1:9191").
    /// * `timeout` - Optional timeout applied to each request on the channel.
    ///   Note: This does NOT set the `grpc-timeout` header, so the server
    ///   will not be informed of this timeout. The timeout is enforced
    ///   client-side only.
    /// * `tls_config` - Optional TLS configuration for secure connections.
    ///
    /// # Implementations
    ///
    /// * `TokioRuntime` uses tonic's built-in endpoint connection.
    /// * `DatabendRuntime` uses `ConnectionFactory` with DNS resolution.
    fn connect(
        addr: String,
        timeout: Option<Duration>,
        tls_config: Option<TlsConfig>,
    ) -> BoxFuture<'static, Result<Channel, ChannelError>>
    where
        Self: Sized;
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
