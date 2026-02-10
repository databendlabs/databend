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

//! Runtime adapter for meta-service using databend-common-base.
//!
//! This crate provides `DatabendRuntime`, which implements `RuntimeApi` and `SpawnApi`
//! from `databend-common-meta-runtime-api` using the runtime infrastructure from
//! `databend-common-base`. This allows meta-service to leverage Databend's advanced
//! runtime features (memory tracking, thread tracking, etc.) while keeping the meta-service
//! core decoupled from the query runtime.

mod metrics;

use std::future::Future;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::runtime;
use databend_common_grpc::ConnectionFactory;
use databend_common_grpc::GrpcConnectionError;
use databend_common_grpc::RpcClientTlsConfig;
use databend_meta_runtime_api::BoxFuture;
use databend_meta_runtime_api::Channel;
use databend_meta_runtime_api::ChannelError;
use databend_meta_runtime_api::JoinHandle;
use databend_meta_runtime_api::RuntimeApi;
use databend_meta_runtime_api::SpawnApi;
use databend_meta_runtime_api::TlsConfig;
use databend_meta_runtime_api::TrackingData;
pub use metrics::DatabendMetrics;

fn convert_grpc_error(e: GrpcConnectionError) -> ChannelError {
    match e {
        GrpcConnectionError::InvalidUri { uri, source } => ChannelError::InvalidUri {
            uri,
            message: source.to_string(),
        },
        GrpcConnectionError::TLSConfigError { action, source } => ChannelError::TlsConfig {
            action,
            message: source.to_string(),
        },
        GrpcConnectionError::CannotConnect { uri, source } => ChannelError::CannotConnect {
            uri,
            message: source.to_string(),
        },
    }
}

/// Runtime adapter that wraps `databend_common_base::Runtime`.
///
/// This provides the `RuntimeApi` implementation for meta-service binaries,
/// enabling integration with Databend's runtime infrastructure including
/// memory tracking and thread tracking.
///
/// # Cloning
///
/// `DatabendRuntime` is cloneable. Clones share the same underlying runtime.
/// The runtime shuts down only when the last clone is dropped.
#[derive(Clone)]
pub struct DatabendRuntime {
    inner: Arc<runtime::Runtime>,
}

impl std::fmt::Debug for DatabendRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabendRuntime").finish_non_exhaustive()
    }
}

impl SpawnApi for DatabendRuntime {
    type ClientMetrics = DatabendMetrics;

    fn spawn<F>(future: F, name: Option<String>) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match name {
            Some(n) => runtime::spawn_named(future, n),
            None => runtime::spawn_named(future, "meta-spawn".to_string()),
        }
    }

    fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        runtime::spawn_blocking(f)
    }

    fn track_future<'a, T, Fut>(fut: Fut, data: Vec<TrackingData>) -> BoxFuture<'a, T>
    where
        Fut: Future<Output = T> + Send + 'a,
        T: Send + 'a,
    {
        use runtime::TrackingPayloadExt;

        let mut payload = runtime::ThreadTracker::new_tracking_payload();

        for d in data {
            match d {
                TrackingData::QueryId(query_id) => {
                    payload.query_id = query_id;
                }
            }
        }

        Box::pin(payload.tracking(fut))
    }

    fn unlimited_future<'a, T, Fut>(fut: Fut) -> BoxFuture<'a, T>
    where
        Fut: Future<Output = T> + Send + 'a,
        T: Send + 'a,
    {
        Box::pin(runtime::UnlimitedFuture::create(fut))
    }

    fn prepare_request<T>(request: tonic::Request<T>) -> tonic::Request<T> {
        use std::str::FromStr;

        // Inject tracing span context
        let mut req = databend_common_tracing::inject_span_to_tonic_request(request);

        // Inject query ID if available
        if let Some(query_id) = runtime::ThreadTracker::query_id() {
            let key = tonic::metadata::AsciiMetadataKey::from_str("QueryID");
            let value = tonic::metadata::AsciiMetadataValue::from_str(query_id);

            if let Some((key, value)) = key.ok().zip(value.ok()) {
                req.metadata_mut().insert(key, value);
            }
        }

        req
    }

    fn trace_request<'a, T, F, Fut, R>(
        name: &'static str,
        request: tonic::Request<T>,
        f: F,
    ) -> BoxFuture<'a, R>
    where
        F: FnOnce(tonic::Request<T>) -> Fut,
        Fut: Future<Output = R> + Send + 'a,
        R: Send + 'a,
    {
        use fastrace::prelude::*;

        let span = databend_common_tracing::start_trace_for_remote_request(name, &request);
        Box::pin(f(request).in_span(span))
    }

    fn capture_tracking_context() -> Box<dyn FnOnce() -> Box<dyn std::any::Any + Send> + Send> {
        let payload = runtime::ThreadTracker::new_tracking_payload();
        Box::new(move || Box::new(runtime::ThreadTracker::tracking(payload)))
    }

    /// Create a gRPC channel using `ConnectionFactory` with DNS resolution.
    fn connect(
        addr: String,
        timeout: Option<Duration>,
        tls_config: Option<TlsConfig>,
    ) -> BoxFuture<'static, Result<Channel, ChannelError>> {
        Box::pin(async move {
            let grpc_tls = tls_config.map(|c| RpcClientTlsConfig {
                rpc_tls_server_root_ca_cert: c.root_ca_cert_path,
                domain_name: c.domain_name,
            });

            ConnectionFactory::create_rpc_channel(&addr, timeout, grpc_tls, None)
                .await
                .map_err(convert_grpc_error)
        })
    }

    fn resolve(hostname: &str) -> BoxFuture<'static, std::io::Result<Vec<IpAddr>>> {
        let hostname = hostname.to_string();
        Box::pin(async move {
            let resolver = databend_common_grpc::DNSResolver::instance()
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            resolver
                .resolve(&hostname)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))
        })
    }

    fn init_test_logging() -> Box<dyn std::any::Any + Send> {
        use std::collections::BTreeMap;

        let mut config = databend_common_tracing::Config::new_testing();
        config.file.level = "DEBUG".to_string();

        let guards =
            databend_common_tracing::init_logging("meta_unittests", &config, BTreeMap::new());
        Box::new(guards)
    }
}

impl RuntimeApi for DatabendRuntime {
    fn new(workers: Option<usize>, name: Option<String>) -> Result<Self, String> {
        let rt = match workers {
            Some(n) => runtime::Runtime::with_worker_threads(n, name),
            None => runtime::Runtime::with_default_worker_threads(),
        };
        rt.map(|inner| Self {
            inner: Arc::new(inner),
        })
        .map_err(|e| e.to_string())
    }

    fn spawn_on<F>(&self, future: F, name: Option<String>) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match name {
            Some(n) => self.inner.spawn_named(future, n),
            None => self.inner.spawn(future),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_runtime() {
        let rt = DatabendRuntime::new(Some(2), Some("test-rt".to_string()));
        assert!(rt.is_ok());
    }

    #[test]
    fn test_spawn_on() {
        let rt = DatabendRuntime::new(Some(1), Some("test-spawn".to_string())).unwrap();
        let handle = rt.spawn_on(async { 42 }, None);

        #[allow(clippy::disallowed_methods)]
        let result = std::thread::spawn(move || {
            let rt2 = tokio::runtime::Runtime::new().unwrap();
            rt2.block_on(handle)
        })
        .join()
        .unwrap();

        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_spawn() {
        let handle = DatabendRuntime::spawn(async { "hello" }, Some("test-task".to_string()));
        assert_eq!(handle.await.unwrap(), "hello");
    }

    #[tokio::test]
    async fn test_spawn_blocking() {
        let handle = DatabendRuntime::spawn_blocking(|| 42);
        assert_eq!(handle.await.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_track_future() {
        let fut = DatabendRuntime::track_future(async { 100 }, vec![TrackingData::new_query_id(
            Some("test-query"),
        )]);
        assert_eq!(fut.await, 100);
    }
}
