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

//! Bridge implementations for the client-side `SpawnApi` and `RuntimeApi` traits.
//!
//! The `databend-meta` (server) and `databend-meta-client` crates come from different
//! repos, each with its own instance of `databend-meta-runtime-api`. Rust treats traits
//! from these distinct instances as different types.
//!
//! This module implements the client-side traits for `DatabendRuntime` by delegating
//! to the server-side implementations, with thin conversions for types that differ
//! between the two crate instances.

use std::future::Future;
use std::net::IpAddr;
use std::time::Duration;

use databend_meta::runtime_api as server_rt;
use databend_meta_client::runtime_api as client_rt;
use tonic_013::Request;

use crate::DatabendMetrics;
use crate::DatabendRuntime;

fn convert_tls(c: client_rt::TlsConfig) -> server_rt::TlsConfig {
    server_rt::TlsConfig {
        root_ca_cert_path: c.root_ca_cert_path,
        domain_name: c.domain_name,
    }
}

fn convert_channel_error(e: server_rt::ChannelError) -> client_rt::ChannelError {
    match e {
        server_rt::ChannelError::InvalidUri { uri, message } => {
            client_rt::ChannelError::InvalidUri { uri, message }
        }
        server_rt::ChannelError::TlsConfig { action, message } => {
            client_rt::ChannelError::TlsConfig { action, message }
        }
        server_rt::ChannelError::CannotConnect { uri, message } => {
            client_rt::ChannelError::CannotConnect { uri, message }
        }
    }
}

fn convert_tracking_data(d: client_rt::TrackingData) -> server_rt::TrackingData {
    match d {
        client_rt::TrackingData::QueryId(id) => server_rt::TrackingData::QueryId(id),
    }
}

impl client_rt::ClientMetricsApi for DatabendMetrics {
    fn record_request_duration(endpoint: &str, request: &str, duration_ms: f64) {
        <DatabendMetrics as server_rt::ClientMetricsApi>::record_request_duration(
            endpoint,
            request,
            duration_ms,
        )
    }

    fn request_inflight(delta: i64) {
        <DatabendMetrics as server_rt::ClientMetricsApi>::request_inflight(delta)
    }

    fn record_request_success(endpoint: &str, request: &str) {
        <DatabendMetrics as server_rt::ClientMetricsApi>::record_request_success(endpoint, request)
    }

    fn record_request_failed(endpoint: &str, request: &str, error_name: &str) {
        <DatabendMetrics as server_rt::ClientMetricsApi>::record_request_failed(
            endpoint, request, error_name,
        )
    }

    fn record_make_client_fail(endpoint: &str) {
        <DatabendMetrics as server_rt::ClientMetricsApi>::record_make_client_fail(endpoint)
    }
}

impl client_rt::SpawnApi for DatabendRuntime {
    type ClientMetrics = DatabendMetrics;

    fn spawn<F>(future: F, name: Option<String>) -> client_rt::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        <Self as server_rt::SpawnApi>::spawn(future, name)
    }

    fn spawn_blocking<F, R>(f: F) -> client_rt::JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        <Self as server_rt::SpawnApi>::spawn_blocking(f)
    }

    fn track_future<'a, T, Fut>(
        fut: Fut,
        data: Vec<client_rt::TrackingData>,
    ) -> client_rt::BoxFuture<'a, T>
    where
        Fut: Future<Output = T> + Send + 'a,
        T: Send + 'a,
    {
        let data = data.into_iter().map(convert_tracking_data).collect();
        <Self as server_rt::SpawnApi>::track_future(fut, data)
    }

    fn unlimited_future<'a, T, Fut>(fut: Fut) -> client_rt::BoxFuture<'a, T>
    where
        Fut: Future<Output = T> + Send + 'a,
        T: Send + 'a,
    {
        <Self as server_rt::SpawnApi>::unlimited_future(fut)
    }

    fn prepare_request<T>(request: Request<T>) -> Request<T> {
        <DatabendRuntime as server_rt::SpawnApi>::prepare_request(request)
    }

    fn trace_request<'a, T, F, Fut, R>(
        name: &'static str,
        request: Request<T>,
        f: F,
    ) -> client_rt::BoxFuture<'a, R>
    where
        F: FnOnce(Request<T>) -> Fut,
        Fut: Future<Output = R> + Send + 'a,
        R: Send + 'a,
    {
        <DatabendRuntime as server_rt::SpawnApi>::trace_request(name, request, f)
    }

    fn capture_tracking_context() -> Box<dyn FnOnce() -> Box<dyn std::any::Any + Send> + Send> {
        <DatabendRuntime as server_rt::SpawnApi>::capture_tracking_context()
    }

    fn connect(
        addr: String,
        timeout: Option<Duration>,
        tls_config: Option<client_rt::TlsConfig>,
    ) -> client_rt::BoxFuture<'static, Result<client_rt::Channel, client_rt::ChannelError>> {
        let server_tls = tls_config.map(convert_tls);
        Box::pin(async move {
            <DatabendRuntime as server_rt::SpawnApi>::connect(addr, timeout, server_tls)
                .await
                .map_err(convert_channel_error)
        })
    }

    fn resolve(hostname: &str) -> client_rt::BoxFuture<'static, std::io::Result<Vec<IpAddr>>> {
        <DatabendRuntime as server_rt::SpawnApi>::resolve(hostname)
    }

    fn init_test_logging() -> Box<dyn std::any::Any + Send> {
        <DatabendRuntime as server_rt::SpawnApi>::init_test_logging()
    }
}

impl client_rt::RuntimeApi for DatabendRuntime {
    fn new(workers: Option<usize>, name: Option<String>) -> Result<Self, String> {
        <Self as server_rt::RuntimeApi>::new(workers, name)
    }

    fn spawn_on<F>(&self, future: F, name: Option<String>) -> client_rt::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        <Self as server_rt::RuntimeApi>::spawn_on(self, future, name)
    }
}
