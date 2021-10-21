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

use std::convert::Infallible;
use std::net::SocketAddr;

use axum::body::Bytes;
use axum::body::Full;
use axum::extract::Extension;
use axum::handler::get;
use axum::http::Response;
use axum::response::Html;
use axum::response::IntoResponse;
use axum::AddExtensionLayer;
use axum::Router;
use axum_server::Handle;
use common_base::tokio;
use common_base::tokio::task::JoinHandle;
use common_exception::ErrorCode;
use common_exception::Result;
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_exporter_prometheus::PrometheusHandle;

use crate::servers::Server;
use crate::sessions::SessionManagerRef;

pub struct MetricService {
    join_handle: Option<JoinHandle<std::io::Result<()>>>,
    abort_handler: Handle,
}

pub struct MetricTemplate {
    prom: PrometheusHandle,
}

impl IntoResponse for MetricTemplate {
    type Body = Full<Bytes>;
    type BodyError = Infallible;

    fn into_response(self) -> Response<Self::Body> {
        Html(self.prom.render()).into_response()
    }
}

pub async fn metric_handler(prom_extension: Extension<PrometheusHandle>) -> MetricTemplate {
    let prom = prom_extension.0;
    MetricTemplate { prom }
}

// build axum router for metric server
macro_rules! build_router {
    ($prometheus: expr) => {
        Router::new()
            .route("/metrics", get(metric_handler))
            .layer(AddExtensionLayer::new($prometheus.clone()))
    };
}

impl MetricService {
    // TODO add session tls handler
    pub fn create(_sessions: SessionManagerRef) -> Box<MetricService> {
        Box::new(MetricService {
            join_handle: None,
            abort_handler: axum_server::Handle::new(),
        })
    }

    fn create_prometheus_handle() -> Result<PrometheusHandle> {
        let builder = PrometheusBuilder::new();
        let prometheus_recorder = builder.build();
        let prometheus_handle = prometheus_recorder.handle();
        // TODO(zhihanz) add metrics descriptions through regist
        match metrics::set_boxed_recorder(Box::new(prometheus_recorder)) {
            Ok(_) => Ok(prometheus_handle),
            Err(error) => Err(ErrorCode::InitPrometheusFailure(format!(
                "Cannot init prometheus recorder. cause: {}",
                error
            ))),
        }
    }

    async fn start_without_tls(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        let handler =
            MetricService::create_prometheus_handle().expect("cannot build prometheus handler");
        let app = build_router!(handler);
        let server = axum_server::bind(listening.to_string())
            .handle(self.abort_handler.clone())
            .serve(app);

        self.join_handle = Some(tokio::spawn(server));
        self.abort_handler.listening().await;

        match self.abort_handler.listening_addrs() {
            None => Err(ErrorCode::CannotListenerPort("")),
            Some(addresses) if addresses.is_empty() => Err(ErrorCode::CannotListenerPort("")),
            Some(addresses) => {
                // 0.0.0.0, for multiple network interface, we may listen to multiple address
                let first_address = addresses[0];
                for address in addresses {
                    if address.port() != first_address.port() {
                        return Err(ErrorCode::CannotListenerPort(""));
                    }
                }
                Ok(first_address)
            }
        }
    }
}

#[async_trait::async_trait]
impl Server for MetricService {
    async fn shutdown(&mut self) {
        self.abort_handler.graceful_shutdown();

        if let Some(join_handle) = self.join_handle.take() {
            if let Err(error) = join_handle.await {
                log::error!(
                    "Unexpected error during shutdown Http API handler. cause {}",
                    error
                );
            }
        }
    }

    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        self.start_without_tls(listening).await
    }
}
