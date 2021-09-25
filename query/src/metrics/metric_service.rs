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
use std::future::Future;
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
use common_exception::ErrorCode;
use common_exception::Result;
use common_base::tokio;
use common_base::tokio::task::JoinHandle;
use futures::future::AbortHandle;
use futures::future::AbortRegistration;
use futures::future::Abortable;
use futures::StreamExt;
use hyper::server::conn::Http;
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_exporter_prometheus::PrometheusHandle;
use tokio_stream::wrappers::TcpListenerStream;

use crate::servers::server::ListeningStream;
use crate::servers::Server;

pub struct MetricService {
    join_handle: Option<JoinHandle<()>>,
    abort_handle: AbortHandle,
    abort_registration: Option<AbortRegistration>,
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
            .route("/", get(metric_handler))
            .layer(AddExtensionLayer::new($prometheus.clone()))
    };
}

impl MetricService {
    pub fn create() -> Box<dyn Server> {
        let (abort_handle, registration) = AbortHandle::new_pair();
        Box::new(MetricService {
            abort_handle,
            abort_registration: Some(registration),
            join_handle: None,
        })
    }

    fn create_prometheus_handle() -> Result<PrometheusHandle> {
        let builder = PrometheusBuilder::new();
        let prometheus_recorder = builder.build();
        let prometheus_handle = prometheus_recorder.handle();
        match metrics::set_boxed_recorder(Box::new(prometheus_recorder)) {
            Ok(_) => Ok(prometheus_handle),
            Err(error) => Err(ErrorCode::InitPrometheusFailure(format!(
                "Cannot init prometheus recorder. cause: {}",
                error
            ))),
        }
    }

    async fn listener_tcp(socket: SocketAddr) -> Result<(TcpListenerStream, SocketAddr)> {
        let listener = tokio::net::TcpListener::bind(socket).await?;
        let listener_addr = listener.local_addr()?;
        Ok((TcpListenerStream::new(listener), listener_addr))
    }

    // TODO(zhihanz) add TLS config for metric server
    fn listen_loop(
        &self,
        stream: ListeningStream,
        handler: PrometheusHandle,
    ) -> impl Future<Output = ()> {
        let app = build_router!(handler);
        stream.for_each(move |accept_socket| {
            let app = app.clone();
            async move {
                match accept_socket {
                    Err(error) => log::error!("Broken http connection: {}", error),
                    Ok(socket) => {
                        tokio::spawn(async move {
                            Http::new().serve_connection(socket, app).await.unwrap();
                        });
                    }
                };
            }
        })
    }
}

#[async_trait::async_trait]
impl Server for MetricService {
    async fn shutdown(&mut self) {
        self.abort_handle.abort();

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
        match self.abort_registration.take() {
            None => Err(ErrorCode::LogicalError("Http Service already running.")),
            Some(registration) => {
                let handle = MetricService::create_prometheus_handle()?;
                let (stream, listener) = Self::listener_tcp(listening).await?;
                let stream = Abortable::new(stream, registration);
                self.join_handle = Some(tokio::spawn(self.listen_loop(stream, handle)));
                Ok(listener)
            }
        }
    }
}
