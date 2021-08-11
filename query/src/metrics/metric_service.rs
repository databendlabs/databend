// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_runtime::tokio;
use common_runtime::tokio::sync::Notify;
use common_runtime::tokio::task::JoinHandle;
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_exporter_prometheus::PrometheusHandle;
use warp::hyper::Body;
use warp::reply::Response;
use warp::Filter;
use warp::Reply;

use crate::servers::Server;

pub struct MetricService {
    abort_notify: Arc<Notify>,
    join_handle: Option<JoinHandle<()>>,
}

impl MetricService {
    pub fn create() -> Box<dyn Server> {
        Box::new(MetricService {
            abort_notify: Arc::new(Notify::new()),
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

    fn shutdown_notify(&self) -> impl Future<Output = ()> + 'static {
        let notified = self.abort_notify.clone();
        async move {
            notified.notified().await;
        }
    }
}

#[async_trait::async_trait]
impl Server for MetricService {
    async fn shutdown(&mut self) {
        self.abort_notify.notify_waiters();

        if let Some(join_handle) = self.join_handle.take() {
            if let Err(error) = join_handle.await {
                log::error!(
                    "Unexpected error during shutdown MetricServer. cause {}",
                    error
                );
            }
        }
    }

    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        let handle = MetricService::create_prometheus_handle()?;

        let server = warp::serve(warp::any().map(move || MetricsReply(handle.render())));
        let (listening, server) = server
            .try_bind_with_graceful_shutdown(listening, self.shutdown_notify())
            .map_err_to_code(ErrorCode::CannotListenerPort, || {
                format!("Cannot start MetricServer with {}", listening)
            })?;

        self.join_handle = Some(tokio::spawn(server));

        Ok(listening)
    }
}

struct MetricsReply(String);

impl Reply for MetricsReply {
    fn into_response(self) -> Response {
        warp::http::Response::new(Body::from(self.0))
    }
}
