// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_infallible::Mutex;
use common_runtime::tokio;
use common_runtime::tokio::sync::oneshot::Sender;
use common_runtime::tokio::sync::Notify;
use futures::future::FutureExt;
use metrics_exporter_prometheus::PrometheusBuilder;
use warp::hyper::Body;
use warp::reply::Response;
use warp::Filter;
use warp::Reply;

use crate::servers::AbortableServer;
use crate::servers::AbortableService;
use crate::servers::Elapsed;

pub struct MetricService {
    aborted: Arc<AtomicBool>,
    abort_handle: Mutex<Option<Sender<()>>>,
    aborted_notify: Arc<Notify>,
}

impl MetricService {
    pub fn create() -> AbortableServer {
        Arc::new(MetricService {
            aborted: Arc::new(AtomicBool::new(false)),
            abort_handle: Mutex::new(None),
            aborted_notify: Arc::new(Notify::new()),
        })
    }
}

#[async_trait::async_trait]
impl AbortableService<(String, u16), SocketAddr> for MetricService {
    fn abort(&self, _force: bool) -> Result<()> {
        if let Some(abort_handle) = self.abort_handle.lock().take() {
            match abort_handle.send(()) {
                Ok(_) => { /* do nothing */ }
                Err(_) => {
                    return Err(ErrorCode::LogicalError(
                        "Cannot abort MetricService, cause: cannot send signal to service.",
                    ))
                }
            }
        }

        Ok(())
    }

    async fn start(&self, args: (String, u16)) -> Result<SocketAddr> {
        let builder = PrometheusBuilder::new();
        let prometheus_recorder = builder.build();
        let prometheus_handle = prometheus_recorder.handle();
        metrics::set_boxed_recorder(Box::new(prometheus_recorder))
            .map_err_to_code(ErrorCode::UnknownException, || {
                "Init prometheus recorder error"
            })?;

        let server = warp::serve(warp::any().map(move || MetricsReply(prometheus_handle.render())));

        let (tx, rx) = tokio::sync::oneshot::channel();
        *self.abort_handle.lock() = Some(tx);
        let addr = args.to_socket_addrs()?.next().unwrap();
        let (addr, server) = server
            .try_bind_with_graceful_shutdown(addr, rx.map(|_| ()))
            .map_err_to_code(ErrorCode::CannotListenerPort, || {
                format!("Cannot listener port {}", args.1)
            })?;

        let aborted = self.aborted.clone();
        let aborted_notify = self.aborted_notify.clone();
        tokio::spawn(async move {
            server.await;
            aborted.store(true, Ordering::Relaxed);
            aborted_notify.notify_waiters();
        });

        Ok(addr)
    }

    async fn wait_terminal(&self, duration: Option<Duration>) -> Result<Elapsed> {
        let instant = Instant::now();

        match duration {
            None => {
                if !self.aborted.load(Ordering::Relaxed) {
                    self.aborted_notify.notified().await;
                }
            }
            Some(duration) => {
                if !self.aborted.load(Ordering::Relaxed) {
                    tokio::time::timeout(duration, self.aborted_notify.notified())
                        .await
                        .map_err(|_| {
                            ErrorCode::Timeout(format!(
                                "MetricService did not shutdown in {:?}",
                                duration
                            ))
                        })?;
                }
            }
        };

        Ok(instant.elapsed())
    }
}

struct MetricsReply(String);

impl Reply for MetricsReply {
    fn into_response(self) -> Response {
        warp::http::Response::new(Body::from(self.0))
    }
}
