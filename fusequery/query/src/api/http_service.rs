// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::SocketAddr;
use std::sync::Arc;

use common_exception::Result;
use common_runtime::tokio;
use common_runtime::tokio::sync::Notify;
use common_runtime::tokio::task::JoinHandle;
use futures::Future;

use crate::api::http::router::Router;
use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::servers::Server;

pub struct HttpService {
    cfg: Config,
    cluster: ClusterRef,
    abort_notify: Arc<Notify>,
    join_handle: Option<JoinHandle<()>>,
}

impl HttpService {
    pub fn create(cfg: Config, cluster: ClusterRef) -> Box<dyn Server> {
        Box::new(HttpService {
            cfg,
            cluster,
            abort_notify: Arc::new(Notify::new()),
            join_handle: None,
        })
    }

    fn shutdown_notify(&self) -> impl Future<Output = ()> + 'static {
        let notified = self.abort_notify.clone();
        async move {
            notified.notified().await;
        }
    }
}

#[async_trait::async_trait]
impl Server for HttpService {
    async fn shutdown(&mut self) {
        self.abort_notify.notify_waiters();

        if let Some(join_handle) = self.join_handle.take() {
            if let Err(error) = join_handle.await {
                log::error!(
                    "Unexpected error during shutdown HttpServer. cause {}",
                    error
                );
            }
        }
    }

    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        let router = Router::create(self.cfg.clone(), self.cluster.clone());
        let server = warp::serve(router.router()?);

        let conf = self.cfg.clone();
        let tls_cert = conf.tls_server_cert;
        let tls_key = conf.tls_server_key;

        if !tls_cert.is_empty() && !tls_key.is_empty() {
            log::info!("Http API TLS enabled");
            let (listening, server) = server
                .tls()
                .cert_path(tls_cert)
                .key_path(tls_key)
                .bind_with_graceful_shutdown(listening, self.shutdown_notify());
            self.join_handle = Some(tokio::spawn(server));
            Ok(listening)
        } else {
            log::warn!("Http API TLS not set");
            let (listening, server) =
                server.bind_with_graceful_shutdown(listening, self.shutdown_notify());
            self.join_handle = Some(tokio::spawn(server));
            Ok(listening)
        }
    }
}
