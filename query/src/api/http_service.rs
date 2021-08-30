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

use std::net::SocketAddr;

use axum::handler::get;
use axum::handler::post;
use axum::AddExtensionLayer;
use axum::Router;
use common_exception::ErrorCode;
use common_exception::Result;
use common_runtime::tokio;
use common_runtime::tokio::task::JoinHandle;
use futures::future::AbortHandle;
use futures::future::AbortRegistration;
use futures::future::Abortable;
use futures::Future;
use futures::StreamExt;
use hyper::server::conn::Http;
use tokio_stream::wrappers::TcpListenerStream;

// use crate::api::http::router::Router;
use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::servers::server::ListeningStream;
use crate::servers::Server;

pub struct HttpService {
    cfg: Config,
    cluster: ClusterRef,
    join_handle: Option<JoinHandle<()>>,
    abort_handle: AbortHandle,
    abort_registration: Option<AbortRegistration>,
}

// build axum router
macro_rules! build_router {
    ($cfg: expr, $cluster: expr) => {
        Router::new()
            .route("/v1/hello", get(super::http::v1::hello::hello_handler))
            .route("/v1/config", get(super::http::v1::config::config_handler))
            .route(
                "/v1/cluster/add",
                post(super::http::v1::cluster::cluster_add_handler),
            )
            .route(
                "/v1/cluster/list",
                get(super::http::v1::cluster::cluster_list_handler),
            )
            .route(
                "/v1/cluster/remove",
                post(super::http::v1::cluster::cluster_remove_handler),
            )
            .route(
                "/debug/home",
                get(super::http::debug::home::debug_home_handler),
            )
            .route(
                "/debug/pprof/profile",
                get(super::http::debug::pprof::debug_pprof_handler),
            )
            .layer(AddExtensionLayer::new($cluster.clone()))
            .layer(AddExtensionLayer::new($cfg.clone()))
    };
}

impl HttpService {
    pub fn create(cfg: Config, cluster: ClusterRef) -> Box<Self> {
        let (abort_handle, registration) = AbortHandle::new_pair();
        Box::new(HttpService {
            cfg,
            cluster,
            join_handle: None,
            abort_handle,
            abort_registration: Some(registration),
        })
    }

    async fn listener_tcp(socket: SocketAddr) -> Result<(TcpListenerStream, SocketAddr)> {
        let listener = tokio::net::TcpListener::bind(socket).await?;
        let listener_addr = listener.local_addr()?;
        Ok((TcpListenerStream::new(listener), listener_addr))
    }

    fn listen_loop(&self, stream: ListeningStream) -> impl Future<Output = ()> {
        let app = build_router!(self.cfg.clone(), self.cluster.clone());
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
impl Server for HttpService {
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
                let (stream, listener) = Self::listener_tcp(listening).await?;
                let stream = Abortable::new(stream, registration);
                self.join_handle = Some(tokio::spawn(self.listen_loop(stream)));
                Ok(listener)
            }
        }
    }
}
