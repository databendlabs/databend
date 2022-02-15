// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use anyerror::AnyError;
use common_base::tokio;
use common_base::tokio::sync::oneshot;
use common_base::tokio::sync::oneshot::Receiver;
use common_base::tokio::sync::oneshot::Sender;
use common_base::tokio::task::JoinHandle;
use common_base::Stoppable;
use common_meta_types::protobuf::meta_service_server::MetaServiceServer;
use common_meta_types::MetaError;
use common_meta_types::MetaNetworkError;
use common_meta_types::MetaResult;
use common_tracing::tracing;
use common_tracing::tracing::Instrument;
use futures::future::Either;
use tonic::transport::Identity;
use tonic::transport::Server;
use tonic::transport::ServerTlsConfig;

use crate::api::grpc::grpc_service::MetaServiceImpl;
use crate::configs::Config;
use crate::meta_service::MetaNode;

pub struct GrpcServer {
    conf: Config,
    meta_node: Arc<MetaNode>,
    join_handle: Option<JoinHandle<()>>,
    stop_tx: Option<Sender<()>>,
    fin_rx: Option<Receiver<()>>,
}

impl GrpcServer {
    pub fn create(conf: Config, meta_node: Arc<MetaNode>) -> Self {
        Self {
            conf,
            meta_node,
            join_handle: None,
            stop_tx: None,
            fin_rx: None,
        }
    }

    pub fn get_meta_node(&self) -> Arc<MetaNode> {
        self.meta_node.clone()
    }

    async fn do_start(&mut self) -> MetaResult<()> {
        let conf = self.conf.clone();
        let meta_node = self.meta_node.clone();
        // For sending signal when server started.
        let (started_tx, started_rx) = oneshot::channel::<()>();
        // For receive stop signal.
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        // For sending the signal when server finished shutting down.
        let (fin_tx, fin_rx) = oneshot::channel::<()>();

        let builder = Server::builder();

        let tls_conf = Self::tls_config(&self.conf)
            .await
            .map_err(|e| MetaNetworkError::TLSConfigError(AnyError::new(&e)))?;

        let mut builder = if let Some(tls_conf) = tls_conf {
            tracing::info!("gRPC TLS enabled");
            builder
                .tls_config(tls_conf)
                .map_err(|e| MetaNetworkError::TLSConfigError(AnyError::new(&e)))?
        } else {
            builder
        };

        let ret = conf.grpc_api_address.parse::<std::net::SocketAddr>();
        let addr = match ret {
            Ok(addr) => addr,
            Err(e) => {
                let err: MetaNetworkError = e.into();
                return Err(err.into());
            }
        };
        tracing::info!("gRPC addr: {}", addr);

        let grpc_impl = MetaServiceImpl::create(meta_node.clone());
        let grpc_srv = MetaServiceServer::new(grpc_impl);

        let j = tokio::spawn(
            async move {
                let res = builder
                    .add_service(grpc_srv)
                    .serve_with_shutdown(addr, async move {
                        let _ = started_tx.send(());
                        tracing::info!("metasrv starts to wait for stop signal: {}", addr);
                        let _ = stop_rx.await;
                        tracing::info!("metasrv receives stop signal: {}", addr);
                    })
                    .await;

                // gRPC server quit. Starting to shutdown meta node.

                let _ = meta_node.stop().await;
                let send_fin_res = fin_tx.send(());
                tracing::info!(
                    "metasrv sending signal of finishing shutdown {}, res: {:?}",
                    addr,
                    send_fin_res
                );

                tracing::info!("metasrv returned res: {:?}", res);
            }
            .instrument(tracing::debug_span!("spawn-grpc")),
        );

        started_rx
            .await
            .map_err(|e| MetaError::StartMetaServiceError(e.to_string()))?;

        self.join_handle = Some(j);
        self.stop_tx = Some(stop_tx);
        self.fin_rx = Some(fin_rx);

        Ok(())
    }

    async fn do_stop(
        &mut self,
        force: Option<tokio::sync::broadcast::Receiver<()>>,
    ) -> common_exception::Result<()> {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }

        if let Some(j) = self.join_handle.take() {
            if let Some(mut f) = force {
                let f = Box::pin(f.recv());
                let j = Box::pin(j);

                match futures::future::select(f, j).await {
                    Either::Left((_x, j)) => {
                        tracing::info!("received force shutdown signal");
                        j.abort();
                    }
                    Either::Right(_) => {
                        tracing::info!("Done: graceful shutdown");
                    }
                }
            } else {
                tracing::info!("no force signal, block waiting for join handle for ever");
                j.await.map_err(|e| {
                    MetaError::StartMetaServiceError(format!("metasrv join handle error: {}", e))
                })?;
                tracing::info!("Done: waiting for join handle for ever");
            }
        }

        if let Some(rx) = self.fin_rx.take() {
            tracing::info!("block waiting for fin_rx");
            rx.await.map_err(|e| {
                MetaError::StartMetaServiceError(format!("metasrv fin_rx recv error: {}", e))
            })?;
            tracing::info!("Done: block waiting for fin_rx");
        }
        Ok(())
    }

    async fn tls_config(conf: &Config) -> Result<Option<ServerTlsConfig>, std::io::Error> {
        if conf.tls_rpc_server_enabled() {
            let cert = tokio::fs::read(conf.grpc_tls_server_cert.as_str()).await?;
            let key = tokio::fs::read(conf.grpc_tls_server_key.as_str()).await?;
            let server_identity = Identity::from_pem(cert, key);

            let tls = ServerTlsConfig::new().identity(server_identity);
            Ok(Some(tls))
        } else {
            Ok(None)
        }
    }
}

#[tonic::async_trait]
impl Stoppable for GrpcServer {
    async fn start(&mut self) -> common_exception::Result<()> {
        tracing::info!("GrpcServer::start");
        self.do_start().await?;
        tracing::info!("Done GrpcServer::start");
        Ok(())
    }

    async fn stop(
        &mut self,
        force: Option<tokio::sync::broadcast::Receiver<()>>,
    ) -> common_exception::Result<()> {
        tracing::info!("GrpcServer::stop");
        self.do_stop(force).await?;
        tracing::info!("Done GrpcServer::stop");
        Ok(())
    }
}
