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

use common_arrow::arrow_format::flight::service::flight_service_server::FlightServiceServer;
use common_base::tokio;
use common_base::tokio::sync::oneshot;
use common_base::tokio::sync::oneshot::Receiver;
use common_base::tokio::sync::oneshot::Sender;
use common_base::tokio::task::JoinHandle;
use common_base::Stoppable;
use common_exception::ErrorCode;
use common_tracing::tracing;
use common_tracing::tracing::Instrument;
use futures::future::Either;
use tonic::transport;
use tonic::transport::Identity;
use tonic::transport::Server;
use transport::ServerTlsConfig;

use crate::api::rpc::MetaFlightImpl;
use crate::configs::Config;
use crate::meta_service::MetaNode;

pub struct FlightServer {
    conf: Config,
    meta_node: Arc<MetaNode>,
    join_handle: Option<JoinHandle<()>>,
    stop_tx: Option<Sender<()>>,
    fin_rx: Option<Receiver<()>>,
}

impl FlightServer {
    pub fn create(conf: Config, meta_node: Arc<MetaNode>) -> Self {
        Self {
            conf,
            meta_node,
            join_handle: None,
            stop_tx: None,
            fin_rx: None,
        }
    }

    /// Start metasrv and returns two channel to send shutdown signal and receive signal when shutdown finished.
    pub async fn do_start(&mut self) -> Result<(), ErrorCode> {
        let conf = self.conf.clone();
        let meta_node = self.meta_node.clone();

        // For sending signal when server started.
        let (started_tx, started_rx) = oneshot::channel::<()>();
        // For receiving stop signal
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        // For sending the signal when server finished shutting down.
        let (fin_tx, fin_rx) = oneshot::channel::<()>();

        let builder = Server::builder();

        let tls_conf = Self::tls_config(&self.conf).await.map_err(|e| {
            ErrorCode::TLSConfigurationFailure(format!("failed to build ServerTlsConfig, {}", e))
        })?;

        let mut builder = if let Some(conf) = tls_conf {
            tracing::info!("TLS RPC enabled");
            builder.tls_config(conf).map_err(|e| {
                ErrorCode::TLSConfigurationFailure(format!("server tls_config failure {}", e))
            })?
        } else {
            builder
        };

        let addr = conf.flight_api_address.parse::<std::net::SocketAddr>()?;
        tracing::info!("flight addr: {}", addr);

        let flight_impl = MetaFlightImpl::create(conf, meta_node.clone());
        let flight_srv = FlightServiceServer::new(flight_impl);

        let j = tokio::spawn(
            async move {
                let res = builder
                    .add_service(flight_srv)
                    .serve_with_shutdown(addr, async move {
                        let _ = started_tx.send(());
                        tracing::info!("metasrv start to wait for stop signal: {}", addr);
                        let _ = stop_rx.await;
                        tracing::info!("metasrv receives stop signal: {}", addr);
                    })
                    .await;

                // Server quit. Start to shutdown meta node.

                let _ = meta_node.stop().await;
                let send_fin_res = fin_tx.send(());
                tracing::info!(
                    "metasrv sending signal of finishing shutdown {}: res: {:?}",
                    addr,
                    send_fin_res
                );

                tracing::info!("metasrv returned res: {:?}", res);
            }
            .instrument(tracing::debug_span!("spawn-rpc")),
        );

        // Blocks until server started or the tx is dropped.
        started_rx
            .await
            .map_err(|e| ErrorCode::MetaServiceError(e.to_string()))?;

        self.join_handle = Some(j);
        self.stop_tx = Some(stop_tx);
        self.fin_rx = Some(fin_rx);

        Ok(())
    }

    pub async fn do_stop(
        &mut self,
        force: Option<tokio::sync::broadcast::Receiver<()>>,
    ) -> Result<(), ErrorCode> {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }

        if let Some(j) = self.join_handle.take() {
            if let Some(mut frc) = force {
                let f = Box::pin(frc.recv());
                let j = Box::pin(j);

                match futures::future::select(f, j).await {
                    Either::Left((_x, j)) => {
                        tracing::info!("received force shutdown signal");
                        // force shutdown signal received.
                        j.abort();
                    }
                    Either::Right((_, _)) => {
                        tracing::info!("Done: graceful force shutdown");
                        // graceful shutdown finished.
                    }
                }
            } else {
                tracing::info!("no force signal, block waiting for join handle for ever");
                let _ = j.await;
                tracing::info!("Done: waiting for join handle for ever");
            }
        }

        if let Some(rx) = self.fin_rx.take() {
            tracing::info!("block waiting for fin_rx");
            let _ = rx.await;
            tracing::info!("Done: block waiting for fin_rx");
        }
        Ok(())
    }

    async fn tls_config(conf: &Config) -> anyhow::Result<Option<ServerTlsConfig>> {
        if conf.tls_rpc_server_enabled() {
            let cert = tokio::fs::read(conf.flight_tls_server_cert.as_str()).await?;
            let key = tokio::fs::read(conf.flight_tls_server_key.as_str()).await?;
            let server_identity = Identity::from_pem(cert, key);

            let tls = ServerTlsConfig::new().identity(server_identity);
            Ok(Some(tls))
        } else {
            Ok(None)
        }
    }
}

#[async_trait::async_trait]
impl Stoppable for FlightServer {
    async fn start(&mut self) -> Result<(), ErrorCode> {
        self.do_start().await
    }

    async fn stop(
        &mut self,
        force: Option<tokio::sync::broadcast::Receiver<()>>,
    ) -> Result<(), ErrorCode> {
        self.do_stop(force).await
    }
}
