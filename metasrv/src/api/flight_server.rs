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

use common_arrow::arrow_format::flight::service::flight_service_server::FlightServiceServer;
use common_base::tokio;
use common_base::tokio::sync::oneshot;
use common_base::tokio::sync::oneshot::Receiver;
use common_base::tokio::sync::oneshot::Sender;
use common_exception::ErrorCode;
use common_exception::ToErrorCode;
use common_tracing::tracing;
use common_tracing::tracing::Instrument;
use tonic::transport;
use tonic::transport::Identity;
use tonic::transport::Server;
use transport::ServerTlsConfig;

use crate::api::rpc::MetaFlightImpl;
use crate::configs::Config;
use crate::meta_service::MetaNode;

pub struct FlightServer {
    conf: Config,
}

impl FlightServer {
    pub fn create(conf: Config) -> Self {
        Self { conf }
    }

    /// Start metasrv and returns two channel to send shutdown signal and receive signal when shutdown finished.
    pub async fn start(self) -> Result<(oneshot::Sender<()>, oneshot::Receiver<()>), ErrorCode> {
        // TODO(xp): move component startup from serve() to start().
        //           block as long as possible to reduce unknown startup time cost.
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let (fin_tx, fin_rx) = oneshot::channel::<()>();

        let tls_conf = Self::tls_config(&self.conf).await.map_err(|e| {
            ErrorCode::TLSConfigurationFailure(format!(
                "failed to build ServerTlsConfig, {}",
                e.to_string()
            ))
        })?;

        let fut = self.serve(stop_rx, fin_tx, tls_conf);
        tokio::spawn(
            async move {
                // TODO(xp): handle errors.
                // TODO(xp): move server building up actions out of serve(). errors should be caught.
                let res = fut.await;
                tracing::info!("metasrv serve res: {:?}", res);
            }
            .instrument(tracing::debug_span!("spawn-rpc")),
        );

        Ok((stop_tx, fin_rx))
    }

    /// Start serving metasrv. It does not return until metasrv is stopped.
    #[tracing::instrument(level = "debug", skip(self, stop_rx, fin_tx))]
    pub async fn serve(
        self,
        stop_rx: Receiver<()>,
        fin_tx: Sender<()>,
        tls_conf: Option<ServerTlsConfig>,
    ) -> Result<(), ErrorCode> {
        let addr = self
            .conf
            .flight_api_address
            .parse::<std::net::SocketAddr>()?;

        tracing::info!("flight addr: {}", addr);

        // - boot mode: create the first node in a new cluster.
        // - TODO(xp): join mode: create a new node to join a cluster.
        // - open mode: open an existent node.
        tracing::info!(
            "Starting MetaNode boot:{} single: {} with config: {:?}",
            self.conf.raft_config.boot,
            self.conf.raft_config.single,
            self.conf
        );

        let meta_config = &self.conf.raft_config;

        let mn = if meta_config.boot {
            MetaNode::boot(0, meta_config).await?
        } else if meta_config.single {
            let (mn, _is_open) =
                MetaNode::open_create_boot(meta_config, Some(()), Some(()), Some(())).await?;
            mn
        } else {
            MetaNode::open(meta_config).await?
        };
        tracing::info!("Done starting MetaNode: {:?}", self.conf);

        let flight_impl = MetaFlightImpl::create(self.conf.clone(), mn.clone());
        let flight_srv = FlightServiceServer::new(flight_impl);

        let builder = Server::builder();
        let _conf = &self.conf;

        let mut builder = if let Some(conf) = tls_conf {
            tracing::info!("TLS RPC enabled");
            builder.tls_config(conf).map_err(|e| {
                ErrorCode::TLSConfigurationFailure(format!(
                    "server tls_config failure {}",
                    e.to_string()
                ))
            })?
        } else {
            builder
        };

        let res = builder
            .add_service(flight_srv)
            .serve_with_shutdown(addr, async move {
                tracing::info!("metasrv start to wait for stop signal: {}", addr);
                let _ = stop_rx.await;
                tracing::info!("metasrv receives stop signal: {}", addr);
            })
            .await;

        let _ = mn.stop().await;
        let s = fin_tx.send(());
        tracing::info!(
            "metasrv sending signal of finishing shutdown {}: res: {:?}",
            addr,
            s
        );

        tracing::info!("metasrv returning");

        res.map_err_to_code(ErrorCode::KVSrvError, || "metasrv error")
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
