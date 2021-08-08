// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_arrow::arrow_flight::flight_service_server::FlightServiceServer;
use common_exception::ErrorCode;
use common_exception::ToErrorCode;
use common_runtime::tokio;
use common_runtime::tokio::sync::oneshot;
use common_runtime::tokio::sync::oneshot::Receiver;
use common_runtime::tokio::sync::oneshot::Sender;
use common_tracing::tracing;
use tonic::transport;
use tonic::transport::Identity;
use tonic::transport::Server;
use transport::ServerTlsConfig;

use crate::api::rpc::StoreFlightImpl;
use crate::configs::Config;
use crate::dfs::Dfs;
use crate::localfs::LocalFS;
use crate::meta_service::MetaNode;

pub struct StoreServer {
    conf: Config,
}

impl StoreServer {
    pub fn create(conf: Config) -> Self {
        Self { conf }
    }

    /// Start store server and returns two channel to send shutdown signal and receive signal when shutdown finished.
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
        tokio::spawn(async move {
            // TODO(xp): handle errors.
            // TODO(xp): move server building up actions out of serve(). errors should be caught.
            let res = fut.await;
            tracing::info!("StoreServer serve res: {:?}", res);
        });

        Ok((stop_tx, fin_rx))
    }

    /// Start serving FuseStore. It does not return until StoreServer is stopped.
    #[tracing::instrument(level = "debug", skip(self))]
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

        let fs = LocalFS::try_create(self.conf.local_fs_dir.clone())?;

        // - boot mode: create the first node in a new cluster.
        // - TODO(xp): join mode: create a new node to join a cluster.
        // - open mode: open an existent node.
        tracing::info!(
            "Starting MetaNode boot:{} single: {} with config: {:?}",
            self.conf.boot,
            self.conf.single,
            self.conf
        );

        let mn = if self.conf.boot {
            MetaNode::boot(0, &self.conf).await?
        } else if self.conf.single {
            let (mn, _is_open) =
                MetaNode::open_create_boot(&self.conf, Some(()), Some(()), Some(())).await?;
            mn
        } else {
            MetaNode::open(&self.conf).await?
        };
        tracing::info!("Done starting MetaNode: {:?}", self.conf);

        let dfs = Dfs::create(fs, mn.clone());

        let flight_impl = StoreFlightImpl::create(self.conf.clone(), Arc::new(dfs), mn.clone());
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
                tracing::info!("StoreServer start to wait for stop signal: {}", addr);
                let _ = stop_rx.await;
                tracing::info!("StoreServer receives stop signal: {}", addr);
            })
            .await;

        let _ = mn.stop().await;
        let s = fin_tx.send(());
        tracing::info!(
            "StoreServer sending signal of finishing shutdown {}: res: {:?}",
            addr,
            s
        );

        tracing::info!("StoreServer returning");

        res.map_err_to_code(ErrorCode::FuseStoreError, || "StoreServer error")
    }

    async fn tls_config(conf: &Config) -> anyhow::Result<Option<ServerTlsConfig>> {
        if conf.tls_rpc_server_enabled() {
            let cert = tokio::fs::read(conf.rpc_tls_server_cert.as_str()).await?;
            let key = tokio::fs::read(conf.rpc_tls_server_key.as_str()).await?;
            let server_identity = Identity::from_pem(cert, key);

            let tls = ServerTlsConfig::new().identity(server_identity);
            Ok(Some(tls))
        } else {
            Ok(None)
        }
    }
}
