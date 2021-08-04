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
use tonic::transport::Server;

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
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let (fin_tx, fin_rx) = oneshot::channel::<()>();

        let fut = self.serve(stop_rx, fin_tx);
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
    pub async fn serve(self, stop_rx: Receiver<()>, fin_tx: Sender<()>) -> Result<(), ErrorCode> {
        let addr = self
            .conf
            .flight_api_address
            .parse::<std::net::SocketAddr>()?;

        tracing::info!("flight addr: {}", addr);

        // TODO(xp): add local fs dir to config and use it.
        let p = tempfile::tempdir()?;
        let fs = LocalFS::try_create(p.path().to_str().unwrap().into())?;

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
        tracing::info!("Done starting MetaNode");

        let dfs = Dfs::create(fs, mn.clone());

        let flight_impl = StoreFlightImpl::create(self.conf.clone(), Arc::new(dfs), mn.clone());
        let flight_srv = FlightServiceServer::new(flight_impl);

        let res = Server::builder()
            .add_service(flight_srv)
            .serve_with_shutdown(addr, async move {
                tracing::info!("StoreServer start to wait for stop signal");
                let _ = stop_rx.await;
                tracing::info!("StoreServer receives stop signal");
            })
            .await;

        let _ = mn.stop().await;
        let s = fin_tx.send(());
        tracing::info!(
            "StoreServer sending signal of finishing shutdown: res: {:?}",
            s
        );

        tracing::info!("StoreServer returning");

        res.map_err_to_code(ErrorCode::FuseStoreError, || "StoreServer error")
    }
}
