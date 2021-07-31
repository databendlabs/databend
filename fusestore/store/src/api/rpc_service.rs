// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::anyhow;
use common_arrow::arrow_flight::flight_service_server::FlightServiceServer;
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

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn serve(&self) -> anyhow::Result<()> {
        let addr = self
            .conf
            .flight_api_address
            .parse::<std::net::SocketAddr>()?;

        tracing::info!("flight addr: {}", addr);

        // TODO(xp): add local fs dir to config and use it.
        let p = tempfile::tempdir()?;
        let fs = LocalFS::try_create(p.path().to_str().unwrap().into())?;

        // TODO(xp): support non-boot mode.
        //           for now it can only be run in single-node cluster mode.
        // if !self.conf.boot {
        //     todo!("non-boot mode is not impl yet")
        // }

        tracing::info!("--- starting MetaNode with config: {:?}", self.conf);

        let mn = MetaNode::boot(0, &self.conf).await?;

        tracing::info!("boot done");

        let dfs = Dfs::create(fs, mn.clone());

        let flight_impl = StoreFlightImpl::create(self.conf.clone(), Arc::new(dfs), mn);
        let flight_srv = FlightServiceServer::new(flight_impl);

        Server::builder()
            .add_service(flight_srv)
            .serve(addr)
            .await
            .map_err(|e| anyhow!("Flight service error: {:?}", e))
    }
}
