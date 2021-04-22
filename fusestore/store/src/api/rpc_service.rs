// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::anyhow;
use anyhow::Result;
use common_arrow::arrow_flight::flight_service_server::FlightServiceServer;
use tonic::transport::Server;

use crate::api::rpc::StoreFlightImpl;
use crate::configs::Config;
use crate::localfs::LocalFS;

pub struct StoreServer {
    conf: Config
}

impl StoreServer {
    pub fn create(conf: Config) -> Self {
        Self { conf }
    }

    pub async fn serve(&self) -> Result<()> {
        let addr = self.conf.rpc_api_address.parse::<std::net::SocketAddr>()?;

        let p = tempfile::tempdir()?;
        let fs = LocalFS::try_create(p.path().to_str().unwrap().into())?;

        // Flight service:
        let flight_impl = StoreFlightImpl::create(self.conf.clone(), Arc::new(fs));
        let flight_srv = FlightServiceServer::new(flight_impl);

        Server::builder()
            .add_service(flight_srv)
            .serve(addr)
            .await
            .map_err(|e| anyhow!("Flight service error: {:?}", e))
    }
}
