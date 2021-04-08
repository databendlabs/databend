// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::anyhow;
use anyhow::Result;
use tonic::transport::Server;

use crate::api::rpc::FlightServiceImpl;
use crate::configs::Config;

pub struct RpcService {
    conf: Config,
}

impl RpcService {
    pub fn create(conf: Config) -> Self {
        Self { conf }
    }

    pub async fn make_server(&self) -> Result<()> {
        let addr = self.conf.rpc_api_address.parse::<std::net::SocketAddr>()?;

        // Flight service:
        let flight_srv = FlightServiceImpl::create(self.conf.clone());

        Server::builder()
            .add_service(flight_srv.make_server())
            .serve(addr)
            .await
            .map_err(|e| anyhow!("Flight service error: {:?}", e))
    }
}
