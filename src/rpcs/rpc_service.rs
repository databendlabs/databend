// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use tonic::transport::Server;

use crate::configs::Config;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::rpcs::rpc::{ExecutorRPCService, FlightService};

pub struct RpcService {
    conf: Config,
}

impl RpcService {
    pub fn create(conf: Config) -> Self {
        RpcService { conf }
    }

    pub async fn make_server(&self) -> FuseQueryResult<()> {
        let addr = self.conf.rpc_api_address.parse::<std::net::SocketAddr>()?;

        Server::builder()
            .add_service(ExecutorRPCService::make_server())
            .add_service(FlightService::make_server())
            .serve(addr)
            .await
            .map_err(|e| {
                FuseQueryError::Internal(format!("Metrics prometheus exporter error: {:?}", e))
            })
    }
}
