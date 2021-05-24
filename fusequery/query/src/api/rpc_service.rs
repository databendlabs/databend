// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::anyhow;
use anyhow::Result;
use tonic::transport::Server;

use crate::api::rpc::FlightService;
use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::sessions::SessionManagerRef;

pub struct RpcService {
    conf: Config,
    cluster: ClusterRef,
    session_manager: SessionManagerRef,
}

impl RpcService {
    pub fn create(conf: Config, cluster: ClusterRef, session_manager: SessionManagerRef) -> Self {
        Self {
            conf,
            cluster,
            session_manager,
        }
    }

    pub async fn make_server(&self) -> Result<()> {
        let addr = self
            .conf
            .flight_api_address
            .parse::<std::net::SocketAddr>()?;

        // Flight service:
        let flight_srv = FlightService::create(
            self.conf.clone(),
            self.cluster.clone(),
            self.session_manager.clone(),
        );

        Server::builder()
            .add_service(flight_srv.make_server())
            .serve(addr)
            .await
            .map_err(|e| anyhow!("Flight make sever error: {:?}", e))
    }
}
