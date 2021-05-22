// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::anyhow;
use anyhow::Result;
use tonic::transport::Server;

use crate::api::rpc::{FuseQueryService, FlightDispatcher};
use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::sessions::SessionManagerRef;
use common_arrow::arrow_flight::flight_service_server::FlightServiceServer;

pub struct RpcService {
    conf: Config,
    cluster: ClusterRef,
    session_manager: SessionManagerRef
}

impl RpcService {
    pub fn create(conf: Config, cluster: ClusterRef, session_manager: SessionManagerRef) -> Self {
        Self {
            conf,
            cluster,
            session_manager
        }
    }

    pub async fn make_server(&self) -> Result<()> {
        let addr = self
            .conf
            .flight_api_address
            .parse::<std::net::SocketAddr>()?;

        let flight_dispatcher = FlightDispatcher::new(
            self.conf.clone(),
            self.cluster.clone(),
            self.session_manager.clone(),
        );

        // Flight service:
        let dispatcher_request_sender = flight_dispatcher.run();
        let service = FuseQueryService::create(dispatcher_request_sender);

        Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve(addr)
            .await
            .map_err(|e| anyhow!("Flight make sever error: {:?}", e))
    }
}
