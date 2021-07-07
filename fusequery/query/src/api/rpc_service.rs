// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow_flight::flight_service_server::FlightServiceServer;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use tonic::transport::Server;

use crate::api::rpc::FlightDispatcher;
use crate::api::rpc::FuseQueryService;
use crate::configs::Config;
use crate::sessions::SessionMgrRef;

pub struct RpcService {
    conf: Config,
    session_mgr: SessionMgrRef,
}

impl RpcService {
    pub fn create(conf: Config, session_mgr: SessionMgrRef) -> Self {
        Self { conf, session_mgr }
    }

    pub async fn make_server(&self) -> Result<()> {
        let addr = self
            .conf
            .flight_api_address
            .parse::<std::net::SocketAddr>()?;

        let flight_dispatcher = FlightDispatcher::new(self.conf.clone(), self.session_mgr.clone());

        // Flight service:
        let dispatcher_request_sender = flight_dispatcher.run();
        let service = FuseQueryService::create(dispatcher_request_sender);

        Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve(addr)
            .await
            .map_err_to_code(ErrorCode::CannotListenerPort, || {
                format!("Cannot listener port {}", addr)
            })
    }
}
