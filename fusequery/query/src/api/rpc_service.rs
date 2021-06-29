// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::SocketAddr;
use std::sync::Arc;

use common_arrow::arrow_flight::flight_service_server::FlightServiceServer;
use common_exception::Result;
use common_runtime::tokio::net::TcpListener;
use common_runtime::tokio::sync::Notify;
use common_runtime::tokio::time::Duration;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::api::rpc::FuseQueryFlightDispatcher;
use crate::api::rpc::FuseQueryFlightService;
use crate::servers::AbortableServer;
use crate::servers::AbortableService;
use crate::servers::Elapsed;
use crate::sessions::SessionManagerRef;

pub struct RpcService {
    sessions: SessionManagerRef,
    abort_notify: Notify,
}

impl RpcService {
    pub fn create(sessions: SessionManagerRef) -> AbortableServer {
        Arc::new(Self {
            sessions,
            abort_notify: Notify::new(),
        })
    }
}

#[async_trait::async_trait]
impl AbortableService<(String, u16), SocketAddr> for RpcService {
    fn abort(&self, force: bool) -> Result<()> {
        self.abort_notify.notify_waiters();
        Ok(())
    }

    async fn start(&self, addr: (String, u16)) -> Result<SocketAddr> {
        let sessions = self.sessions.clone();
        let listener = TcpListener::bind(addr).await?;
        let listener_socket = listener.local_addr()?;
        let flight_dispatcher = FuseQueryFlightDispatcher::create();
        let flight_service = FuseQueryFlightService::create(flight_dispatcher, sessions);

        Server::builder()
            .add_service(FlightServiceServer::new(flight_service))
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                self.abort_notify.notified(),
            );

        Ok(listener_socket)
    }

    async fn wait_terminal(&self, duration: Option<Duration>) -> Result<Elapsed> {
        unimplemented!()
    }
}
