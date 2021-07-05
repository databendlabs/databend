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
use crate::sessions::SessionManagerRef;
use crate::servers::Server as FuseQueryServer;
use std::future::Future;

pub struct RpcService {
    sessions: SessionManagerRef,
    abort_notify: Arc<Notify>,
    dispatcher: Arc<FuseQueryFlightDispatcher>,
}

impl RpcService {
    pub fn create(sessions: SessionManagerRef) -> Box<dyn FuseQueryServer> {
        Box::new(Self {
            sessions,
            abort_notify: Arc::new(Notify::new()),
            dispatcher: Arc::new(FuseQueryFlightDispatcher::create()),
        })
    }

    async fn listener_tcp(listening: SocketAddr) -> Result<(TcpListenerStream, SocketAddr)> {
        let listener = TcpListener::bind(listening).await?;
        let listener_addr = listener.local_addr()?;
        Ok((TcpListenerStream::new(listener), listener_addr))
    }

    fn shutdown_notify(&self) -> impl Future<Output=()> + 'static {
        let notified = self.abort_notify.clone();
        async move { notified.notified().await; }
    }
}

#[async_trait::async_trait]
impl FuseQueryServer for RpcService {
    async fn shutdown(&mut self) {
        self.dispatcher.abort();
        // We can't turn off listening on the connection
        // self.abort_notify.notify_waiters();
    }

    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        let sessions = self.sessions.clone();
        let flight_dispatcher = self.dispatcher.clone();
        let flight_api_service = FuseQueryFlightService::create(flight_dispatcher, sessions);

        let (listener_stream, listening) = Self::listener_tcp(listening).await?;
        let server = Server::builder()
            .add_service(FlightServiceServer::new(flight_api_service))
            .serve_with_incoming_shutdown(listener_stream, self.shutdown_notify());

        Ok(listening)
    }
}
