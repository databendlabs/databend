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
    abort_notify: Arc<Notify>,
    dispatcher: Arc<FuseQueryFlightDispatcher>,
}

impl RpcService {
    pub fn create(sessions: SessionManagerRef) -> AbortableServer {
        Arc::new(Self {
            sessions,
            abort_notify: Arc::new(Notify::new()),
            dispatcher: Arc::new(FuseQueryFlightDispatcher::create()),
        })
    }

    async fn listener_tcp(address: (String, u16)) -> Result<(TcpListenerStream, SocketAddr)> {
        let listener = TcpListener::bind(address).await?;
        let listener_addr = listener.local_addr()?;
        Ok((TcpListenerStream::new(listener), listener_addr))
    }
}

#[async_trait::async_trait]
impl AbortableService<(String, u16), SocketAddr> for RpcService {
    fn abort(&self, force: bool) -> Result<()> {
        match force {
            false => self.dispatcher.abort(),
            true => {
                self.dispatcher.abort();
                self.abort_notify.notify_waiters();
            }
        };

        Ok(())
    }

    async fn start(&self, addr: (String, u16)) -> Result<SocketAddr> {
        let sessions = self.sessions.clone();
        let shutdown_notify = self.abort_notify.clone();
        let flight_dispatcher = self.dispatcher.clone();
        let flight_api_service = FuseQueryFlightService::create(flight_dispatcher, sessions);

        let (listener_stream, socket) = Self::listener_tcp(addr).await?;

        common_runtime::tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(FlightServiceServer::new(flight_api_service))
                .serve_with_incoming_shutdown(listener_stream, shutdown_notify.notified())
                .await;
        });

        Ok(socket)
    }

    async fn wait_terminal(&self, _duration: Option<Duration>) -> Result<Elapsed> {
        unimplemented!()
    }
}
