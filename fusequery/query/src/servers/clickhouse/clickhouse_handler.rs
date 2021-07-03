// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common_exception::Result;
use common_runtime::tokio;
use common_runtime::tokio::net::TcpStream;
use futures::Future;
use futures::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;

use crate::servers::clickhouse::clickhouse_session::ClickHouseConnection;
use crate::servers::AbortableServer;
use crate::servers::AbortableService;
use crate::servers::Elapsed;
use crate::sessions::SessionManager;
use crate::sessions::SessionManagerRef;

pub struct ClickHouseHandler {
    sessions: SessionManagerRef,
}

impl ClickHouseHandler {
    pub fn create(sessions: SessionManagerRef) -> AbortableServer {
        Arc::new(ClickHouseHandler { sessions })
    }

    async fn listener_tcp(socket: (String, u16)) -> Result<(TcpListenerStream, SocketAddr)> {
        let listener = tokio::net::TcpListener::bind(socket).await?;
        let listener_addr = listener.local_addr()?;
        Ok((TcpListenerStream::new(listener), listener_addr))
    }

    fn listener_loop(&self, stream: TcpListenerStream) -> impl Future<Output = ()> {
        let sessions = self.sessions.clone();
        stream.for_each(move |accept_socket| {
            let sessions = sessions.clone();
            async move {
                match accept_socket {
                    Err(error) => log::error!("Broken session connection: {}", error),
                    Ok(socket) => ClickHouseHandler::accept_socket(sessions, socket),
                };
            }
        })
    }

    fn accept_socket(sessions: Arc<SessionManager>, socket: TcpStream) {
        match sessions.create_session("ClickHouseSession") {
            Err(error) => {}
            Ok(session) => {
                ClickHouseConnection::run_on_stream(session, socket);
            }
        }
    }
}

#[async_trait::async_trait]
impl AbortableService<(String, u16), SocketAddr> for ClickHouseHandler {
    fn abort(&self, force: bool) -> Result<()> {
        todo!()
    }

    async fn start(&self, socket: (String, u16)) -> Result<SocketAddr> {
        let (stream, listener) = Self::listener_tcp(socket).await?;
        tokio::spawn(self.listener_loop(stream));
        Ok(listener)
    }

    async fn wait_terminal(&self, duration: Option<Duration>) -> Result<Elapsed> {
        todo!()
    }
}
