// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use clickhouse_srv::connection::Connection;
use clickhouse_srv::errors::ServerError;
use clickhouse_srv::types::Block as ClickHouseBlock;
use clickhouse_srv::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_runtime::tokio;
use common_runtime::tokio::net::TcpListener;
use common_runtime::tokio::net::TcpStream;
use common_runtime::tokio::sync::mpsc;
use common_runtime::tokio::time;
use futures::Future;
use futures::StreamExt;
use log::error;
use metrics::histogram;
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::wrappers::TcpListenerStream;

use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::interpreters::InterpreterFactory;
use crate::servers::clickhouse::clickhouse_session::ClickHouseConnection;
use crate::servers::clickhouse::interactive_worker::InteractiveWorker;
use crate::servers::clickhouse::ClickHouseStream;
use crate::servers::AbortableServer;
use crate::servers::AbortableService;
use crate::servers::Elapsed;
use crate::sessions::FuseQueryContextRef;
use crate::sessions::SessionManager;
use crate::sessions::SessionManagerRef;
use crate::sql::PlanParser;

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
