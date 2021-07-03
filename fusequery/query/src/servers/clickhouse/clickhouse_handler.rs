// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::Mutex;
use common_runtime::tokio;
use common_runtime::tokio::net::TcpStream;
use common_runtime::Runtime;
use futures::future::AbortHandle;
use futures::future::AbortRegistration;
use futures::stream::Abortable;
use futures::Future;
use futures::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;

use crate::servers::clickhouse::clickhouse_session::ClickHouseConnection;
use crate::servers::clickhouse::reject_connection::RejectCHConnection;
use crate::servers::AbortableServer;
use crate::servers::AbortableService;
use crate::servers::Elapsed;
use crate::sessions::SessionManager;
use crate::sessions::SessionManagerRef;

pub struct ClickHouseHandler {
    sessions: SessionManagerRef,

    abort_handle: Arc<AbortHandle>,
    registration: Mutex<Option<AbortRegistration>>,
}

impl ClickHouseHandler {
    pub fn create(sessions: SessionManagerRef) -> AbortableServer {
        let (abort_handle, reg) = AbortHandle::new_pair();
        Arc::new(ClickHouseHandler {
            sessions,
            abort_handle: Arc::new(abort_handle),
            registration: Mutex::new(Some(reg)),
        })
    }

    async fn listener_tcp(socket: (String, u16)) -> Result<(TcpListenerStream, SocketAddr)> {
        let listener = tokio::net::TcpListener::bind(socket).await?;
        let listener_addr = listener.local_addr()?;
        Ok((TcpListenerStream::new(listener), listener_addr))
    }

    fn listen_loop(&self, stream: TcpListenerStream, r: Arc<Runtime>) -> impl Future<Output=()> {
        let sessions = self.sessions.clone();
        let registration = self.registration.lock().take().unwrap();
        Abortable::new(stream, registration).for_each(move |accept_socket| {
            let executor = r.clone();
            let sessions = sessions.clone();
            async move {
                match accept_socket {
                    Err(error) => log::error!("Broken session connection: {}", error),
                    Ok(socket) => ClickHouseHandler::accept_socket(sessions, executor, socket),
                };
            }
        })
    }

    fn reject_connection(stream: TcpStream, executor: Arc<Runtime>, error: ErrorCode) {
        executor.spawn(async move {
            if let Err(error) = RejectCHConnection::reject(stream, error).await {
                log::error!(
                    "Unexpected error occurred during reject connection: {:?}",
                    error
                );
            }
        });
    }

    fn accept_socket(sessions: Arc<SessionManager>, executor: Arc<Runtime>, socket: TcpStream) {
        match sessions.create_session("ClickHouseSession") {
            Err(error) => Self::reject_connection(socket, executor, error),
            Ok(session) => {
                if let Err(error) = ClickHouseConnection::run_on_stream(session, socket) {
                    log::error!("Unexpected error occurred during query: {:?}", error);
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl AbortableService<(String, u16), SocketAddr> for ClickHouseHandler {
    fn abort(&self, _force: bool) -> Result<()> {
        self.abort_handle.abort();
        Ok(())
    }

    async fn start(&self, socket: (String, u16)) -> Result<SocketAddr> {
        let rejected_executor = Arc::new(Runtime::with_worker_threads(1)?);
        let (stream, listener) = Self::listener_tcp(socket).await?;
        tokio::spawn(self.listen_loop(stream, rejected_executor));
        Ok(listener)
    }

    async fn wait_terminal(&self, _: Option<Duration>) -> Result<Elapsed> {
        todo!()
    }
}
