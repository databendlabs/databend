// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::net::SocketAddr;
use std::sync::Arc;

use common_base::tokio;
use common_base::tokio::net::TcpStream;
use common_base::tokio::task::JoinHandle;
use common_base::Runtime;
use common_base::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing;
use futures::future::AbortHandle;
use futures::future::AbortRegistration;
use futures::stream::Abortable;
use futures::Future;
use futures::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;

use crate::servers::clickhouse::clickhouse_session::ClickHouseConnection;
use crate::servers::clickhouse::reject_connection::RejectCHConnection;
use crate::servers::server::ListeningStream;
use crate::servers::server::Server;
use crate::sessions::SessionManager;

pub struct ClickHouseHandler {
    sessions: Arc<SessionManager>,

    abort_handle: AbortHandle,
    abort_registration: Option<AbortRegistration>,
    join_handle: Option<JoinHandle<()>>,
}

impl ClickHouseHandler {
    pub fn create(sessions: Arc<SessionManager>) -> Box<dyn Server> {
        let (abort_handle, registration) = AbortHandle::new_pair();
        Box::new(ClickHouseHandler {
            sessions,
            abort_handle,
            abort_registration: Some(registration),
            join_handle: None,
        })
    }

    async fn listener_tcp(socket: SocketAddr) -> Result<(TcpListenerStream, SocketAddr)> {
        let listener = tokio::net::TcpListener::bind(socket).await.map_err(|e| {
            ErrorCode::TokioError(format!("{{{}:{}}} {}", socket.ip(), socket.port(), e))
        })?;
        let listener_addr = listener.local_addr()?;
        Ok((TcpListenerStream::new(listener), listener_addr))
    }

    fn listen_loop(&self, stream: ListeningStream, r: Arc<Runtime>) -> impl Future<Output = ()> {
        let sessions = self.sessions.clone();
        stream.for_each(move |accept_socket| {
            let executor = r.clone();
            let sessions = sessions.clone();
            async move {
                match accept_socket {
                    Err(error) => tracing::error!("Broken session connection: {}", error),
                    Ok(socket) => ClickHouseHandler::accept_socket(sessions, executor, socket),
                };
            }
        })
    }

    fn reject_connection(stream: TcpStream, executor: Arc<Runtime>, error: ErrorCode) {
        executor.spawn(async move {
            if let Err(error) = RejectCHConnection::reject(stream, error).await {
                tracing::error!(
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
                tracing::info!("ClickHouse connection coming: {:?}", socket.peer_addr());
                if let Err(error) = ClickHouseConnection::run_on_stream(session, socket) {
                    tracing::error!("Unexpected error occurred during query: {:?}", error);
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Server for ClickHouseHandler {
    async fn shutdown(&mut self, graceful: bool) {
        if !graceful {
            return;
        }
        self.abort_handle.abort();

        if let Some(join_handle) = self.join_handle.take() {
            if let Err(error) = join_handle.await {
                tracing::error!(
                    "Unexpected error during shutdown ClickHouseHandler. cause {}",
                    error
                );
            }
        }
    }

    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        match self.abort_registration.take() {
            None => Err(ErrorCode::LogicalError(
                "ClickHouseHandler already running.",
            )),
            Some(registration) => {
                let rejected_rt = Arc::new(Runtime::with_worker_threads(1)?);
                let (stream, listener) = Self::listener_tcp(listening).await?;
                let stream = Abortable::new(stream, registration);
                self.join_handle = Some(tokio::spawn(self.listen_loop(stream, rejected_rt)));
                Ok(listener)
            }
        }
    }
}
