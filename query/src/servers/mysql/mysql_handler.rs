// Copyright 2020 Datafuse Labs.
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

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use common_base::tokio;
use common_base::tokio::net::TcpStream;
use common_base::tokio::task::JoinHandle;
use common_base::Runtime;
use common_base::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::future::AbortHandle;
use futures::future::AbortRegistration;
use futures::future::Abortable;
use futures::StreamExt;
use msql_srv::*;
use tokio_stream::wrappers::TcpListenerStream;

use crate::servers::mysql::mysql_session::MySQLConnection;
use crate::servers::mysql::reject_connection::RejectConnection;
use crate::servers::server::ListeningStream;
use crate::servers::server::Server;
use crate::sessions::SessionManager;
use crate::sessions::SessionManagerRef;

pub struct MySQLHandler {
    sessions: SessionManagerRef,
    abort_handle: AbortHandle,
    abort_registration: Option<AbortRegistration>,
    join_handle: Option<JoinHandle<()>>,
}

impl MySQLHandler {
    pub fn create(sessions: SessionManagerRef) -> Box<dyn Server> {
        let (abort_handle, registration) = AbortHandle::new_pair();
        Box::new(MySQLHandler {
            sessions,
            abort_handle,
            abort_registration: Some(registration),
            join_handle: None,
        })
    }

    async fn listener_tcp(listening: SocketAddr) -> Result<(TcpListenerStream, SocketAddr)> {
        let listener = tokio::net::TcpListener::bind(listening)
            .await
            .map_err(|e| {
                ErrorCode::TokioError(format!(
                    "{{{}:{}}} {}",
                    listening.ip().to_string(),
                    listening.port().to_string(),
                    e
                ))
            })?;
        let listener_addr = listener.local_addr()?;
        Ok((TcpListenerStream::new(listener), listener_addr))
    }

    fn listen_loop(&self, stream: ListeningStream, rt: Arc<Runtime>) -> impl Future<Output = ()> {
        let sessions = self.sessions.clone();
        stream.for_each(move |accept_socket| {
            let executor = rt.clone();
            let sessions = sessions.clone();
            async move {
                match accept_socket {
                    Err(error) => log::error!("Broken session connection: {}", error),
                    Ok(socket) => MySQLHandler::accept_socket(sessions, executor, socket),
                };
            }
        })
    }

    fn accept_socket(sessions: Arc<SessionManager>, executor: Arc<Runtime>, socket: TcpStream) {
        match sessions.create_session("MySQL") {
            Err(error) => Self::reject_session(socket, executor, error),
            Ok(session) => {
                log::info!("MySQL connection coming: {:?}", socket.peer_addr());
                if let Err(error) = MySQLConnection::run_on_stream(session, socket) {
                    log::error!("Unexpected error occurred during query: {:?}", error);
                };
            }
        }
    }

    fn reject_session(stream: TcpStream, executor: Arc<Runtime>, error: ErrorCode) {
        executor.spawn(async move {
            let (kind, message) = match error.code() {
                41 => (ErrorKind::ER_TOO_MANY_USER_CONNECTIONS, error.message()),
                _ => (ErrorKind::ER_INTERNAL_ERROR, error.message()),
            };

            if let Err(error) =
                RejectConnection::reject_mysql_connection(stream, kind, message).await
            {
                log::error!(
                    "Unexpected error occurred during reject connection: {:?}",
                    error
                );
            }
        });
    }
}

#[async_trait::async_trait]
impl Server for MySQLHandler {
    async fn shutdown(&mut self, graceful: bool) {
        if !graceful {
            return;
        }

        self.abort_handle.abort();

        if let Some(join_handle) = self.join_handle.take() {
            if let Err(error) = join_handle.await {
                log::error!(
                    "Unexpected error during shutdown MySQLHandler. cause {}",
                    error
                );
            }
        }
    }

    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        match self.abort_registration.take() {
            None => Err(ErrorCode::LogicalError("MySQLHandler already running.")),
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
