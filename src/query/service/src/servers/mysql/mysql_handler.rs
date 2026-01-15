// Copyright 2021 Datafuse Labs
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

use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use futures::StreamExt;
use futures::future::AbortHandle;
use futures::future::AbortRegistration;
use futures::future::Abortable;
use log::error;
use rustls::ServerConfig;
use socket2::TcpKeepalive;
use tokio;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;

use crate::servers::mysql::mysql_session::MySQLConnection;
use crate::servers::mysql::tls::MySQLTlsConfig;
use crate::servers::server::ListeningStream;
use crate::servers::server::Server;
use crate::sessions::SessionManager;

pub struct MySQLHandler {
    abort_handle: AbortHandle,
    abort_registration: Option<AbortRegistration>,
    join_handle: Option<JoinHandle<()>>,
    keepalive: TcpKeepalive,
    tls: Option<Arc<ServerConfig>>,
}

impl MySQLHandler {
    pub fn create(
        tcp_keepalive_timeout_secs: u64,
        tls_config: MySQLTlsConfig,
    ) -> Result<Box<dyn Server>> {
        let (abort_handle, registration) = AbortHandle::new_pair();
        let keepalive = TcpKeepalive::new()
            .with_time(std::time::Duration::from_secs(tcp_keepalive_timeout_secs));
        let tls = tls_config.setup()?.map(Arc::new);

        Ok(Box::new(MySQLHandler {
            abort_handle,
            abort_registration: Some(registration),
            join_handle: None,
            keepalive,
            tls,
        }))
    }

    #[async_backtrace::framed]
    async fn listener_tcp(listening: SocketAddr) -> Result<(TcpListenerStream, SocketAddr)> {
        let listener = tokio::net::TcpListener::bind(listening)
            .await
            .map_err(|e| {
                ErrorCode::TokioError(format!("{{{}:{}}} {}", listening.ip(), listening.port(), e))
            })?;
        let listener_addr = listener.local_addr()?;
        Ok((TcpListenerStream::new(listener), listener_addr))
    }

    fn listen_loop(
        &self,
        stream: ListeningStream,
        rt: Arc<Runtime>,
    ) -> impl Future<Output = ()> + use<> {
        let keepalive = self.keepalive.clone();
        let tls = self.tls.clone();

        stream.for_each(move |accept_socket| {
            let tls = tls.clone();
            let keepalive = keepalive.clone();
            let executor = rt.clone();
            let sessions = SessionManager::instance();
            async move {
                match accept_socket {
                    Err(error) => error!("Broken session connection: {}", error),
                    Ok(socket) => {
                        MySQLHandler::accept_socket(sessions, executor, socket, keepalive, tls)
                    }
                };
            }
        })
    }

    fn accept_socket(
        session_manager: Arc<SessionManager>,
        executor: Arc<Runtime>,
        socket: TcpStream,
        keepalive: TcpKeepalive,
        tls: Option<Arc<ServerConfig>>,
    ) {
        executor.spawn(async move {
            if let Err(error) =
                MySQLConnection::run_on_stream(session_manager, socket, keepalive, tls).await
            {
                error!("Unexpected error occurred during query: {:?}", error);
            }
        });
    }
}

#[async_trait::async_trait]
impl Server for MySQLHandler {
    #[async_backtrace::framed]
    async fn shutdown(&mut self, graceful: bool) {
        if !graceful {
            return;
        }

        self.abort_handle.abort();

        if let Some(join_handle) = self.join_handle.take() {
            if let Err(error) = join_handle.await {
                error!(
                    "Unexpected error during shutdown MySQLHandler. cause {}",
                    error
                );
            }
        }
    }

    #[async_backtrace::framed]
    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        match self.abort_registration.take() {
            None => Err(ErrorCode::Internal("MySQLHandler already running.")),
            Some(registration) => {
                let rejected_rt = Arc::new(Runtime::with_worker_threads(
                    1,
                    Some("mysql-handler".to_string()),
                )?);
                let (stream, listener) = Self::listener_tcp(listening).await?;
                let stream = Abortable::new(stream, registration);
                self.join_handle = Some(databend_common_base::runtime::spawn(
                    self.listen_loop(stream, rejected_rt),
                ));
                Ok(listener)
            }
        }
    }
}
