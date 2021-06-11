// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::SocketAddr;
use std::sync::Arc;

use futures::future::{AbortHandle, Abortable};
use common_exception::ErrorCode;
use common_exception::Result;
use msql_srv::*;
use tokio::net::TcpStream;
use tokio_stream::StreamExt as OtherStreamExt;

use common_exception::ToErrorCode;
use common_infallible::Mutex;

use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::servers::mysql::mysql_session::Session;
use crate::servers::{RunnableService, Elapsed};
use crate::sessions::SessionManagerRef;
use tokio_stream::wrappers::TcpListenerStream;
use crate::servers::mysql::reject_connection::RejectConnection;
use common_runtime::Runtime;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio::sync::broadcast::Sender;
use std::ops::Sub;

pub struct MySQLHandler {
    session_manager: SessionManagerRef,

    abort_handler: Mutex<Option<AbortHandle>>,
    terminal_sender: Arc<Mutex<Option<Sender<()>>>>,
}

impl MySQLHandler {
    pub fn create(session_manager: SessionManagerRef) -> Self {
        MySQLHandler {
            session_manager,
            abort_handler: Mutex::new(None),
            terminal_sender: Arc::new(Mutex::new(None)),
        }
    }

    pub fn started(&self, handler: JoinHandle<()>, abort_handler: AbortHandle) {
        {
            *self.abort_handler.lock() = Some(abort_handler);
            let (terminal_sender, _) = tokio::sync::broadcast::channel(1);
            *self.terminal_sender.lock() = Some(terminal_sender);
        }

        let terminal_sender = self.terminal_sender.clone();

        tokio::spawn(async move {
            if let Err(error) = handler.await {
                log::error!("Cannot join: {}", error);
            }

            if let Some(terminal_watchers) = terminal_sender.lock().take() {
                let _ = terminal_watchers.send(());
            }
        });
    }

    async fn listener_tcp(hostname: &str, port: u16) -> Result<(TcpListenerStream, SocketAddr)> {
        let address = format!("{}:{}", hostname, port);
        let listener = tokio::net::TcpListener::bind(address).await?;
        let listener_addr = listener.local_addr()?;
        Ok((TcpListenerStream::new(listener), listener_addr))
    }

    fn reject_session(mut stream: TcpStream, executor: &Runtime, error: ErrorCode) {
        executor.spawn(async move {
            let (kind, message) = match error.code() {
                41 => (ErrorKind::ER_TOO_MANY_USER_CONNECTIONS, error.message()),
                _ => (ErrorKind::ER_INTERNAL_ERROR, error.message())
            };

            if let Err(error) = RejectConnection::reject_mysql_connection(stream, kind, message).await {
                log::error!("Unexpected error occurred during reject connection: {:?}", error);
            }
        });
    }
}

#[async_trait::async_trait]
impl RunnableService<(String, u16), SocketAddr> for MySQLHandler {
    fn abort(&self, force: bool) {
        if let Some(abort_handler) = &*self.abort_handler.lock() {
            abort_handler.abort();
        }

        self.session_manager.abort(force);
    }

    async fn start(&self, args: (String, u16)) -> Result<SocketAddr> {
        let sessions = self.session_manager.clone();
        let rejected_executor = Runtime::with_worker_threads(1)?;

        let (abort_handle, mut reg) = AbortHandle::new_pair();
        let (stream, listener_addr) = Self::listener_tcp(&args.0, args.1).await?;

        let listener_loop = tokio::spawn(async move {
            let mut abortable_listener_stream = Abortable::new(stream, reg);

            loop {
                match abortable_listener_stream.next().await {
                    None => break,
                    Some(Err(error)) => log::error!("Unexpected error during process accept: {}", error),
                    Some(Ok(tcp_stream)) => {
                        if let Ok(addr) = tcp_stream.peer_addr() {
                            log::debug!("Received connect from {}", addr);
                        }

                        match sessions.create_session::<Session>() {
                            Err(error) => Self::reject_session(tcp_stream, &rejected_executor, error),
                            Ok(runnable_session) => {
                                if let Err(error) = runnable_session.start(tcp_stream).await {
                                    log::error!("Unexpected error occurred during start session: {:?}", error);
                                };
                            }
                        }
                    }
                }
            }
        });

        self.started(listener_loop, abort_handle);

        Ok(listener_addr)
    }

    async fn wait_terminal(&self, duration: Option<Duration>) -> Result<Elapsed> {
        let instant = Instant::now();

        let subscribe = || {
            let terminal_sender = self.terminal_sender.lock();
            match &*terminal_sender {
                None => Err(ErrorCode::LogicalError("Logical error, call wait_terminal before start.")),
                Some(terminal_watchers) => Ok(Some(terminal_watchers.clone().subscribe()))
            }
        };

        match duration {
            None => {
                self.session_manager.wait_terminal(None).await?;
                if let Some(mut rx) = subscribe()? {
                    rx.recv().await;
                }
            }
            Some(duration) => {
                let elapsed = self.session_manager.wait_terminal(Some(duration)).await?;
                if let Some(mut rx) = subscribe()? {
                    let duration = duration.sub(elapsed);
                    tokio::time::timeout(duration, rx.recv())
                        .await
                        .map_err_to_code(ErrorCode::Timeout, || "")?;
                }
            }
        };

        Ok(instant.elapsed())
    }
}

