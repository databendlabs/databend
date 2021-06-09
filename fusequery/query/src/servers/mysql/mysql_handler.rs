// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::io;
use std::net;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use futures::future::{Either, AbortHandle, Abortable};
use futures::future::select;
use futures::future::TryFutureExt;
use futures::FutureExt;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use log::debug;
use msql_srv::*;
use threadpool::ThreadPool;
use tokio::io::{AsyncWriteExt, Interest, AsyncReadExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Receiver;
use tokio_stream::StreamExt as OtherStreamExt;

use common_arrow::parquet::data_type::AsBytes;
use common_exception::ToErrorCode;
use common_infallible::Mutex;

use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::interpreters::InterpreterFactory;
use crate::interpreters::InterpreterPtr;
use crate::servers::mysql::endpoints::{IMySQLEndpoint, MySQLOnInitEndpoint, MySQLOnQueryEndpoint};
use crate::servers::mysql::mysql_session::Session;
use crate::servers::RunningServer;
use crate::sessions::FuseQueryContextRef;
use crate::sessions::SessionManagerRef;
use crate::sql::PlanParser;
use tokio_stream::wrappers::TcpListenerStream;
use crate::servers::mysql::reject_connection::RejectConnection;
use common_runtime::Runtime;

pub struct MySQLHandler {
    conf: Config,
    cluster: ClusterRef,
    session_manager: SessionManagerRef,
}

impl MySQLHandler {
    pub fn create(conf: Config, cluster: ClusterRef, session_manager: SessionManagerRef) -> Self {
        MySQLHandler {
            conf,
            cluster,
            session_manager,
        }
    }

    pub async fn start(&self, hostname: &str, port: u16) -> Result<RunningServer> {
        let cluster = self.cluster.clone();
        let sessions = self.session_manager.clone();
        let rejected_executor = Runtime::with_worker_threads(1)?;

        let (abort_handle, mut reg) = AbortHandle::new_pair();
        let (stream, listener_addr) = Self::listener_tcp(hostname, port).await?;

        let listener_loop = tokio::spawn(async move {
            let max_sessions = Arc::new(Mutex::new(sessions.max_mysql_sessions()));
            let mut abortable_listener_stream = Abortable::new(stream, reg);

            loop {
                match abortable_listener_stream.next().await {
                    None => break,
                    Some(Err(error)) => log::error!("Unexpected error during process accept[skip]: {}", error),
                    Some(Ok(tcp_stream)) => {
                        let mut locked_max_sessions = max_sessions.lock();

                        if *locked_max_sessions != 0 {
                            *locked_max_sessions -= 1;

                            if let Ok(addr) = tcp_stream.peer_addr() {
                                log::debug!("Received connect from {}", addr);
                            }
                            // let session = Session::create(cluster.clone(), sessions.clone());
                            /*if let Err(error) = */Self::accept_session(tcp_stream, &max_sessions, sessions.clone(), cluster.clone()); /*{
                                log::error!("Unexpected error during process accept[skip]: {}", error);
                            }*/
                        } else {
                            if let Ok(addr) = tcp_stream.peer_addr() {
                                log::debug!("Rejected connect from {}", addr);
                            }

                            Self::reject_session(tcp_stream, &rejected_executor, sessions.clone());
                        }
                    }
                }
            }
            // TODO: join all session.
        });

        Ok(RunningServer::create(listener_addr, abort_handle, listener_loop))
    }

    async fn listener_tcp(hostname: &str, port: u16) -> Result<(TcpListenerStream, SocketAddr)> {
        let address = format!("{}:{}", hostname, port);
        let listener = tokio::net::TcpListener::bind(address).await?;
        let listener_addr = listener.local_addr()?;
        Ok((TcpListenerStream::new(listener), listener_addr))
    }

    fn reject_session(mut stream: TcpStream, executor: &Runtime, sessions: SessionManagerRef) {
        let join_handler = executor.spawn(async move {
            if let Err(error) = RejectConnection::reject_mysql_connection(
                stream,
                ErrorKind::ER_TOO_MANY_USER_CONNECTIONS,
                "Rejected MySQL connection. The current accept connection has exceeded mysql_handler_thread_num config",
            ).await {
                log::error!("Unexpected error occurred during reject connection: {:?}", error);
            }

            // TODO: remove actives session count
        });

        sessions.add_reject_mysql_session();
    }

    fn accept_session(
        stream: TcpStream,
        max_session: &Arc<Mutex<u64>>,
        sessions: SessionManagerRef,
        cluster: ClusterRef,
    ) {
        let session = Session::create(cluster.clone(), sessions.clone());
        let max_session = max_session.clone();
        let join_handler = std::thread::spawn(move || {
            let stream = stream.into_std().map_err_to_code(ErrorCode::TokioError, || "").unwrap();
            stream.set_nonblocking(false).unwrap();

            if let Err(error) = MysqlIntermediary::run_on_tcp(session, stream) {
                log::error!("Unexpected error occurred during query execution: {:?}", error);
            };

            *max_session.lock() += 1;
            // TODO: remove actives session count
        });

        sessions.add_accepted_mysql_session();
    }
}
