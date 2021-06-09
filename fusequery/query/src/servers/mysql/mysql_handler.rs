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
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Receiver;
use tokio_stream::StreamExt as OtherStreamExt;

use common_arrow::parquet::data_type::AsBytes;
use common_exception::ToErrorCode;
use common_ext::ResultExt;
use common_ext::ResultTupleExt;
use common_infallible::Mutex;

use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::interpreters::InterpreterFactory;
use crate::interpreters::InterpreterPtr;
use crate::servers::mysql::endpoints::{IMySQLEndpoint, MySQLOnInitEndpoint, MySQLOnQueryEndpoint};
use crate::servers::mysql::mysql_session::{Session, RejectedSession};
use crate::servers::active_session::ActiveSession;
use crate::servers::RunningServer;
use crate::sessions::FuseQueryContextRef;
use crate::sessions::SessionManagerRef;
use crate::sql::PlanParser;
use tokio_stream::wrappers::TcpListenerStream;

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
        let rejected_executor = common_runtime::Runtime::with_worker_threads(1)?;

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

                            let session = Session::create(cluster.clone(), sessions.clone());
                            if let Err(error) = Self::accept_session(session, tcp_stream, &max_sessions) {
                                log::error!("Unexpected error during process accept[skip]: {}", error);
                            }
                        } else {
                            if let Ok(addr) = tcp_stream.peer_addr() {
                                log::debug!("Rejected connect from {}", addr);
                            }

                            Self::reject_session(tcp_stream, &rejected_executor);
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

    async fn reject_mysql_connection(mut stream: TcpStream) -> Result<()> {
        // Send handshake, packet from msql-srv. Packet[seq = 0]
        stream.write(&vec![
            69, 00, 00, 00, 10, 53, 46, 49, 46, 49, 48, 45, 97, 108,
            112, 104, 97, 45, 109, 115, 113, 108, 45, 112, 114, 111,
            120, 121, 0, 8, 0, 0, 0, 59, 88, 44, 112, 111, 95, 107,
            125, 0, 0, 66, 33, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 62, 111, 54, 94, 87, 122, 33, 47, 107, 77, 125, 78, 0
        ]).await?;
        stream.flush().await?;

        let mut buffer = vec![0; 4];
        stream.read(&mut buffer).await?;

        // Ignore handshake response. Packet[seq = 1]
        let len = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], 0]);
        buffer.resize(len as usize, 0);
        stream.read(&mut buffer).await?;

        // Send error. Packet[seq = 2]
        let mut buffer = vec![0xFF_u8];
        buffer.extend(&(ErrorKind::ER_TOO_MANY_USER_CONNECTIONS as u16).to_le_bytes());
        buffer.extend(&vec![b'#', b'4', b'2', b'0', b'0', b'0']);
        buffer.extend("Rejected MySQL connection. The current accept connection has exceeded mysql_handler_thread_num config".as_bytes());

        let size = buffer.len().to_le_bytes();
        buffer.splice(0..0, [size[0], size[1], size[2], 2].iter().cloned());
        stream.write(&buffer).await?;
        stream.flush().await?;

        Ok(())
    }

    fn reject_session(mut stream: TcpStream, executor: &common_runtime::Runtime) -> Result<ActiveSession> {
        let join_handler = executor.spawn(async move {
            if let Err(error) = MySQLHandler::reject_mysql_connection(stream).await {
                log::error!("Unexpected error occurred during reject connection: {:?}", error);
            }
        });

        Ok(ActiveSession::Rejected(Some(join_handler)))
    }

    fn accept_session(session: Session, stream: TcpStream, max_session: &Arc<Mutex<u64>>) -> Result<ActiveSession> {
        let stream = stream.into_std().map_err_to_code(ErrorCode::TokioError, || "")?;
        stream.set_nonblocking(false)?;
        let max_session = max_session.clone();
        let join_handler = std::thread::spawn(move || {
            if let Err(error) = MysqlIntermediary::run_on_tcp(session, stream) {
                log::error!("Unexpected error occurred during query execution: {:?}", error);
            };

            *max_session.lock() += 1;
        });
        Ok(ActiveSession::Accepted(Some(join_handler)))
    }
}
