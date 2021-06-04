// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::io;
use std::net;
use std::time::Instant;

use common_datablocks::DataBlock;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_ext::ResultExt;
use common_ext::ResultTupleExt;
use log::debug;
use metrics::histogram;
use msql_srv::*;
use threadpool::ThreadPool;
use tokio::runtime::Runtime;
use tokio_stream::StreamExt as OtherStreamExt;

use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::interpreters::InterpreterFactory;
use crate::interpreters::InterpreterPtr;
use crate::sessions::FuseQueryContextRef;
use crate::sessions::SessionManagerRef;
use crate::sql::PlanParser;
use crate::servers::mysql::endpoints::{MySQLOnQueryEndpoint, IMySQLEndpoint, MySQLOnInitEndpoint};
use std::sync::atomic::{Ordering, AtomicU64};
use std::sync::Arc;
use common_infallible::Mutex;
use std::net::SocketAddr;
use futures::future::Either;
use crate::servers::RunnableServer;
use tokio::net::{TcpListener, TcpStream};
use futures::FutureExt;
use futures::future::select;
use futures::future::TryFutureExt;
use tokio::io::AsyncWriteExt;
use crate::servers::runnable_session::RunnableSession;
use tokio::sync::mpsc::Receiver;

struct Session {
    cluster: ClusterRef,
    session_manager: SessionManagerRef,
    current_database: Arc<Mutex<String>>,
}

impl Session {
    pub fn create(cluster: ClusterRef, session_manager: SessionManagerRef) -> Self {
        Session {
            cluster,
            session_manager,
            current_database: Arc::new(Mutex::new(String::from("default"))),
        }
    }
}

impl<W: io::Write> MysqlShim<W> for Session {
    type Error = ErrorCodes;

    fn on_prepare(&mut self, _: &str, writer: StatementMetaWriter<W>) -> Result<()> {
        writer.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            "Prepare is not support in DataFuse.".as_bytes(),
        )?;

        Ok(())
    }

    fn on_execute(&mut self, _: u32, _: ParamParser, writer: QueryResultWriter<W>) -> Result<()> {
        writer.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            "Execute is not support in DataFuse.".as_bytes(),
        )?;

        Ok(())
    }

    fn on_close(&mut self, _: u32) {
        // unimplemented!()
    }

    fn on_query(&mut self, query: &str, writer: QueryResultWriter<W>) -> Result<()> {
        debug!("{}", query);
        let cluster = self.cluster.clone();
        let database = self.current_database.lock().clone();
        let session_manager = self.session_manager.clone();

        MySQLOnQueryEndpoint::on_action(writer, move || {
            let start = Instant::now();

            let context = session_manager
                .try_create_context()?
                .with_cluster(cluster.clone())?;

            context.set_current_database(database.clone());

            let query_plan = PlanParser::create(context.clone()).build_from_sql(query)?;
            let query_interpreter = InterpreterFactory::get(context, query_plan)?;

            let received_data = futures::executor::block_on(query_interpreter
                .execute()
                .and_then(|stream| stream.collect::<Result<Vec<DataBlock>>>()));

            histogram!(
                super::mysql_metrics::METRIC_MYSQL_PROCESSOR_REQUEST_DURATION,
                start.elapsed()
            );

            received_data
        })
    }

    fn on_init(&mut self, database_name: &str, writer: InitWriter<W>) -> Result<()> {
        log::debug!("Use `{}` for MySQLHandler", database_name);
        let current_database = self.current_database.clone();

        MySQLOnInitEndpoint::on_action(writer, move || -> Result<()> {
            // TODO: test database is exists.
            *current_database.lock() = database_name.to_string();

            Ok(())
        })
    }
}

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

    pub async fn start(&self, hostname: &str, port: u16) -> Result<RunnableServer> {
        let cluster = self.cluster.clone();
        let sessions = self.session_manager.clone();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);

        let listener = Self::listener_tcp(hostname, port).await?;
        let listener_address = listener.local_addr()?.clone();

        let listener_loop = tokio::spawn(async move {
            let rejected_executor = ThreadPool::new(1);

            loop {
                let mut shutdown_receiver = Box::pin(receiver.recv());
                match select(Box::pin(listener.accept()), shutdown_receiver).await {
                    Either::Right((_, _)) => break,
                    Either::Left((Err(error), future)) => {
                        shutdown_receiver = future;
                        log::error!("Unexpected error during process accept[skip]: {}", error);
                    }
                    Either::Left((Ok((stream, socket)), future)) => {
                        shutdown_receiver = future;

                        if !sessions.accept_session() {
                            log::debug!("Rejected connect from {}", socket);
                            Self::reject_session(stream, &rejected_executor);
                            break;
                        }

                        log::debug!("Received connect from {}", socket);
                        let session = Session::create(cluster.clone(), sessions.clone());
                        if let Err(error) = Self::accept_session(session, stream) {
                            log::error!("Unexpected error during process accept[skip]: {}", error);
                        }
                    }
                };
            }
        });

        Ok(RunnableServer::create(listener_address, sender.clone(), listener_loop))
    }

    async fn listener_tcp(hostname: &str, port: u16) -> Result<TcpListener> {
        let address = format!("{}:{}", hostname, port);
        let listener = tokio::net::TcpListener::bind(address).await?;
        Ok(listener)
    }

    fn reject_session(stream: TcpStream, executor: &ThreadPool) -> Result<RunnableSession> {
        match stream.into_std() {
            Err(error) => log::error!("{}", error),
            Ok(stream) => {
                stream.set_nonblocking(false)?;
                let join_handler = std::thread::spawn(move || -> Result<()> {
                    if let Err(error) = MysqlIntermediary::run_on_tcp(session, stream) {
                        log::error!("Unexpected error occurred during query execution: {:?}", error);
                    };

                    Ok(())
                });
            }
        };

        Err(ErrorCodes::UnImplement(""))
    }

    fn accept_session(session: Session, stream: TcpStream) -> Result<RunnableSession> {
        match stream.into_std() {
            Err(error) => log::error!("{}", error),
            Ok(stream) => {
                stream.set_nonblocking(false)?;
                let join_handler = std::thread::spawn(move || -> Result<()> {
                    if let Err(error) = MysqlIntermediary::run_on_tcp(session, stream) {
                        log::error!("Unexpected error occurred during query execution: {:?}", error);
                    };

                    Ok(())
                });
            }
        };


        Ok(RunnableSession {})
    }
}
