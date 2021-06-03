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
use crate::servers::mysql::endpoints::{MySQLOnQueryEndpoint, IMySQLEndpoint};
use std::sync::atomic::{Ordering, AtomicU64};
use std::sync::Arc;
use common_infallible::Mutex;
use std::net::TcpStream;
use futures::future::Either;
use crate::servers::RunnableServer;
use tokio::net::TcpListener;
use futures::FutureExt;
use futures::future::select;

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
        let session_manager = self.session_manager.clone();

        MySQLOnQueryEndpoint::on_query(writer, move || {
            let start = Instant::now();

            let context = session_manager.try_create_context()?;
            // TODO: init context

            fn build_runtime() -> Result<Runtime> {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .map_err(|tokio_error| ErrorCodes::TokioError(format!("{}", tokio_error)))
            }

            let query_runtime = build_runtime()?;
            let query_plan = PlanParser::create(context.clone()).build_from_sql(query)?;
            let query_interpreter = InterpreterFactory::get(context, query_plan)?;

            use futures::future::TryFutureExt;
            let query_result = query_runtime.block_on(
                query_interpreter
                    .execute()
                    .and_then(|stream| stream.collect::<Result<Vec<DataBlock>>>()),
            );

            histogram!(
                super::mysql_metrics::METRIC_MYSQL_PROCESSOR_REQUEST_DURATION,
                start.elapsed()
            );

            query_result
        })
    }

    fn on_init(&mut self, database_name: &str, writer: InitWriter<W>) -> Result<()> {
        log::debug!("Use `{}` for MySQLHandler", database_name);
        let current_database = self.current_database.clone();

        *current_database.lock() = database_name.to_string();
        // match self.session_manager.set_current_database(database_name.to_string()) {
        //     Ok(_) => writer.ok()?,
        //     Err(error) => {
        //         log::error!("OnInit Error: {:?}", error);
        //         writer.error(ErrorKind::ER_UNKNOWN_ERROR, format!("{}", error).as_bytes())?;
        //     }
        // };

        Ok(())
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

    pub fn listener(hostname: &str, port: u16) -> Result<TcpListener> {
        let address = format!("{}:{}", hostname, port);
        let join_handler = tokio::spawn(async move { tokio::net::TcpListener::bind(address).await });

        match join_handler.await {
            Err(error) => Err(ErrorCodes::TokioError("")),
            Ok(listener) => {}
        }
    }

    pub fn start(&self) -> Result<RunnableServer> {
        let cluster = self.cluster.clone();
        let session_manager = self.session_manager.clone();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let listener = Self::listener(&self.conf.mysql_handler_host, self.conf.mysql_handler_port)?;

        let join_handler = tokio::spawn(async move {
            let max_session_size = self.conf.mysql_handler_thread_num;
            let max_session_size = Arc::new(AtomicU64::new(max_session_size));

            loop {
                match select(Box::pin(listener.accept()), Box::pin(receiver.recv())).await {
                    Either::Right((_, _)) => break,
                    Either::Left((Err(error), _)) => {}
                    Either::Left((Ok((stream, _)), _)) => {
                        match stream.into_std() {
                            Err(error) => {},
                            Ok(stream) => Self::process_accept_session(
                                max_session_size.clone(),
                                stream,
                                cluster.clone(),
                                session_manager.clone(),
                            ),
                        }
                    },
                };
            }
        });

        Ok(RunnableServer::create(sender.clone(), join_handler))
    }

    fn process_accept_session(
        max_session_size: Arc<AtomicU64>,
        stream: TcpStream,
        cluster: ClusterRef,
        session_manager: SessionManagerRef,
    ) {
        match max_session_size.fetch_sub(1, Ordering::SeqCst) {
            0 => {
                let rejected_executor = ThreadPool::new(1);
                rejected_executor.execute(move || {});
            }
            _ => Self::accept_session(max_session_size, stream, cluster, session_manager),
        }
    }

    fn accept_session(
        max_session_size: Arc<AtomicU64>,
        stream: TcpStream,
        cluster: ClusterRef,
        session_manager: SessionManagerRef,
    ) {
        std::thread::spawn(move || {
            let session = Session::create(cluster, session_manager);
            if let Err(error) = MysqlIntermediary::run_on_tcp(session, stream) {
                log::error!(
                    "Unexpected error occurred during query execution: {:?}",
                    error
                );
            };
            max_session_size.fetch_add(1, Ordering::SeqCst);
        });
    }

    pub fn stop(&self) -> Result<()> {
        Ok(())
    }
}
