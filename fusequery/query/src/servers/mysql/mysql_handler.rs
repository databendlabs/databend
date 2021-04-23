// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::io;
use std::net;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Error;
use anyhow::Result;
use common_datablocks::DataBlock;
use common_exception::ErrorCodes;
use common_planners::Statistics;
use common_streams::SendableDataBlockStream;
use futures::future;
use futures::future::BoxFuture;
use futures::future::Then;
use futures::Future;
use futures::FutureExt;
use futures::TryFutureExt;
use log::debug;
use log::error;
use metrics::histogram;
use msql_srv::*;
use threadpool::ThreadPool;
use tokio::runtime::Runtime;
use tokio_stream::StreamExt as OtherStreamExt;
use warp::body::stream;

use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::interpreters::IInterpreter;
use crate::interpreters::InterpreterFactory;
use crate::servers::mysql::MysqlStream;
use crate::sessions::FuseQueryContextRef;
use crate::sessions::SessionRef;
use crate::sql::PlanParser;

struct Session {
    ctx: FuseQueryContextRef
}

impl Session {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        Session { ctx }
    }
}

impl<W: io::Write> MysqlShim<W> for Session {
    type Error = anyhow::Error;

    fn on_prepare(&mut self, _: &str, _: StatementMetaWriter<W>) -> Result<()> {
        unimplemented!()
    }

    fn on_execute(&mut self, _: u32, _: ParamParser, _: QueryResultWriter<W>) -> Result<()> {
        unimplemented!()
    }

    fn on_close(&mut self, _: u32) {
        unimplemented!()
    }

    fn on_query(&mut self, query: &str, writer: QueryResultWriter<W>) -> Result<()> {
        debug!("{}", query);
        self.ctx.reset()?;

        let start = Instant::now();

        fn build_runtime(max_threads: Result<u64>) -> Result<Runtime, ErrorCodes> {
            max_threads.map_err(|e| ErrorCodes::UnknownException(String::from("Missing max_thread settings.")))
                .and_then(|v| {
                    tokio::runtime::Builder::new_multi_thread().enable_io()
                        .worker_threads(v as usize).build()
                        .map_err(|tokio_error| ErrorCodes::TokioError(format!("{}", tokio_error)))
                })
        }

        type DataPuller<'a> = BoxFuture<'a, Result<Vec<DataBlock>, ErrorCodes>>;
        fn data_puller<'a>(interpreter: &'a Arc<dyn IInterpreter>, _statistics: Result<Statistics>) -> DataPuller<'a> {
            return interpreter.execute().then(|r_stream| match r_stream {
                Ok(stream) => stream.collect().left_future(),
                Err(e) => futures::future::err(e).right_future()
            }).map(|data_blocks| data_blocks.map_err(|exception| ErrorCodes::UnknownException(format!("{}", exception)))).boxed();
        }

        PlanParser::create(self.ctx.clone()).build_from_sql(query)
            .and_then(|built_plan| InterpreterFactory::get(self.ctx.clone(), built_plan))
            .and_then(|interpreter| build_runtime(self.ctx.get_max_threads()).map(|runtime| (runtime, interpreter)))
            .and_then(|(runtime, interpreter)| runtime.block_on(data_puller(&interpreter, self.ctx.try_get_statistics())))
            .and_then(|data_blocks| MysqlStream::create(data_blocks).execute(writer).or_else(|_| Result::Ok(())))
            .or_else(|error_codes| Result::Ok(()))
        // .or_else(|exception| writer.error(ErrorKind::ER_UNKNOWN_ERROR, format!("{}", exception).as_bytes()).or_else(|_| { Result::Ok(()) }))
    }

    fn on_init(&mut self, db: &str, writer: InitWriter<W>) -> Result<()> {
        debug!("MySQL use db:{}", db);
        match self.ctx.set_default_db(db.to_string()) {
            Ok(..) => {
                writer.ok()?;
            }
            Err(e) => {
                error!("{}", e);
                writer.error(
                    ErrorKind::ER_BAD_DB_ERROR,
                    format!("Unknown database: {:?}", db).as_bytes()
                )?;
            }
        };
        Ok(())
    }
}

pub struct MysqlHandler {
    conf: Config,
    cluster: ClusterRef,
    session_manager: SessionRef
}

impl MysqlHandler {
    pub fn create(conf: Config, cluster: ClusterRef, session_manager: SessionRef) -> Self {
        Self {
            conf,
            cluster,
            session_manager
        }
    }

    pub fn start(&self) -> Result<()> {
        let listener = net::TcpListener::bind(format!(
            "{}:{}",
            self.conf.mysql_handler_host, self.conf.mysql_handler_port
        ))?;
        let pool = ThreadPool::new(self.conf.mysql_handler_thread_num as usize);

        for stream in listener.incoming() {
            let stream = stream?;
            let ctx = self
                .session_manager
                .try_create_context()?
                .with_cluster(self.cluster.clone())?;
            ctx.set_max_threads(self.conf.num_cpus)?;

            let session_mgr = self.session_manager.clone();
            pool.execute(move || {
                MysqlIntermediary::run_on_tcp(Session::create(ctx.clone()), stream).unwrap();
                session_mgr.try_remove_context(ctx).unwrap();
            })
        }
        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        Ok(())
    }
}
