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

struct Session {
    ctx: FuseQueryContextRef,
}

impl Session {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        Session { ctx }
    }
}

impl<W: io::Write> MysqlShim<W> for Session {
    type Error = std::io::Error;

    fn on_prepare(&mut self, _: &str, writer: StatementMetaWriter<W>) -> std::io::Result<()> {
        writer.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            "Prepare is not support in DataFuse.".as_bytes(),
        )
    }

    fn on_execute(
        &mut self,
        _: u32,
        _: ParamParser,
        writer: QueryResultWriter<W>,
    ) -> std::io::Result<()> {
        writer.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            "Execute is not support in DataFuse.".as_bytes(),
        )
    }

    fn on_close(&mut self, _: u32) {
        unimplemented!()
    }

    fn on_query(&mut self, query: &str, writer: QueryResultWriter<W>) -> std::io::Result<()> {
        debug!("{}", query);
        self.ctx.reset().unwrap();
        let start = Instant::now();

        fn build_runtime() -> Result<Runtime> {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|tokio_error| ErrorCodes::TokioError(format!("{}", tokio_error)))
        }

        type ResultSet = Result<Vec<DataBlock>>;
        fn receive_data_set(runtime: Runtime, interpreter: InterpreterPtr) -> ResultSet {
            use futures::future::TryFutureExt;
            runtime.block_on(
                interpreter
                    .execute()
                    .and_then(|stream| stream.collect::<Result<Vec<DataBlock>>>()),
            )
        }

        use crate::servers::mysql::endpoints::on_query_done as done;
        let output = PlanParser::create(self.ctx.clone())
            .build_from_sql(query)
            .and_then(|built_plan| InterpreterFactory::get(self.ctx.clone(), built_plan))
            .zip(build_runtime())
            // Execute query and get result
            .and_then_tuple(receive_data_set)
            // Push result set to client
            .and_match(done(writer));

        histogram!(
            super::mysql_metrics::METRIC_MYSQL_PROCESSOR_REQUEST_DURATION,
            start.elapsed()
        );

        output
    }

    fn on_init(&mut self, database_name: &str, writer: InitWriter<W>) -> std::io::Result<()> {
        debug!("Use `{}` for MySQLHandler", database_name);
        use crate::servers::mysql::endpoints::on_init_done as done;
        self.ctx
            .set_current_database(database_name.to_string())
            .map(|_| Some(()))
            .transpose()
            .map(done(writer))
            .unwrap()
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

    pub fn start(&self) -> anyhow::Result<()> {
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
