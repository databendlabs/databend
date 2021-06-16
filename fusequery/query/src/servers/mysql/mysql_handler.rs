// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::io;
use std::net;
use std::time::Instant;

use common_datablocks::DataBlock;
use common_exception::ErrorCode;
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
use crate::sql::DfHint;
use crate::sql::PlanParser;

struct Session {
    ctx: FuseQueryContextRef,
}

impl Session {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        Session { ctx }
    }

    pub fn execute_query(&mut self, query: &str) -> Result<Vec<DataBlock>> {
        debug!("{}", query);
        self.ctx.reset().unwrap();
        let start = Instant::now();

        fn build_runtime() -> Result<Runtime> {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|tokio_error| ErrorCode::TokioError(format!("{}", tokio_error)))
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

        let (plan, hints) = PlanParser::create(self.ctx.clone()).build_with_hint_from_sql(query);

        let output = plan
            .and_then(|p| InterpreterFactory::get(self.ctx.clone(), p))
            .zip(build_runtime())
            // Execute query and get result
            .and_then_tuple(receive_data_set);

        let output = match output {
            Ok(v) => Ok(v),
            Err(e) => {
                let hint = hints.iter().find(|v| v.error_code.is_some());
                if let Some(DfHint {
                    error_code: Some(code),
                    ..
                }) = hint
                {
                    if *code == e.code() {
                        Ok(vec![DataBlock::empty()])
                    } else {
                        let actual_code = e.code();
                        Err(e.add_message(format!(
                            "Expected server error code: {} but got: {}.",
                            code, actual_code
                        )))
                    }
                } else {
                    Err(e)
                }
            }
        };

        histogram!(
            super::mysql_metrics::METRIC_MYSQL_PROCESSOR_REQUEST_DURATION,
            start.elapsed()
        );

        output
    }
}

impl<W: io::Write> MysqlShim<W> for Session {
    type Error = ErrorCode;

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
        unimplemented!()
    }

    fn on_query(&mut self, query: &str, writer: QueryResultWriter<W>) -> Result<()> {
        // Push result set to client
        let output = self.execute_query(query);

        use crate::servers::mysql::endpoints::on_query_done as done;
        output.and_match(done(writer))
    }

    fn on_init(&mut self, database_name: &str, writer: InitWriter<W>) -> Result<()> {
        let query = format!("USE {}", database_name);
        match self.execute_query(&query) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
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

        let max_session_size = self.conf.mysql_handler_thread_num as usize;
        let session_executor = ThreadPool::new(max_session_size);

        for stream in listener.incoming() {
            let stream = stream?;
            let ctx = self
                .session_manager
                .try_create_context()?
                .with_cluster(self.cluster.clone())?;
            ctx.set_max_threads(self.conf.num_cpus)?;

            let session_mgr = self.session_manager.clone();
            session_executor.execute(move || {
                if let Err(error) =
                    MysqlIntermediary::run_on_tcp(Session::create(ctx.clone()), stream)
                {
                    log::error!(
                        "Unexpected error occurred during query execution: {:?}",
                        error
                    );
                }

                if let Err(error) = session_mgr.try_remove_context(ctx) {
                    log::error!("Cannot to destroy FuseQueryContext: {:?}", error);
                }
            })
        }
        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        Ok(())
    }
}
