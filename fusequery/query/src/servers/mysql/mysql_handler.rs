// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::time::Instant;
use std::{io, net};

use futures::stream::StreamExt;
use log::{debug, error};
use metrics::histogram;
use msql_srv::*;
use threadpool::ThreadPool;

use crate::clusters::ClusterRef;
use crate::common_datablocks::DataBlock;
use crate::configs::Config;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::interpreters::InterpreterFactory;
use crate::servers::mysql::MysqlStream;
use crate::sessions::{FuseQueryContextRef, SessionRef};
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
    type Error = FuseQueryError;

    fn on_prepare(&mut self, _: &str, _: StatementMetaWriter<W>) -> FuseQueryResult<()> {
        unimplemented!()
    }

    fn on_execute(
        &mut self,
        _: u32,
        _: ParamParser,
        _: QueryResultWriter<W>,
    ) -> FuseQueryResult<()> {
        unimplemented!()
    }

    fn on_close(&mut self, _: u32) {
        unimplemented!()
    }

    fn on_query(&mut self, query: &str, writer: QueryResultWriter<W>) -> FuseQueryResult<()> {
        debug!("{}", query);
        self.ctx.reset()?;

        let start = Instant::now();
        let plan = PlanParser::create(self.ctx.clone()).build_from_sql(query);
        match plan {
            Ok(v) => match InterpreterFactory::get(self.ctx.clone(), v) {
                Ok(executor) => {
                    let result: FuseQueryResult<Vec<DataBlock>> =
                        tokio::runtime::Builder::new_multi_thread()
                            .enable_io()
                            .worker_threads(self.ctx.get_max_threads()? as usize)
                            .build()?
                            .block_on(async move {
                                let start = Instant::now();
                                let mut r = vec![];
                                let mut stream = executor.execute().await?;
                                while let Some(block) = stream.next().await {
                                    r.push(block?);
                                }
                                let duration = start.elapsed();
                                debug!(
                                    "MySQLHandler executor cost:{:?}, statistics:{:?}",
                                    duration,
                                    self.ctx.try_get_statistics()?
                                );
                                Ok(r)
                            });

                    match result {
                        Ok(blocks) => {
                            let start = Instant::now();
                            let stream = MysqlStream::create(blocks);
                            stream.execute(writer)?;
                            let duration = start.elapsed();
                            debug!("MySQLHandler send to client cost:{:?}", duration);
                        }
                        Err(e) => {
                            error!("FuseQueryResultError {:?}", e);
                            writer
                                .error(ErrorKind::ER_UNKNOWN_ERROR, format!("{:?}", e).as_bytes())?
                        }
                    }
                }
                Err(e) => {
                    error!("FuseQueryResultError {:?}", e);
                    writer.error(ErrorKind::ER_UNKNOWN_ERROR, format!("{:?}", e).as_bytes())?
                }
            },
            Err(e) => {
                error!("FuseQueryResultError {:?}", e);
                writer.error(ErrorKind::ER_UNKNOWN_ERROR, format!("{:?}", e).as_bytes())?;
            }
        }
        histogram!(
            super::mysql_metrics::METRIC_MYSQL_PROCESSOR_REQUEST_DURATION,
            start.elapsed()
        );

        Ok(())
    }

    fn on_init(&mut self, db: &str, writer: InitWriter<W>) -> FuseQueryResult<()> {
        debug!("MySQL use db:{}", db);
        match self.ctx.set_default_db(db.to_string()) {
            Ok(..) => {
                writer.ok()?;
            }
            Err(e) => {
                error!("{}", e);
                writer.error(
                    ErrorKind::ER_BAD_DB_ERROR,
                    format!("Unknown database: {:?}", db).as_bytes(),
                )?;
            }
        };
        Ok(())
    }
}

pub struct MysqlHandler {
    conf: Config,
    cluster: ClusterRef,
    session_manager: SessionRef,
}

impl MysqlHandler {
    pub fn create(conf: Config, cluster: ClusterRef, session_manager: SessionRef) -> Self {
        Self {
            conf,
            cluster,
            session_manager,
        }
    }

    pub fn start(&self) -> FuseQueryResult<()> {
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

    pub fn stop(&self) -> FuseQueryResult<()> {
        Ok(())
    }
}
