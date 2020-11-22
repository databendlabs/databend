// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use log::debug;
use msql_srv::*;
use std::sync::Arc;
use std::{io, net, thread};
use tokio::stream::StreamExt;

use crate::contexts::{FuseQueryContext, Options};
use crate::datablocks::DataBlock;
use crate::datasources::DataSource;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::executors::ExecutorFactory;
use crate::planners::Planner;
use crate::servers::mysql::MySQLStream;

struct Session {
    ctx: Arc<FuseQueryContext>,
}

impl Session {
    pub fn create(ctx: Arc<FuseQueryContext>) -> Self {
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

        let plan = Planner::new().build_from_sql(self.ctx.clone(), query);
        match plan {
            Ok(v) => match ExecutorFactory::get(self.ctx.clone(), v) {
                Ok(executor) => {
                    let result: FuseQueryResult<Vec<DataBlock>> =
                        tokio::runtime::Builder::new_multi_thread()
                            .worker_threads(self.ctx.worker_threads)
                            .build()?
                            .block_on(async move {
                                let mut r = vec![];
                                let mut stream = executor.execute().await?;
                                while let Some(block) = stream.next().await {
                                    r.push(block?);
                                }
                                Ok(r)
                            });

                    match result {
                        Ok(blocks) => {
                            let stream = MySQLStream::create(blocks);
                            stream.execute(writer)?;
                        }
                        Err(e) => writer
                            .error(ErrorKind::ER_UNKNOWN_ERROR, format!("{:?}", e).as_bytes())?,
                    }
                }
                Err(e) => {
                    writer.error(ErrorKind::ER_UNKNOWN_ERROR, format!("{:?}", e).as_bytes())?
                }
            },
            Err(e) => {
                writer.error(ErrorKind::ER_UNKNOWN_ERROR, format!("{:?}", e).as_bytes())?;
            }
        }
        Ok(())
    }

    fn on_init(&mut self, _: &str, _writer: InitWriter<W>) -> FuseQueryResult<()> {
        unimplemented!();
    }
}

pub struct MySQLHandler {
    opts: Options,
    datasource: Arc<DataSource>,
}

impl MySQLHandler {
    pub fn create(opts: Options, datasource: Arc<DataSource>) -> Self {
        MySQLHandler { opts, datasource }
    }

    pub fn start(&self) -> FuseQueryResult<()> {
        let listener =
            net::TcpListener::bind(format!("0.0.0.0:{}", self.opts.mysql_handler_port)).unwrap();

        let worker_threads = self.opts.num_cpus;
        let datasource = self.datasource.clone();
        let jh = thread::spawn(move || {
            if let Ok((s, _)) = listener.accept() {
                let ctx = Arc::new(FuseQueryContext::create_ctx(worker_threads, datasource));
                MysqlIntermediary::run_on_tcp(Session::create(ctx), s).unwrap();
            }
        });
        jh.join().unwrap();
        Ok(())
    }

    pub async fn stop(&self) -> FuseQueryResult<()> {
        Ok(())
    }
}
