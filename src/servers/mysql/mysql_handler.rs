// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use log::{debug, error};

use msql_srv::*;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::{io, net, thread};
use tokio::stream::StreamExt;

use crate::contexts::{FuseQueryContext, Options};
use crate::datablocks::DataBlock;
use crate::datasources::IDataSource;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::executors::ExecutorFactory;
use crate::optimizers::Optimizer;
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
            Ok(v) => {
                let v = Optimizer::create().optimize(&v)?;
                match ExecutorFactory::get(self.ctx.clone(), v) {
                    Ok(executor) => {
                        let result: FuseQueryResult<Vec<DataBlock>> =
                            tokio::runtime::Builder::new_multi_thread()
                                .worker_threads(self.ctx.worker_threads)
                                .build()?
                                .block_on(async move {
                                    let start = Instant::now();
                                    let mut r = vec![];
                                    let mut stream = executor.execute().await?;
                                    while let Some(block) = stream.next().await {
                                        r.push(block?);
                                    }
                                    let duration = start.elapsed();
                                    debug!("MySQLHandler executor cost:{:?}", duration);
                                    Ok(r)
                                });

                        match result {
                            Ok(blocks) => {
                                let start = Instant::now();
                                let stream = MySQLStream::create(blocks);
                                stream.execute(writer)?;
                                let duration = start.elapsed();
                                debug!("MySQLHandler send to client cost:{:?}", duration);
                            }
                            Err(e) => {
                                error!("{}", e);
                                writer.error(
                                    ErrorKind::ER_UNKNOWN_ERROR,
                                    format!("{:?}", e).as_bytes(),
                                )?
                            }
                        }
                    }
                    Err(e) => {
                        error!("{}", e);
                        writer.error(ErrorKind::ER_UNKNOWN_ERROR, format!("{:?}", e).as_bytes())?
                    }
                }
            }
            Err(e) => {
                error!("{}", e);
                writer.error(ErrorKind::ER_UNKNOWN_ERROR, format!("{:?}", e).as_bytes())?;
            }
        }
        Ok(())
    }

    fn on_init(&mut self, db: &str, _writer: InitWriter<W>) -> FuseQueryResult<()> {
        debug!("MySQL use db:{}", db);
        self.ctx.set_current_database(db)?;
        Ok(())
    }
}

pub struct MySQLHandler {
    opts: Options,
    datasource: Arc<Mutex<dyn IDataSource>>,
}

impl MySQLHandler {
    pub fn create(opts: Options, datasource: Arc<Mutex<dyn IDataSource>>) -> Self {
        MySQLHandler { opts, datasource }
    }

    pub fn start(&self) -> FuseQueryResult<()> {
        let listener =
            net::TcpListener::bind(format!("0.0.0.0:{}", self.opts.mysql_handler_port)).unwrap();

        let worker_threads = self.opts.num_cpus;
        let datasource = self.datasource.clone();
        let jh = thread::spawn(move || {
            if let Ok((s, client)) = listener.accept() {
                let ctx = Arc::new(FuseQueryContext::create_ctx(worker_threads, datasource));
                debug!(
                    "New client from {:?}:{} with {} worker threads",
                    client.ip(),
                    client.port(),
                    ctx.worker_threads
                );
                MysqlIntermediary::run_on_tcp(Session::create(ctx), s).unwrap();
            }
        });
        jh.join().unwrap();
        Ok(())
    }

    pub fn stop(&self) -> FuseQueryResult<()> {
        Ok(())
    }
}
