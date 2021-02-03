// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use log::{debug, error};

use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::{io, net};

use futures::stream::StreamExt;
use msql_srv::*;
use threadpool::ThreadPool;

use crate::contexts::{FuseQueryContext, FuseQueryContextRef, Options};
use crate::datablocks::DataBlock;
use crate::datasources::IDataSource;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::executors::ExecutorFactory;
use crate::servers::mysql::MySQLStream;
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

        let plan = PlanParser::new().build_from_sql(self.ctx.clone(), query);
        match plan {
            Ok(v) => match ExecutorFactory::get(self.ctx.clone(), v) {
                Ok(executor) => {
                    let result: FuseQueryResult<Vec<DataBlock>> =
                        tokio::runtime::Builder::new_multi_thread()
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
                                    self.ctx.get_statistics()?
                                );
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
                            writer
                                .error(ErrorKind::ER_UNKNOWN_ERROR, format!("{:?}", e).as_bytes())?
                        }
                    }
                }
                Err(e) => {
                    error!("{}", e);
                    writer.error(ErrorKind::ER_UNKNOWN_ERROR, format!("{:?}", e).as_bytes())?
                }
            },
            Err(e) => {
                error!("{}", e);
                writer.error(ErrorKind::ER_UNKNOWN_ERROR, format!("{:?}", e).as_bytes())?;
            }
        }
        Ok(())
    }

    fn on_init(&mut self, db: &str, writer: InitWriter<W>) -> FuseQueryResult<()> {
        debug!("MySQL use db:{}", db);
        match self.ctx.set_default_db(db.to_string()) {
            Ok(..) => {}
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

pub struct MySQLHandler {
    opts: Options,
    datasource: Arc<Mutex<dyn IDataSource>>,
}

impl MySQLHandler {
    pub fn create(opts: Options, datasource: Arc<Mutex<dyn IDataSource>>) -> Self {
        MySQLHandler { opts, datasource }
    }

    pub fn start(&self) -> FuseQueryResult<()> {
        let listener = net::TcpListener::bind(format!(
            "{}:{}",
            self.opts.get_mysql_listen_host()?,
            self.opts.get_mysql_handler_port()?
        ))?;
        let pool = ThreadPool::new(self.opts.get_mysql_handler_thread_num()? as usize);

        for stream in listener.incoming() {
            let stream = stream?;
            let datasource = self.datasource.clone();
            let ctx = FuseQueryContext::try_create_ctx(datasource)?;
            ctx.set_max_threads(self.opts.get_num_cpus()?)?;

            pool.execute(move || {
                MysqlIntermediary::run_on_tcp(Session::create(ctx), stream).unwrap();
            })
        }
        Ok(())
    }

    pub fn stop(&self) -> FuseQueryResult<()> {
        Ok(())
    }
}
