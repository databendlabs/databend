// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::borrow::Cow;
use std::net;
use std::time::Instant;

use anyhow::bail;
use anyhow::Result;
use clickhouse_srv::types::ResultWriter;
use clickhouse_srv::*;
use log::debug;
use log::error;
use metrics::histogram;
use threadpool::ThreadPool;
use tokio_stream::StreamExt;

use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::interpreters::InterpreterFactory;
use crate::servers::clickhouse::ClickHouseStream;
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

    fn execute_fuse_query(
        &self,
        query: &str,
        _stage: u64,
        writer: &mut ResultWriter
    ) -> Result<()> {
        self.ctx.reset()?;
        let start = Instant::now();
        let plan = PlanParser::create(self.ctx.clone()).build_from_sql(query);
        match plan {
            Ok(v) => match InterpreterFactory::get(self.ctx.clone(), v) {
                Ok(executor) => {
                    let start = Instant::now();

                    let result: Result<()> = tokio::runtime::Builder::new_multi_thread()
                        .enable_io()
                        .worker_threads(self.ctx.get_max_threads()? as usize)
                        .build()?
                        .block_on(async move {
                            let start = Instant::now();
                            let stream = executor.execute().await?;
                            let mut clickhouse_stream = ClickHouseStream::create(stream);
                            while let Some(block) = clickhouse_stream.next().await {
                                writer.write_block(block?)?;
                            }
                            let duration = start.elapsed();
                            debug!(
                                "ClickHouseHandler executor cost:{:?}, statistics:{:?}",
                                duration,
                                self.ctx.try_get_statistics()?
                            );
                            Ok(())
                        });

                    if let Err(e) = result {
                        error!("Execute error: {:?}", e);
                        bail!("Execute error: {:?}", e)
                    }

                    let duration = start.elapsed();
                    debug!("ClickHouseHandler send to client cost:{:?}", duration);
                }
                Err(e) => {
                    error!("Execute error: {:?}", e);
                    bail!("Execute error: {:?}", e)
                }
            },
            Err(e) => {
                error!("Execute error: {:?}", e);
                bail!("Execute error: {:?}", e)
            }
        }
        histogram!(
            super::clickhouse_metrics::METRIC_CLICKHOUSE_PROCESSOR_REQUEST_DURATION,
            start.elapsed()
        );

        Ok(())
    }
}

impl ClickHouseSession for Session {
    fn execute_query(
        &self,
        query: &str,
        stage: u64,
        writer: &mut ResultWriter
    ) -> clickhouse_srv::errors::Result<()> {
        match self.execute_fuse_query(query, stage, writer) {
            Err(e) => Err(clickhouse_srv::errors::Error::Other(Cow::from(
                e.to_string()
            ))),
            _ => Ok(())
        }
    }

    fn dbms_name(&self) -> &str {
        "datafuse"
    }

    fn server_display_name(&self) -> &str {
        "datafuse"
    }
}

pub struct ClickHouseHandler {
    conf: Config,
    cluster: ClusterRef,
    session_manager: SessionRef
}

impl ClickHouseHandler {
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
            self.conf.clickhouse_handler_host, self.conf.clickhouse_handler_port
        ))?;
        let pool = ThreadPool::new(self.conf.clickhouse_handler_thread_num as usize);

        for stream in listener.incoming() {
            let stream = stream?;
            let ctx = self
                .session_manager
                .try_create_context()?
                .with_cluster(self.cluster.clone())?;
            ctx.set_max_threads(self.conf.num_cpus)?;

            let session_mgr = self.session_manager.clone();
            pool.execute(move || {
                ClickHouseServer::run_on_tcp(Session::create(ctx.clone()), stream).unwrap();
                session_mgr.try_remove_context(ctx).unwrap();
            })
        }
        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        Ok(())
    }
}
