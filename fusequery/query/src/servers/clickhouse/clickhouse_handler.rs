// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::borrow::Cow;
use std::sync::Arc;

use anyhow::bail;
use anyhow::Result;
use clickhouse_srv::*;
use log::error;
use log::info;
use tokio::net::TcpListener;

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

    async fn execute_fuse_query(&self, state: &QueryState) -> Result<QueryResponse> {
        self.ctx.reset()?;
        let plan = PlanParser::create(self.ctx.clone()).build_from_sql(&state.query);
        match plan {
            Ok(v) => match InterpreterFactory::get(self.ctx.clone(), v) {
                Ok(executor) => {
                    let stream = executor.execute().await?;
                    let input_stream = Box::pin(ClickHouseStream::create(stream));
                    return Ok(QueryResponse { input_stream });
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
    }
}

#[async_trait::async_trait]
impl ClickHouseSession for Session {
    async fn execute_query(
        &self,
        state: &QueryState
    ) -> clickhouse_srv::errors::Result<QueryResponse> {
        match self.execute_fuse_query(state).await {
            Err(e) => Err(clickhouse_srv::errors::Error::Other(Cow::from(
                e.to_string()
            ))),
            Ok(v) => Ok(v)
        }
    }

    fn dbms_name(&self) -> &str {
        "datafuse"
    }

    fn server_display_name(&self) -> &str {
        "datafuse"
    }

    fn dbms_version_major(&self) -> u64 {
        2021
    }

    fn dbms_version_minor(&self) -> u64 {
        5
    }

    fn dbms_version_patch(&self) -> u64 {
        0
    }

    fn timezone(&self) -> &str {
        "UTC"
    }

    // the MIN_SERVER_REVISION for suggestions is 54406
    fn dbms_tcp_protocol_version(&self) -> u64 {
        54405
    }

    fn get_progress(&self) -> clickhouse_srv::types::Progress {
        let values = self.ctx.get_progress_value();
        info!("got new progress values {:?}", values);
        clickhouse_srv::types::Progress {
            rows: values.read_rows as u64,
            bytes: values.read_bytes as u64,
            total_rows: 0
        }
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

    pub async fn start(&self) -> Result<()> {
        let listener = TcpListener::bind(format!(
            "{}:{}",
            self.conf.clickhouse_handler_host, self.conf.clickhouse_handler_port
        ))
        .await?;

        loop {
            // Asynchronously wait for an inbound TcpStream.
            let (stream, _) = listener.accept().await?;
            let ctx = self
                .session_manager
                .try_create_context()?
                .with_cluster(self.cluster.clone())?;
            ctx.set_max_threads(self.conf.num_cpus)?;
            let session_mgr = self.session_manager.clone();
            // Spawn our handler to be run asynchronously.
            tokio::spawn(async move {
                if let Err(e) =
                    ClickHouseServer::run_on_stream(Arc::new(Session::create(ctx.clone())), stream)
                        .await
                {
                    info!("Error: {:?}", e);
                }
                session_mgr.try_remove_context(ctx).unwrap();
            });
        }
    }

    pub fn stop(&self) -> Result<()> {
        Ok(())
    }
}
