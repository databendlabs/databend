// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use clickhouse_srv::connection::Connection;
use clickhouse_srv::*;
use common_exception::ErrorCodes;
use common_exception::Result;
use log::info;
use metrics::histogram;
use tokio::net::TcpListener;
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
}

pub fn to_clickhouse_err(res: ErrorCodes) -> clickhouse_srv::errors::Error {
    clickhouse_srv::errors::Error::Other(Cow::from(res.to_string()))
}

#[async_trait::async_trait]
impl ClickHouseSession for Session {
    async fn execute_query(
        &self,
        ctx: &mut CHContext,
        connection: &mut Connection
    ) -> clickhouse_srv::errors::Result<()> {
        self.ctx.reset().map_err(to_clickhouse_err)?;
        let start = Instant::now();
        let mut last_progress_send = Instant::now();

        let interpreter = PlanParser::create(self.ctx.clone())
            .build_from_sql(&ctx.state.query)
            .and_then(|built_plan| InterpreterFactory::get(self.ctx.clone(), built_plan))
            .map_err(to_clickhouse_err)?;

        let mut clickhouse_stream = interpreter
            .execute()
            .await
            .map(|stream| ClickHouseStream::create(stream))
            .map_err(to_clickhouse_err)?;

        while let Some(block) = clickhouse_stream.next().await {
            connection
                .write_block(block.map_err(to_clickhouse_err)?)
                .await?;
            if last_progress_send.elapsed() >= Duration::from_millis(10) {
                let progress = self.get_progress();
                connection
                    .write_progress(progress, ctx.client_revision)
                    .await?;

                last_progress_send = Instant::now();
            }
        }

        let progress = self.get_progress();
        connection
            .write_progress(progress, ctx.client_revision)
            .await?;

        histogram!(
            super::clickhouse_metrics::METRIC_CLICKHOUSE_PROCESSOR_REQUEST_DURATION,
            start.elapsed()
        );
        Ok(())
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

    pub async fn start(&self) -> anyhow::Result<()> {
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
