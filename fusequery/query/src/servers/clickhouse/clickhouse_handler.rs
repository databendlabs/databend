// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use clickhouse_srv::connection::Connection;
use clickhouse_srv::errors::ServerError;
use clickhouse_srv::types::Block as ClickHouseBlock;
use clickhouse_srv::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_runtime::tokio;
use common_runtime::tokio::net::TcpListener;
use common_runtime::tokio::sync::mpsc;
use common_runtime::tokio::time;
use log::error;
use metrics::histogram;
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::StreamExt;

use crate::configs::Config;
use crate::interpreters::InterpreterFactory;
use crate::servers::clickhouse::ClickHouseStream;
use crate::sessions::FuseQueryContextRef;
use crate::sessions::SessionMgrRef;
use crate::sql::PlanParser;

struct Session {
    ctx: FuseQueryContextRef,
}

impl Session {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        Session { ctx }
    }
}

pub fn to_clickhouse_err(res: ErrorCode) -> clickhouse_srv::errors::Error {
    clickhouse_srv::errors::Error::Server(ServerError {
        code: res.code() as u32,
        name: "DB:Exception".to_string(),
        message: res.message(),
        stack_trace: res.backtrace_str(),
    })
}

enum BlockItem {
    Block(Result<ClickHouseBlock>),
    ProgressTicker,
}

#[async_trait::async_trait]
impl ClickHouseSession for Session {
    async fn execute_query(
        &self,
        ctx: &mut CHContext,
        connection: &mut Connection,
    ) -> clickhouse_srv::errors::Result<()> {
        self.ctx.reset().map_err(to_clickhouse_err)?;
        let start = Instant::now();

        let interpreter = PlanParser::create(self.ctx.clone())
            .build_from_sql(&ctx.state.query)
            .and_then(|built_plan| InterpreterFactory::get(self.ctx.clone(), built_plan))
            .map_err(to_clickhouse_err)?;

        let schema = interpreter.schema();

        let mut interval_stream = IntervalStream::new(time::interval(Duration::from_millis(30)));
        let (tx, mut rx) = mpsc::channel(20);
        let cancel = Arc::new(AtomicBool::new(false));

        let tx2 = tx.clone();
        let cancel_clone = cancel.clone();

        tokio::spawn(async move {
            while !cancel.load(Ordering::Relaxed) {
                let _ = interval_stream.next().await;
                tx.send(BlockItem::ProgressTicker).await.ok();
            }
        });

        self.ctx.execute_task(async move {
            let clickhouse_stream = interpreter
                .execute()
                .await
                .map(|stream| ClickHouseStream::create(stream, schema));

            match clickhouse_stream {
                Ok(mut clickhouse_stream) => {
                    while let Some(block) = clickhouse_stream.next().await {
                        tx2.send(BlockItem::Block(block)).await.ok();
                    }
                }

                Err(e) => {
                    tx2.send(BlockItem::Block(Err(e))).await.ok();
                }
            }
            cancel_clone.store(true, Ordering::Relaxed);
        });

        while let Some(item) = rx.recv().await {
            match item {
                BlockItem::Block(block) => {
                    connection
                        .write_block(block.map_err(to_clickhouse_err)?)
                        .await?;
                }

                BlockItem::ProgressTicker => {
                    let progress = self.get_progress();
                    connection
                        .write_progress(progress, ctx.client_revision)
                        .await?;
                }
            }
        }

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
        let values = self.ctx.get_and_reset_progress_value();
        clickhouse_srv::types::Progress {
            rows: values.read_rows as u64,
            bytes: values.read_bytes as u64,
            total_rows: 0,
        }
    }
}

pub struct ClickHouseHandler {
    conf: Config,
    session_manager: SessionMgrRef,
}

impl ClickHouseHandler {
    pub fn create(conf: Config, session_manager: SessionMgrRef) -> Self {
        Self {
            conf,
            session_manager,
        }
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(format!(
            "{}:{}",
            self.conf.clickhouse_handler_host, self.conf.clickhouse_handler_port
        ))
        .await?;

        loop {
            let session_mgr = self.session_manager.clone();
            // Asynchronously wait for an inbound TcpStream.
            let (stream, _) = listener.accept().await?;
            let ctx = self.session_manager.try_create_context()?;
            ctx.set_max_threads(self.conf.num_cpus)?;

            // Spawn our handler to be run asynchronously.
            tokio::spawn(async move {
                if let Err(e) =
                    ClickHouseServer::run_on_stream(Arc::new(Session::create(ctx.clone())), stream)
                        .await
                {
                    error!("Error: {:?}", e);
                }
                session_mgr.try_remove_context(ctx).unwrap();
            });
        }
    }

    pub fn stop(&self) -> Result<()> {
        Ok(())
    }
}
