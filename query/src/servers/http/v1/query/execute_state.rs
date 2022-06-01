// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use common_base::base::tokio;
use common_base::base::tokio::sync::mpsc;
use common_base::base::tokio::sync::RwLock;
use common_base::base::ProgressValues;
use common_base::base::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing;
use futures::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use ExecuteState::*;

use super::http_query::HttpQueryRequest;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::interpreters::InterpreterQueryLog;
use crate::sessions::QueryContext;
use crate::sessions::SessionRef;
use crate::sql::PlanParser;
use crate::sql::Planner;
use crate::storages::result::block_buffer::BlockBuffer;
use crate::storages::result::block_buffer::BlockBufferWriterMemOnly;
use crate::storages::result::block_buffer::BlockBufferWriterWithResultTable;
use crate::storages::result::ResultQueryInfo;

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq)]
pub enum ExecuteStateKind {
    Running,
    Failed,
    Succeeded,
}

pub(crate) enum ExecuteState {
    Running(ExecuteRunning),
    Stopped(ExecuteStopped),
}

impl ExecuteState {
    pub(crate) fn extract(&self) -> (ExecuteStateKind, Option<ErrorCode>) {
        match self {
            Running(_) => (ExecuteStateKind::Running, None),
            Stopped(v) => match &v.reason {
                Ok(_) => (ExecuteStateKind::Succeeded, None),
                Err(e) => (ExecuteStateKind::Failed, Some(e.clone())),
            },
        }
    }
}

pub(crate) struct ExecuteRunning {
    // used to kill query
    session: SessionRef,
    // mainly used to get progress for now
    ctx: Arc<QueryContext>,
    interpreter: Arc<dyn Interpreter>,
}

pub(crate) struct ExecuteStopped {
    progress: Option<ProgressValues>,
    reason: Result<()>,
    stop_time: Instant,
}

pub(crate) struct Executor {
    start_time: Instant,
    pub(crate) state: ExecuteState,
}

impl Executor {
    pub(crate) fn get_progress(&self) -> Option<ProgressValues> {
        match &self.state {
            Running(r) => Some(r.ctx.get_scan_progress_value()),
            Stopped(f) => f.progress.clone(),
        }
    }

    pub(crate) fn elapsed(&self) -> Duration {
        match &self.state {
            Running(_) => Instant::now() - self.start_time,
            Stopped(f) => f.stop_time - self.start_time,
        }
    }

    pub(crate) async fn stop(this: &Arc<RwLock<Executor>>, reason: Result<()>, kill: bool) {
        let mut guard = this.write().await;
        if let Running(r) = &guard.state {
            // release session
            let progress = Some(r.ctx.get_scan_progress_value());
            if kill {
                r.session.force_kill_query();
            }
            // Write Finish to query log table.
            let _ = r
                .interpreter
                .finish()
                .await
                .map_err(|e| tracing::error!("interpreter.finish error: {:?}", e));
            guard.state = Stopped(ExecuteStopped {
                progress,
                reason: reason.clone(),
                stop_time: Instant::now(),
            });

            if let Err(e) = reason {
                if e.code() != ErrorCode::aborted_session_code()
                    && e.code() != ErrorCode::aborted_query_code()
                {
                    // query state can be pulled multi times, only log it once
                    tracing::error!("Query Error: {:?}", e);
                }
            }
        };
    }
}

pub struct HttpQueryHandle {
    pub abort_sender: mpsc::Sender<()>,
}

impl HttpQueryHandle {
    pub fn abort(&self) {
        let sender = self.abort_sender.clone();
        tokio::spawn(async move {
            sender.send(()).await.ok();
        });
    }
}

impl ExecuteState {
    pub(crate) async fn try_create(
        request: &HttpQueryRequest,
        session: SessionRef,
        ctx: Arc<QueryContext>,
        block_buffer: Arc<BlockBuffer>,
    ) -> Result<Arc<RwLock<Executor>>> {
        let sql = &request.sql;
        let start_time = Instant::now();
        ctx.attach_query_str(sql);
        let plan = match PlanParser::parse(ctx.clone(), sql).await {
            Ok(p) => p,
            Err(e) => {
                InterpreterQueryLog::fail_to_start(ctx, e.clone()).await;
                return Err(e);
            }
        };

        let interpreter = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        // Write Start to query log table.
        let _ = interpreter
            .start()
            .await
            .map_err(|e| tracing::error!("interpreter.start.error: {:?}", e));

        let running_state = ExecuteRunning {
            session,
            ctx: ctx.clone(),
            interpreter: interpreter.clone(),
        };
        let executor = Arc::new(RwLock::new(Executor {
            start_time,
            state: Running(running_state),
        }));

        let executor_clone = executor.clone();
        let ctx_clone = ctx.clone();
        let block_buffer_clone = block_buffer.clone();
        ctx.try_spawn(async move {
            if let Err(err) =
                execute(interpreter, ctx_clone, block_buffer, executor_clone.clone()).await
            {
                Executor::stop(&executor_clone, Err(err), false).await;
                block_buffer_clone.stop_push().await.unwrap();
            };
        })?;

        Ok(executor)
    }
}

async fn execute(
    interpreter: Arc<dyn Interpreter>,
    ctx: Arc<QueryContext>,
    block_buffer: Arc<BlockBuffer>,
    executor: Arc<RwLock<Executor>>,
) -> Result<()> {
    let data_stream = interpreter.execute(None).await?;
    let mut data_stream = ctx.try_create_abortable(data_stream)?;
    let use_result_cache = !ctx.get_config().query.management_mode;

    match data_stream.next().await {
        None => {
            Executor::stop(&executor, Ok(()), false).await;
            block_buffer.stop_push().await?;
        }
        Some(Err(err)) => {
            Executor::stop(&executor, Err(err), false).await;
            block_buffer.stop_push().await?;
        }
        Some(Ok(block)) => {
            let mut block_writer = if use_result_cache {
                BlockBufferWriterWithResultTable::create(
                    block_buffer.clone(),
                    ctx.clone(),
                    ResultQueryInfo {
                        query_id: ctx.get_id(),
                        schema: block.schema().clone(),
                        user: ctx.get_current_user()?.identity(),
                    },
                )
                .await?
            } else {
                Box::new(BlockBufferWriterMemOnly(block_buffer))
            };

            block_writer.push(block).await?;
            while let Some(block_r) = data_stream.next().await {
                match block_r {
                    Ok(block) => {
                        block_writer.push(block.clone()).await?;
                    }
                    Err(err) => {
                        block_writer.stop_push(true).await?;
                        return Err(err);
                    }
                };
            }
            Executor::stop(&executor, Ok(()), false).await;
            block_writer.stop_push(false).await?;
        }
    }
    Ok(())
}
