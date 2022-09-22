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

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use common_base::base::tokio::sync::mpsc;
use common_base::base::tokio::sync::RwLock;
use common_base::base::GlobalIORuntime;
use common_base::base::ProgressValues;
use common_base::base::Thread;
use common_base::base::TrySpawn;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;
use futures_util::FutureExt;
use serde::Deserialize;
use serde::Serialize;
use tracing::error;
use ExecuteState::*;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::interpreters::InterpreterQueryLog;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::Pipe;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryAffect;
use crate::sessions::QueryContext;
use crate::sessions::Session;
use crate::sessions::TableContext;
use crate::sql::plans::Plan;
use crate::sql::Planner;
use crate::storages::result::block_buffer::BlockBuffer;
use crate::storages::result::block_buffer::BlockBufferWriterMemOnly;
use crate::storages::result::block_buffer::BlockBufferWriterWithResultTable;
use crate::storages::result::ResultQueryInfo;
use crate::storages::result::ResultTableSink;

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
pub enum ExecuteStateKind {
    Running,
    Failed,
    Succeeded,
}

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
pub struct Progresses {
    pub scan_progress: ProgressValues,
    pub write_progress: ProgressValues,
    pub result_progress: ProgressValues,
}

impl Progresses {
    fn from_context(ctx: &Arc<QueryContext>) -> Self {
        Progresses {
            scan_progress: ctx.get_scan_progress_value(),
            write_progress: ctx.get_write_progress_value(),
            result_progress: ctx.get_result_progress_value(),
        }
    }
}

pub enum ExecuteState {
    Starting(ExecuteStarting),
    Running(ExecuteRunning),
    Stopped(ExecuteStopped),
}

impl ExecuteState {
    pub(crate) fn extract(&self) -> (ExecuteStateKind, Option<ErrorCode>) {
        match self {
            Starting(_) | Running(_) => (ExecuteStateKind::Running, None),
            Stopped(v) => match &v.reason {
                Ok(_) => (ExecuteStateKind::Succeeded, None),
                Err(e) => (ExecuteStateKind::Failed, Some(e.clone())),
            },
        }
    }
}

pub struct ExecuteStarting {
    pub(crate) ctx: Arc<QueryContext>,
}

pub struct ExecuteRunning {
    // used to kill query
    session: Arc<Session>,
    // mainly used to get progress for now
    ctx: Arc<QueryContext>,
    interpreter: Arc<dyn Interpreter>,
}

pub struct ExecuteStopped {
    pub stats: Progresses,
    pub affect: Option<QueryAffect>,
    pub reason: Result<()>,
    pub stop_time: Instant,
}

pub struct Executor {
    pub query_id: String,
    pub start_time: Instant,
    pub state: ExecuteState,
}

impl Executor {
    pub fn get_progress(&self) -> Progresses {
        match &self.state {
            Starting(_) => Default::default(),
            Running(r) => Progresses::from_context(&r.ctx),
            Stopped(f) => f.stats.clone(),
        }
    }

    pub fn get_affect(&self) -> Option<QueryAffect> {
        match &self.state {
            Starting(_) => None,
            Running(r) => r.ctx.get_affect(),
            Stopped(r) => r.affect.clone(),
        }
    }

    pub fn elapsed(&self) -> Duration {
        match &self.state {
            Starting(_) | Running(_) => Instant::now() - self.start_time,
            Stopped(f) => f.stop_time - self.start_time,
        }
    }

    pub async fn start_to_running(this: &Arc<RwLock<Executor>>, state: ExecuteState) {
        let mut guard = this.write().await;
        if let Starting(_) = &guard.state {
            guard.state = state
        }
    }

    pub async fn start_to_stop(this: &Arc<RwLock<Executor>>, state: ExecuteState) {
        let mut guard = this.write().await;
        if let Starting(_) = &guard.state {
            guard.state = state
        }
    }
    pub async fn stop(this: &Arc<RwLock<Executor>>, reason: Result<()>, kill: bool) {
        {
            let guard = this.read().await;
            tracing::info!(
                "http query {}: change state to Stopped, reason {:?}",
                &guard.query_id,
                reason
            );
        }

        let mut guard = this.write().await;
        match &guard.state {
            Starting(s) => {
                if let Err(e) = &reason {
                    s.ctx.set_error(e.clone());
                    InterpreterQueryLog::create(s.ctx.clone(), "".to_string())
                        .log_finish(SystemTime::now(), Some(e.clone()))
                        .unwrap_or_else(|e| error!("fail to write query_log {:?}", e));
                }
                guard.state = Stopped(ExecuteStopped {
                    stats: Default::default(),
                    reason,
                    stop_time: Instant::now(),
                    affect: Default::default(),
                })
            }
            Running(r) => {
                // release session
                if kill {
                    r.session.force_kill_query(ErrorCode::AbortedQuery(
                        "Aborted query, because the server is shutting down or the query was killed",
                    ));
                }
                if let Err(e) = &reason {
                    r.ctx.set_error(e.clone());
                }
                // Write Finish to query log table.
                let _ = r
                    .interpreter
                    .finish()
                    .await
                    .map_err(|e| error!("interpreter.finish error: {:?}", e));
                guard.state = Stopped(ExecuteStopped {
                    stats: Progresses::from_context(&r.ctx),
                    reason,
                    stop_time: Instant::now(),
                    affect: r.ctx.get_affect(),
                })
            }
            Stopped(s) => {
                tracing::info!(
                    "http query {}: already stopped, reason {:?}, new reason {:?}",
                    &guard.query_id,
                    s.reason,
                    reason
                );
            }
        }
    }
}

impl ExecuteState {
    pub(crate) async fn try_start_query(
        executor: Arc<RwLock<Executor>>,
        sql: &str,
        session: Arc<Session>,
        ctx: Arc<QueryContext>,
        block_buffer: Arc<BlockBuffer>,
    ) -> Result<ExecuteRunning> {
        ctx.attach_query_str(sql);

        let mut planner = Planner::new(ctx.clone());
        let (plan, _, _) = planner.plan_sql(sql).await?;
        let is_select = matches!(&plan, Plan::Query { .. });
        let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;

        if is_select {
            let _ = interpreter
                .start()
                .await
                .map_err(|e| error!("interpreter.start.error: {:?}", e));
            let running_state = ExecuteRunning {
                session,
                ctx: ctx.clone(),
                interpreter: interpreter.clone(),
            };
            ctx.attach_http_query(HttpQueryHandle {
                executor: executor.clone(),
                block_buffer,
            });
            interpreter.execute(ctx).await?;
            Ok(running_state)
        } else {
            // Write Start to query log table.
            let _ = interpreter
                .start()
                .await
                .map_err(|e| error!("interpreter.start.error: {:?}", e));

            let running_state = ExecuteRunning {
                session,
                ctx: ctx.clone(),
                interpreter: interpreter.clone(),
            };

            let executor_clone = executor.clone();
            let ctx_clone = ctx.clone();
            let block_buffer_clone = block_buffer.clone();

            ctx.try_spawn(async move {
                let res = execute(interpreter, ctx_clone, block_buffer, executor_clone.clone());
                match AssertUnwindSafe(res).catch_unwind().await {
                    Ok(Err(err)) => {
                        Executor::stop(&executor_clone, Err(err), false).await;
                        block_buffer_clone.stop_push().await.unwrap();
                    }
                    Err(_) => {
                        Executor::stop(
                            &executor_clone,
                            Err(ErrorCode::PanicError("interpreter panic!")),
                            false,
                        )
                        .await;
                        block_buffer_clone.stop_push().await.unwrap();
                    }
                    _ => {}
                }
            })?;
            Ok(running_state)
        }
    }
}

async fn execute(
    interpreter: Arc<dyn Interpreter>,
    ctx: Arc<QueryContext>,
    block_buffer: Arc<BlockBuffer>,
    executor: Arc<RwLock<Executor>>,
) -> Result<()> {
    let mut data_stream = interpreter.execute(ctx.clone()).await?;
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

#[derive(Clone)]
pub struct HttpQueryHandle {
    pub executor: Arc<RwLock<Executor>>,
    pub block_buffer: Arc<BlockBuffer>,
}

impl HttpQueryHandle {
    pub async fn execute(
        self,
        ctx: Arc<QueryContext>,
        mut build_res: PipelineBuildResult,
        result_schema: DataSchemaRef,
    ) -> Result<SendableDataBlockStream> {
        let executor = self.executor.clone();
        let block_buffer = self.block_buffer.clone();

        build_res.main_pipeline.resize(1)?;
        let input = InputPort::create();

        let query_info = ResultQueryInfo {
            query_id: ctx.get_id(),
            schema: result_schema.clone(),
            user: ctx.get_current_user()?.identity(),
        };
        let data_accessor = ctx.get_storage_operator()?;

        let sink = ResultTableSink::create(
            input.clone(),
            ctx.clone(),
            data_accessor,
            query_info,
            self.block_buffer,
        )?;

        build_res.main_pipeline.add_pipe(Pipe::SimplePipe {
            outputs_port: vec![],
            inputs_port: vec![input],
            processors: vec![sink],
        });

        let query_ctx = ctx.clone();
        let executor_settings = ExecutorSettings::try_create(&ctx.get_settings())?;

        let run = move || -> Result<()> {
            let mut pipelines = build_res.sources_pipelines;
            pipelines.push(build_res.main_pipeline);

            let pipeline_executor =
                PipelineCompleteExecutor::from_pipelines(pipelines, executor_settings)?;

            query_ctx.set_executor(Arc::downgrade(&pipeline_executor.get_inner()));
            pipeline_executor.execute()
        };

        let (error_sender, mut error_receiver) = mpsc::channel::<Result<()>>(1);

        GlobalIORuntime::instance().spawn(async move {
            match error_receiver.recv().await {
                Some(Err(e)) => {
                    Executor::stop(&executor, Err(e), false).await;
                    block_buffer.stop_push().await.unwrap();
                }
                _ => {
                    Executor::stop(&executor, Ok(()), false).await;
                    block_buffer.stop_push().await.unwrap();
                }
            }
        });

        Thread::spawn(move || {
            if let Err(cause) = run() {
                if error_sender.blocking_send(Err(cause)).is_err() {
                    tracing::warn!("Error sender is disconnect");
                }
            }
        });
        Ok(Box::pin(DataBlockStream::create(
            result_schema,
            None,
            vec![],
        )))
    }
}
