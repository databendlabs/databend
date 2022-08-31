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

use common_base::base::tokio::sync::mpsc;
use common_base::base::tokio::sync::RwLock;
use common_base::base::GlobalIORuntime;
use common_base::base::ProgressValues;
use common_base::base::TrySpawn;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::future::AbortHandle;
use futures::future::Abortable;
use futures::StreamExt;
use futures_util::FutureExt;
use serde::Deserialize;
use serde::Serialize;
use tracing::error;
use ExecuteState::*;

use super::http_query::HttpQueryRequest;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::interpreters::InterpreterFactoryV2;
use crate::interpreters::InterpreterQueryLog;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::Pipe;
use crate::pipelines::PipelineBuildResult;
use crate::servers::utils::use_planner_v2;
use crate::sessions::QueryAffect;
use crate::sessions::QueryContext;
use crate::sessions::Session;
use crate::sessions::TableContext;
use crate::sql::ColumnBinding;
use crate::sql::DfParser;
use crate::sql::DfStatement;
use crate::sql::PlanParser;
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

pub struct ExecuteRunning {
    // used to kill query
    session: Arc<Session>,
    // mainly used to get progress for now
    ctx: Arc<QueryContext>,
    interpreter: Arc<dyn Interpreter>,
}

pub struct ExecuteStopped {
    stats: Progresses,
    affect: Option<QueryAffect>,
    reason: Result<()>,
    stop_time: Instant,
}

pub struct Executor {
    start_time: Instant,
    pub(crate) state: ExecuteState,
}

impl Executor {
    pub(crate) fn get_progress(&self) -> Progresses {
        match &self.state {
            Running(r) => Progresses::from_context(&r.ctx),
            Stopped(f) => f.stats.clone(),
        }
    }

    pub(crate) fn get_affect(&self) -> Option<QueryAffect> {
        match &self.state {
            Running(r) => r.ctx.get_affect(),
            Stopped(r) => r.affect.clone(),
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
            if kill {
                r.session.force_kill_query();
            }
            // Write Finish to query log table.
            let _ = r
                .interpreter
                .finish()
                .await
                .map_err(|e| error!("interpreter.finish error: {:?}", e));
            guard.state = Stopped(ExecuteStopped {
                stats: Progresses::from_context(&r.ctx),
                reason: reason.clone(),
                stop_time: Instant::now(),
                affect: r.ctx.get_affect(),
            });

            if let Err(e) = reason {
                if e.code() != ErrorCode::aborted_session_code()
                    && e.code() != ErrorCode::aborted_query_code()
                {
                    // query state can be pulled multi times, only log it once
                    error!("Query Error: {:?}", e);
                }
            }
        };
    }
}

impl ExecuteState {
    pub(crate) async fn try_create(
        request: &HttpQueryRequest,
        session: Arc<Session>,
        ctx: Arc<QueryContext>,
        block_buffer: Arc<BlockBuffer>,
    ) -> Result<Arc<RwLock<Executor>>> {
        let sql = &request.sql;
        let start_time = Instant::now();
        ctx.attach_query_str(sql);

        let stmts = DfParser::parse_sql(sql, ctx.get_current_session().get_type());
        let settings = ctx.get_settings();
        let is_v2 = use_planner_v2(&settings, &stmts)?;
        let is_select = if let Ok((s, _)) = &stmts {
            s.get(0)
                .map_or(false, |stmt| matches!(stmt, DfStatement::Query(_)))
        } else {
            false
        };

        let interpreter = if is_v2 {
            let mut planner = Planner::new(ctx.clone());
            let (plan, _, _) = planner.plan_sql(sql).await?;
            InterpreterFactoryV2::get(ctx.clone(), &plan)
        } else {
            let plan = match PlanParser::parse(ctx.clone(), sql).await {
                Ok(p) => p,
                Err(e) => {
                    InterpreterQueryLog::fail_to_start(ctx, e.clone()).await;
                    return Err(e);
                }
            };
            InterpreterFactory::get(ctx.clone(), plan)
        }?;
        if is_v2 && is_select {
            let _ = interpreter
                .start()
                .await
                .map_err(|e| error!("interpreter.start.error: {:?}", e));
            let running_state = ExecuteRunning {
                session,
                ctx: ctx.clone(),
                interpreter: interpreter.clone(),
            };
            let executor = Arc::new(RwLock::new(Executor {
                start_time,
                state: Running(running_state),
            }));
            ctx.attach_http_query(HttpQueryHandle {
                executor: executor.clone(),
                block_buffer,
            });
            interpreter.execute().await?;
            Ok(executor)
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
            let executor = Arc::new(RwLock::new(Executor {
                start_time,
                state: Running(running_state),
            }));

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

            Ok(executor)
        }
    }
}

async fn execute(
    interpreter: Arc<dyn Interpreter>,
    ctx: Arc<QueryContext>,
    block_buffer: Arc<BlockBuffer>,
    executor: Arc<RwLock<Executor>>,
) -> Result<()> {
    let data_stream = interpreter.execute().await?;
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
        result_columns: &[ColumnBinding],
    ) -> Result<SendableDataBlockStream> {
        let id = ctx.get_id();
        tracing::info!("http query {id} execute() begin");

        let executor = self.executor.clone();
        let block_buffer = self.block_buffer.clone();

        build_res.main_pipeline.resize(1)?;
        let input = InputPort::create();

        let schema = DataSchemaRefExt::create(
            result_columns
                .iter()
                .map(|v| DataField::new(&v.column_name, *v.data_type.clone()))
                .collect(),
        );

        let query_info = ResultQueryInfo {
            query_id: ctx.get_id(),
            schema: schema.clone(),
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

        let async_runtime = GlobalIORuntime::instance();
        let async_runtime_clone = async_runtime.clone();
        let query_need_abort = ctx.query_need_abort();
        let executor_settings = ExecutorSettings::try_create(&ctx.get_settings())?;

        let run = move || -> Result<()> {
            let mut pipelines = build_res.sources_pipelines;
            pipelines.push(build_res.main_pipeline);

            let pipeline_executor = PipelineCompleteExecutor::from_pipelines(
                async_runtime_clone,
                query_need_abort,
                pipelines,
                executor_settings,
            )?;
            pipeline_executor.execute()
        };

        let (error_sender, mut error_receiver) = mpsc::channel::<Result<()>>(1);
        let (abort_handle, abort_registration) = AbortHandle::new_pair();

        async_runtime.spawn(async move {
            let error_receiver = Abortable::new(error_receiver.recv(), abort_registration);
            ctx.add_source_abort_handle(abort_handle);
            match error_receiver.await {
                Err(_) => {
                    Executor::stop(&executor, Err(ErrorCode::AbortedQuery("")), false).await;
                    block_buffer.stop_push().await.unwrap();
                }
                Ok(Some(Err(e))) => {
                    Executor::stop(&executor, Err(e), false).await;
                    block_buffer.stop_push().await.unwrap();
                }
                _ => {
                    Executor::stop(&executor, Ok(()), false).await;
                    block_buffer.stop_push().await.unwrap();
                }
            }
        });

        std::thread::spawn(move || {
            if let Err(cause) = run() {
                if error_sender.blocking_send(Err(cause)).is_err() {
                    tracing::warn!("Error sender is disconnect");
                }
            }
        });
        tracing::info!("http query {id} execute() end");
        Ok(Box::pin(DataBlockStream::create(schema, None, vec![])))
    }
}
