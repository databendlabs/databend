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

use common_base::base::tokio::sync::mpsc;
use common_base::base::tokio::sync::RwLock;
use common_base::base::ProgressValues;
use common_base::base::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanNode;
use common_planners::PlanNode::Insert;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use futures::future::AbortHandle;
use futures::future::Abortable;
use futures::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use ExecuteState::*;

use super::http_query::HttpQueryRequest;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::interpreters::InterpreterFactoryV2;
use crate::interpreters::InterpreterQueryLog;
use crate::pipelines::new::executor::PipelineCompleteExecutor;
use crate::pipelines::new::executor::PipelineExecutor;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::NewPipe;
use crate::sessions::QueryContext;
use crate::sessions::SessionRef;
use crate::sql::exec::PipelineBuilder;
use crate::sql::DfParser;
use crate::sql::DfStatement;
use crate::sql::PlanParser;
use crate::sql::Planner;
use crate::storages::result::block_buffer::BlockBuffer;
use crate::storages::result::block_buffer::BlockBufferWriterMemOnly;
use crate::storages::result::block_buffer::BlockBufferWriterWithResultTable;
use crate::storages::result::ResultQueryInfo;
use crate::storages::result::ResultTableSink;

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq)]
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
    session: SessionRef,
    // mainly used to get progress for now
    ctx: Arc<QueryContext>,
    interpreter: Arc<dyn Interpreter>,
}

pub struct ExecuteStopped {
    stats: Progresses,
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
                .map_err(|e| tracing::error!("interpreter.finish error: {:?}", e));
            guard.state = Stopped(ExecuteStopped {
                stats: Progresses::from_context(&r.ctx),
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

        let (stmts, _) = match DfParser::parse_sql(sql, ctx.get_current_session().get_type()) {
            Ok(t) => t,
            Err(e) => {
                InterpreterQueryLog::fail_to_start(ctx, e.clone()).await;
                return Err(e);
            }
        };

        let settings = ctx.get_settings();
        if settings.get_enable_new_processor_framework()? != 0
            && !ctx.get_config().query.management_mode
            && ctx.get_cluster().is_empty()
            && settings.get_enable_planner_v2()? != 0
            && matches!(stmts.get(0), Some(DfStatement::Query(_)))
        {
            let mut planner = Planner::new(ctx.clone());
            let (plan, _) = planner.plan_sql(sql).await?;
            let interpreter = InterpreterFactoryV2::get(ctx.clone(), &plan)?;

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
            ctx.attach_http_query(HttpQueryHandle {
                executor: executor.clone(),
                block_buffer,
            });
            interpreter.execute(None).await?;

            Ok(executor)
        } else {
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
                if let Err(err) = execute(
                    interpreter,
                    ctx_clone,
                    block_buffer,
                    executor_clone.clone(),
                    Arc::new(plan),
                )
                .await
                {
                    Executor::stop(&executor_clone, Err(err), false).await;
                    block_buffer_clone.stop_push().await.unwrap();
                };
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
    plan: Arc<PlanNode>,
) -> Result<()> {
    let data_stream: Result<SendableDataBlockStream> =
        if ctx.get_settings().get_enable_async_insert()? != 0
            && matches!(&*plan, PlanNode::Insert(_))
        {
            match &*plan {
                Insert(insert_plan) => {
                    let queue = ctx
                        .get_current_session()
                        .get_session_manager()
                        .get_async_insert_queue()
                        .read()
                        .clone()
                        .unwrap();
                    queue
                        .clone()
                        .push(Arc::new(insert_plan.to_owned()), ctx.clone())
                        .await?;

                    let wait_for_async_insert = ctx.get_settings().get_wait_for_async_insert()?;
                    let wait_for_async_insert_timeout =
                        ctx.get_settings().get_wait_for_async_insert_timeout()?;

                    if wait_for_async_insert == 1 {
                        queue
                            .clone()
                            .wait_for_processing_insert(
                                ctx.get_id(),
                                Duration::from_secs(wait_for_async_insert_timeout),
                            )
                            .await?;
                    }

                    Ok(Box::pin(DataBlockStream::create(
                        plan.schema(),
                        None,
                        vec![],
                    )))
                }
                _ => unreachable!(),
            }
        } else {
            interpreter.execute(None).await
        };

    let mut data_stream = ctx.try_create_abortable(data_stream?)?;
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
        pb: PipelineBuilder,
    ) -> Result<SendableDataBlockStream> {
        let executor = self.executor.clone();
        let block_buffer = self.block_buffer.clone();
        let (mut root_pipeline, pipelines, schema) = pb.spawn()?;
        let async_runtime = ctx.get_storage_runtime();

        root_pipeline.resize(1)?;
        let input = InputPort::create();

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
        root_pipeline.add_pipe(NewPipe::SimplePipe {
            outputs_port: vec![],
            inputs_port: vec![input],
            processors: vec![sink],
        });
        let pipeline_executor =
            PipelineCompleteExecutor::try_create(async_runtime.clone(), root_pipeline)?;
        let async_runtime_clone = async_runtime.clone();
        let run = move || -> Result<()> {
            for pipeline in pipelines {
                let executor = PipelineExecutor::create(async_runtime_clone.clone(), pipeline)?;
                executor.execute()?;
            }
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
                error_sender.blocking_send(Err(cause)).unwrap();
            }
        });
        Ok(Box::pin(DataBlockStream::create(schema, None, vec![])))
    }
}
