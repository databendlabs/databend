// Copyright 2021 Datafuse Labs
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

use common_base::base::tokio::sync::RwLock;
use common_base::base::ProgressValues;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_sql::Planner;
use futures::StreamExt;
use futures_util::FutureExt;
use serde::Deserialize;
use serde::Serialize;
use tracing::error;
use tracing::info;
use ExecuteState::*;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::interpreters::InterpreterQueryLog;
use crate::servers::http::v1::query::sized_spsc::SizedChannelSender;
use crate::sessions::QueryAffect;
use crate::sessions::QueryContext;
use crate::sessions::Session;
use crate::sessions::TableContext;

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
pub enum ExecuteStateKind {
    Running,
    Failed,
    Succeeded,
}

impl std::fmt::Display for ExecuteStateKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
pub struct Progresses {
    pub scan_progress: ProgressValues,
    pub write_progress: ProgressValues,
    pub result_progress: ProgressValues,
    pub total_scan: ProgressValues,
}

impl Progresses {
    fn from_context(ctx: &Arc<QueryContext>) -> Self {
        Progresses {
            scan_progress: ctx.get_scan_progress_value(),
            write_progress: ctx.get_write_progress_value(),
            result_progress: ctx.get_result_progress_value(),
            total_scan: ctx.get_total_scan_value(),
        }
    }
}

pub enum ExecuteState {
    Starting(ExecuteStarting),
    Running(ExecuteRunning),
    Stopped(Box<ExecuteStopped>),
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

    #[async_backtrace::framed]
    pub async fn start_to_running(this: &Arc<RwLock<Executor>>, state: ExecuteState) {
        let mut guard = this.write().await;
        if let Starting(_) = &guard.state {
            guard.state = state
        }
    }

    #[async_backtrace::framed]
    pub async fn start_to_stop(this: &Arc<RwLock<Executor>>, state: ExecuteState) {
        let mut guard = this.write().await;
        if let Starting(_) = &guard.state {
            guard.state = state
        }
    }
    #[async_backtrace::framed]
    pub async fn stop(this: &Arc<RwLock<Executor>>, reason: Result<()>, kill: bool) {
        {
            let guard = this.read().await;
            info!(
                "http query {}: change state to Stopped, reason {:?}",
                &guard.query_id, reason
            );
        }

        let mut guard = this.write().await;
        match &guard.state {
            Starting(s) => {
                if let Err(e) = &reason {
                    InterpreterQueryLog::log_finish(&s.ctx, SystemTime::now(), Some(e.clone()))
                        .unwrap_or_else(|e| error!("fail to write query_log {:?}", e));
                }
                guard.state = Stopped(Box::new(ExecuteStopped {
                    stats: Default::default(),
                    reason,
                    stop_time: Instant::now(),
                    affect: Default::default(),
                }))
            }
            Running(r) => {
                // release session
                if kill {
                    if let Err(error) = &reason {
                        r.session.force_kill_query(error.clone());
                    } else {
                        r.session.force_kill_query(ErrorCode::AbortedQuery(
                            "Aborted query, because the server is shutting down or the query was killed",
                        ));
                    }
                }

                guard.state = Stopped(Box::new(ExecuteStopped {
                    stats: Progresses::from_context(&r.ctx),
                    reason,
                    stop_time: Instant::now(),
                    affect: r.ctx.get_affect(),
                }))
            }
            Stopped(s) => {
                info!(
                    "http query {}: already stopped, reason {:?}, new reason {:?}",
                    &guard.query_id, s.reason, reason
                );
            }
        }
    }
}

impl ExecuteState {
    #[async_backtrace::framed]
    pub(crate) async fn get_schema(sql: &str, ctx: Arc<QueryContext>) -> Result<DataSchemaRef> {
        let mut planner = Planner::new(ctx.clone());
        let (plan, _) = planner.plan_sql(sql).await?;
        Ok(InterpreterFactory::get_schema(ctx, &plan))
    }

    #[async_backtrace::framed]
    pub(crate) async fn try_start_query(
        executor: Arc<RwLock<Executor>>,
        sql: &str,
        session: Arc<Session>,
        ctx: Arc<QueryContext>,
        block_sender: SizedChannelSender<DataBlock>,
    ) -> Result<()> {
        let mut planner = Planner::new(ctx.clone());
        let (plan, extras) = planner.plan_sql(sql).await?;
        ctx.attach_query_str(plan.to_string(), extras.statement.to_mask_sql());

        let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let running_state = ExecuteRunning {
            session,
            ctx: ctx.clone(),
        };
        info!("http query {}, change state to Running", &ctx.get_id());
        Executor::start_to_running(&executor, Running(running_state)).await;

        let executor_clone = executor.clone();
        let ctx_clone = ctx.clone();
        let block_sender_closer = block_sender.closer();

        let res = execute(interpreter, ctx_clone, block_sender, executor_clone.clone());
        match AssertUnwindSafe(res).catch_unwind().await {
            Ok(Err(err)) => {
                Executor::stop(&executor_clone, Err(err), false).await;
                block_sender_closer.close();
            }
            Err(e) => {
                Executor::stop(
                    &executor_clone,
                    Err(ErrorCode::PanicError(format!("interpreter panic: {e:?}"))),
                    false,
                )
                .await;
                block_sender_closer.close();
            }
            _ => {}
        }
        Ok(())
    }
}

async fn execute(
    interpreter: Arc<dyn Interpreter>,
    ctx: Arc<QueryContext>,
    block_sender: SizedChannelSender<DataBlock>,
    executor: Arc<RwLock<Executor>>,
) -> Result<()> {
    let mut data_stream = interpreter.execute(ctx.clone()).await?;

    match data_stream.next().await {
        None => {
            let block = DataBlock::empty_with_schema(interpreter.schema());
            block_sender.send(block, 0).await;
            Executor::stop(&executor, Ok(()), false).await;
            block_sender.close();
        }
        Some(Err(err)) => {
            Executor::stop(&executor, Err(err), false).await;
            block_sender.close();
        }
        Some(Ok(block)) => {
            let size = block.num_rows();
            block_sender.send(block, size).await;
            while let Some(block_r) = data_stream.next().await {
                match block_r {
                    Ok(block) => {
                        block_sender.send(block.clone(), block.num_rows()).await;
                    }
                    Err(err) => {
                        block_sender.close();
                        return Err(err);
                    }
                };
            }
            Executor::stop(&executor, Ok(()), false).await;
            block_sender.close();
        }
    }
    Ok(())
}
