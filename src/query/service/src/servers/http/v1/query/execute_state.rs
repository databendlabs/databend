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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use databend_common_base::base::tokio::sync::RwLock;
use databend_common_base::base::ProgressValues;
use databend_common_base::base::SpillProgress;
use databend_common_base::runtime::CatchUnwindFuture;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ResultExt;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Scalar;
use databend_common_io::prelude::FormatSettings;
use databend_common_settings::Settings;
use databend_storages_common_session::TempTblMgrRef;
use databend_storages_common_session::TxnManagerRef;
use futures::StreamExt;
use log::debug;
use log::error;
use log::info;
use serde::Deserialize;
use serde::Serialize;
use ExecuteState::*;

use crate::interpreters::interpreter_plan_sql;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::interpreters::InterpreterQueryLog;
use crate::servers::http::v1::http_query_handlers::QueryResponseField;
use crate::servers::http::v1::query::http_query::ResponseState;
use crate::servers::http::v1::query::sized_spsc::SizedChannelSender;
use crate::sessions::AcquireQueueGuard;
use crate::sessions::QueryAffect;
use crate::sessions::QueryContext;
use crate::sessions::Session;
use crate::sessions::TableContext;

pub struct ExecutionError;

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
pub enum ExecuteStateKind {
    Starting,
    Running,
    Failed,
    Succeeded,
}

impl ExecuteStateKind {
    pub fn is_stopped(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }
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
    pub spill_progress: SpillProgress,
}

impl Progresses {
    fn from_context(ctx: &Arc<QueryContext>) -> Self {
        Progresses {
            scan_progress: ctx.get_scan_progress_value(),
            write_progress: ctx.get_write_progress_value(),
            result_progress: ctx.get_result_progress_value(),
            total_scan: ctx.get_total_scan_value(),
            spill_progress: ctx.get_total_spill_progress(),
        }
    }
}

pub enum ExecuteState {
    Starting(ExecuteStarting),
    Running(ExecuteRunning),
    Stopped(Box<ExecuteStopped>),
}

impl ExecuteState {
    pub(crate) fn extract(&self) -> (ExecuteStateKind, Option<ErrorCode<ExecutionError>>) {
        match self {
            Starting(_) => (ExecuteStateKind::Starting, None),
            Running(_) => (ExecuteStateKind::Running, None),
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
    schema: Vec<QueryResponseField>,
    has_result_set: bool,
    #[allow(dead_code)]
    queue_guard: AcquireQueueGuard,
}

pub struct ExecuteStopped {
    pub schema: Vec<QueryResponseField>,
    pub has_result_set: Option<bool>,
    pub stats: Progresses,
    pub affect: Option<QueryAffect>,
    pub reason: Result<(), ExecutionError>,
    pub session_state: ExecutorSessionState,
    pub query_duration_ms: i64,
    pub warnings: Vec<String>,
}

pub struct Executor {
    pub query_id: String,
    pub state: ExecuteState,
}

// ExecutorSessionState is used to record the session state when the query is stopped.
// The HTTP Query API returns the session state to the client on each request. The client
// may store these new session state, and pass it to the next http query request.
#[derive(Debug, Clone)]
pub struct ExecutorSessionState {
    pub current_catalog: String,
    pub current_database: String,
    pub current_role: Option<String>,
    pub secondary_roles: Option<Vec<String>>,
    pub settings: Arc<Settings>,
    pub txn_manager: TxnManagerRef,
    pub temp_tbl_mgr: TempTblMgrRef,
    pub variables: HashMap<String, Scalar>,
}

impl ExecutorSessionState {
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            current_catalog: session.get_current_catalog(),
            current_database: session.get_current_database(),
            current_role: session.get_current_role().map(|r| r.name),
            secondary_roles: session.get_secondary_roles(),
            settings: session.get_settings(),
            txn_manager: session.txn_mgr(),
            temp_tbl_mgr: session.temp_tbl_mgr(),
            variables: session.get_all_variables(),
        }
    }
}

impl Executor {
    pub fn get_response_state(&self) -> ResponseState {
        let (exe_state, err) = self.state.extract();
        ResponseState {
            running_time_ms: self.get_query_duration_ms(),
            progresses: self.get_progress(),
            state: exe_state,
            error: err,
            warnings: self.get_warnings(),
            affect: self.get_affect(),
            schema: self.get_schema(),
            has_result_set: self.has_result_set(),
        }
    }
    pub fn get_schema(&self) -> Vec<QueryResponseField> {
        match &self.state {
            Starting(_) => Default::default(),
            Running(r) => r.schema.clone(),
            Stopped(f) => f.schema.clone(),
        }
    }

    pub fn has_result_set(&self) -> Option<bool> {
        match &self.state {
            Starting(_) => None,
            Running(r) => Some(r.has_result_set),
            Stopped(f) => f.has_result_set,
        }
    }

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

    pub fn get_warnings(&self) -> Vec<String> {
        match &self.state {
            Starting(_) => vec![],
            Running(r) => r.ctx.pop_warnings(),
            Stopped(r) => r.warnings.clone(),
        }
    }

    pub fn get_session_state(&self) -> ExecutorSessionState {
        match &self.state {
            Starting(r) => ExecutorSessionState::new(r.ctx.get_current_session()),
            Running(r) => ExecutorSessionState::new(r.ctx.get_current_session()),
            Stopped(r) => r.session_state.clone(),
        }
    }

    pub fn get_query_duration_ms(&self) -> i64 {
        match &self.state {
            Starting(ExecuteStarting { ctx }) | Running(ExecuteRunning { ctx, .. }) => {
                ctx.get_query_duration_ms()
            }
            Stopped(f) => f.query_duration_ms,
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
    pub async fn stop<C>(this: &Arc<RwLock<Executor>>, reason: Result<(), C>) {
        let reason = reason.with_context(|| "execution stopped");
        let mut guard = this.write().await;

        let state = match &guard.state {
            Starting(s) => {
                info!(
                    "{}: http query begin changing state from Staring to Stopped, reason {:?}",
                    &guard.query_id, reason
                );
                if let Err(e) = &reason {
                    InterpreterQueryLog::log_finish(
                        &s.ctx,
                        SystemTime::now(),
                        Some(e.clone()),
                        false,
                    )
                    .unwrap_or_else(|e| error!("fail to write query_log {:?}", e));
                }
                if let Err(e) = &reason {
                    if e.code() != ErrorCode::CLOSED_QUERY {
                        s.ctx.get_current_session().txn_mgr().lock().set_fail();
                    }
                }
                ExecuteStopped {
                    stats: Default::default(),
                    schema: vec![],
                    has_result_set: None,
                    reason: reason.clone(),
                    session_state: ExecutorSessionState::new(s.ctx.get_current_session()),
                    query_duration_ms: s.ctx.get_query_duration_ms(),
                    warnings: s.ctx.pop_warnings(),
                    affect: Default::default(),
                }
            }
            Running(r) => {
                info!(
                    "{}: http query changing state from Running to Stopped, reason {:?}",
                    &guard.query_id, reason,
                );
                if let Err(e) = &reason {
                    if e.code() != ErrorCode::CLOSED_QUERY {
                        r.session.txn_mgr().lock().set_fail();
                    }
                    r.session.force_kill_query(e.clone());
                }
                ExecuteStopped {
                    stats: Progresses::from_context(&r.ctx),
                    schema: r.schema.clone(),
                    has_result_set: Some(r.has_result_set),
                    reason: reason.clone(),
                    session_state: ExecutorSessionState::new(r.ctx.get_current_session()),
                    query_duration_ms: r.ctx.get_query_duration_ms(),
                    warnings: r.ctx.pop_warnings(),
                    affect: r.ctx.get_affect(),
                }
            }
            Stopped(s) => {
                debug!(
                    "{}: http query already stopped, reason {:?}, new reason {:?}",
                    &guard.query_id, s.reason, reason
                );
                return;
            }
        };
        info!(
            "{}: http query has change state to Stopped, reason {:?}",
            &guard.query_id, reason
        );
        guard.state = Stopped(Box::new(state));
    }
}

impl ExecuteState {
    #[async_backtrace::framed]
    pub(crate) async fn try_start_query(
        executor: Arc<RwLock<Executor>>,
        sql: String,
        session: Arc<Session>,
        ctx: Arc<QueryContext>,
        block_sender: SizedChannelSender<DataBlock>,
        format_settings: Arc<parking_lot::RwLock<Option<FormatSettings>>>,
    ) -> Result<(), ExecutionError> {
        let make_error = || format!("failed to start query: {sql}");

        info!("http query prepare to plan sql");

        // Use interpreter_plan_sql, we can write the query log if an error occurs.
        let (plan, _, queue_guard) = interpreter_plan_sql(ctx.clone(), &sql, true)
            .await
            .map_err(|err| err.display_with_sql(&sql))
            .with_context(make_error)?;
        {
            // set_var may change settings
            let mut guard = format_settings.write();
            *guard = Some(ctx.get_format_settings().with_context(make_error)?);
        }

        let interpreter = InterpreterFactory::get(ctx.clone(), &plan)
            .await
            .with_context(make_error)?;
        let has_result_set = plan.has_result_set();
        let schema = if has_result_set {
            // check has_result_set first for safety
            QueryResponseField::from_schema(plan.schema())
        } else {
            vec![]
        };
        let running_state = ExecuteRunning {
            session,
            ctx: ctx.clone(),
            queue_guard,
            schema,
            has_result_set,
        };
        info!("http query change state to Running");
        Executor::start_to_running(&executor, Running(running_state)).await;

        let executor_clone = executor.clone();
        let ctx_clone = ctx.clone();
        let block_sender_closer = block_sender.closer();

        let res = execute(
            interpreter,
            plan.schema(),
            ctx_clone,
            block_sender,
            executor_clone.clone(),
        );
        match CatchUnwindFuture::create(res).await {
            Ok(Err(err)) => {
                Executor::stop(&executor_clone, Err(err.clone())).await;
                block_sender_closer.close();
            }
            Err(e) => {
                Executor::stop(&executor_clone, Err(e)).await;
                block_sender_closer.close();
            }
            _ => {}
        }

        Ok(())
    }
}

async fn execute(
    interpreter: Arc<dyn Interpreter>,
    schema: DataSchemaRef,
    ctx: Arc<QueryContext>,
    block_sender: SizedChannelSender<DataBlock>,
    executor: Arc<RwLock<Executor>>,
) -> Result<(), ExecutionError> {
    let make_error = || format!("failed to execute {}", interpreter.name());

    let mut data_stream = interpreter
        .execute(ctx.clone())
        .await
        .with_context(make_error)?;
    match data_stream.next().await {
        None => {
            let block = DataBlock::empty_with_schema(schema);
            block_sender.send(block, 0).await;
            Executor::stop::<()>(&executor, Ok(())).await;
            block_sender.close();
        }
        Some(Err(err)) => {
            Executor::stop(&executor, Err(err)).await;
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
                        return Err(err.with_context(make_error()));
                    }
                };
            }
            Executor::stop::<()>(&executor, Ok(())).await;
            block_sender.close();
        }
    }
    Ok(())
}
