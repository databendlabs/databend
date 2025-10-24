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

use databend_common_base::base::ProgressValues;
use databend_common_base::base::SpillProgress;
use databend_common_base::runtime::CatchUnwindFuture;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ResultExt;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Scalar;
use databend_common_settings::Settings;
use databend_common_storage::DataOperator;
use databend_storages_common_cache::TempDirManager;
use databend_storages_common_session::TxnManagerRef;
use futures::StreamExt;
use log::debug;
use log::error;
use log::info;
use parking_lot::Mutex;
use serde::Deserialize;
use serde::Serialize;
use ExecuteState::*;

use super::http_query::ResponseState;
use super::sized_spsc::SizedChannelSender;
use crate::interpreters::interpreter_plan_sql;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::interpreters::InterpreterQueryLog;
use crate::sessions::AcquireQueueGuard;
use crate::sessions::QueryAffect;
use crate::sessions::QueryContext;
use crate::sessions::Session;
use crate::sessions::TableContext;
use crate::spillers::LiteSpiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerDiskConfig;
use crate::spillers::SpillerType;

type Sender = SizedChannelSender<LiteSpiller>;

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
    pub(crate) sender: Option<Sender>,
}

pub struct ExecuteRunning {
    // used to kill query
    session: Arc<Session>,
    // mainly used to get progress for now
    pub(crate) ctx: Arc<QueryContext>,
    schema: DataSchemaRef,
    has_result_set: bool,
    #[allow(dead_code)]
    queue_guard: AcquireQueueGuard,
}

pub struct ExecuteStopped {
    pub schema: DataSchemaRef,
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
    pub variables: HashMap<String, Scalar>,
    pub last_query_result_cache_key: String,
}

impl ExecutorSessionState {
    pub fn new(session: Arc<Session>) -> Self {
        let mut last_query_result_cache_key = String::new();

        if let Some(last_query_id) = session.get_current_query_id() {
            if let Some(meta_key) = session.get_query_result_cache_key(&last_query_id) {
                last_query_result_cache_key = meta_key;
            }
        }

        Self {
            current_catalog: session.get_current_catalog(),
            current_database: session.get_current_database(),
            current_role: session.get_current_role().map(|r| r.name),
            secondary_roles: session.get_secondary_roles(),
            settings: session.get_settings(),
            txn_manager: session.txn_mgr(),
            variables: session.get_all_variables(),
            last_query_result_cache_key,
        }
    }
}

impl Executor {
    pub fn get_response_state(&self) -> ResponseState {
        let (exe_state, err) = self.state.extract();
        let schema = match &self.state {
            Starting(_) => Default::default(),
            Running(r) => r.schema.clone(),
            Stopped(f) => f.schema.clone(),
        };

        ResponseState {
            running_time_ms: self.get_query_duration_ms(),
            progresses: self.get_progress(),
            state: exe_state,
            error: err,
            warnings: self.get_warnings(),
            affect: self.get_affect(),
            schema,
            has_result_set: self.has_result_set(),
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

    pub fn update_schema(this: &Arc<Mutex<Executor>>, schema: DataSchemaRef) {
        match &mut this.lock().state {
            Starting(_) => {}
            Running(r) => r.schema = schema,
            Stopped(f) => f.schema = schema,
        }
    }

    pub fn get_query_duration_ms(&self) -> i64 {
        match &self.state {
            Starting(ExecuteStarting { ctx, .. }) | Running(ExecuteRunning { ctx, .. }) => {
                ctx.get_query_duration_ms()
            }
            Stopped(f) => f.query_duration_ms,
        }
    }

    pub fn start_to_running(this: &Arc<Mutex<Executor>>, state: ExecuteState) {
        let mut guard = this.lock();
        if let Starting(_) = &guard.state {
            guard.state = state
        }
    }

    pub fn start_to_stop(this: &Arc<Mutex<Executor>>, state: ExecuteState) {
        let mut guard = this.lock();
        if let Starting(_) = &guard.state {
            guard.state = state
        }
    }

    pub fn stop<C>(this: &Arc<Mutex<Executor>>, reason: Result<(), C>) {
        let reason = reason.with_context(|| "execution stopped");
        let mut guard = this.lock();

        let state = match &guard.state {
            Starting(s) => {
                info!(
                    query_id = guard.query_id, reason:? = reason;
                    "[HTTP-QUERY] Query state transitioning from Starting to Stopped"
                );

                s.ctx.get_abort_notify().notify_waiters();
                if let Err(e) = &reason {
                    InterpreterQueryLog::log_finish(
                        &s.ctx,
                        SystemTime::now(),
                        Some(e.clone()),
                        false,
                    )
                    .unwrap_or_else(|e| error!("[HTTP-QUERY] Failed to write query_log: {:?}", e));
                }
                if let Err(e) = &reason {
                    if e.code() != ErrorCode::CLOSED_QUERY {
                        s.ctx.get_current_session().txn_mgr().lock().set_fail();
                    }
                }
                ExecuteStopped {
                    stats: Default::default(),
                    schema: Default::default(),
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
                    "[HTTP-QUERY] Query {} state transitioning from Running to Stopped, reason: {:?}",
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
                    "[HTTP-QUERY] Query {} already in Stopped state, original reason: {:?}, new reason: {:?}",
                    &guard.query_id, s.reason, reason
                );
                return;
            }
        };
        info!(
            "[HTTP-QUERY] Query {} state changed to Stopped, reason: {:?}",
            &guard.query_id, reason
        );
        guard.state = Stopped(Box::new(state));
    }
}

impl ExecuteState {
    #[async_backtrace::framed]
    #[fastrace::trace(name = "ExecuteState::try_start_query")]
    pub(crate) async fn try_start_query(
        executor: Arc<Mutex<Executor>>,
        sql: String,
        session: Arc<Session>,
        ctx: Arc<QueryContext>,
        mut block_sender: Sender,
    ) -> Result<(), ExecutionError> {
        let make_error = || format!("failed to start query: {sql}");

        info!("[HTTP-QUERY] Preparing to plan SQL query");

        // Use interpreter_plan_sql, we can write the query log if an error occurs.
        let (plan, _, queue_guard) = interpreter_plan_sql(ctx.clone(), &sql, true)
            .await
            .map_err(|err| err.display_with_sql(&sql))
            .with_context(make_error)?;

        Self::apply_settings(&ctx, &mut block_sender).with_context(make_error)?;

        let interpreter = InterpreterFactory::get(ctx.clone(), &plan)
            .await
            .with_context(make_error)?;
        let has_result_set = plan.has_result_set();
        // For dynamic schema, we just return empty schema and update it later.
        let is_dynamic_schema = plan.is_dynamic_schema();
        let schema = if has_result_set && !is_dynamic_schema {
            // check has_result_set first for safety
            plan.schema()
        } else {
            Default::default()
        };

        let running_state = ExecuteRunning {
            session,
            ctx: ctx.clone(),
            queue_guard,
            schema,
            has_result_set,
        };
        info!("[HTTP-QUERY] Query state changed to Running");
        Executor::start_to_running(&executor, Running(running_state));

        let executor_clone = executor.clone();
        let ctx_clone = ctx.clone();
        let block_sender_closer = block_sender.closer();

        let res = Self::pull_and_send(
            interpreter,
            is_dynamic_schema,
            plan.schema(),
            ctx_clone,
            block_sender,
            executor_clone.clone(),
        );
        match CatchUnwindFuture::create(res).await {
            Ok(Err(err)) => {
                Executor::stop(&executor_clone, Err(err.clone()));
                block_sender_closer.abort();
            }
            Err(e) => {
                Executor::stop(&executor_clone, Err(e));
                block_sender_closer.abort();
            }
            _ => {}
        }

        Ok(())
    }

    #[fastrace::trace(name = "ExecuteState::pull_and_send")]
    async fn pull_and_send(
        interpreter: Arc<dyn Interpreter>,
        is_dynamic_schema: bool,
        schema: DataSchemaRef,
        ctx: Arc<QueryContext>,
        mut sender: Sender,
        executor: Arc<Mutex<Executor>>,
    ) -> Result<(), ExecutionError> {
        let make_error = || format!("failed to execute {}", interpreter.name());

        let mut data_stream = interpreter
            .execute(ctx.clone())
            .await
            .with_context(make_error)?;
        match data_stream.next().await {
            None => {
                Self::send_data_block(&mut sender, &executor, DataBlock::empty_with_schema(schema))
                    .await
                    .with_context(make_error)?;
                Executor::stop::<()>(&executor, Ok(()));
                sender.finish();
            }
            Some(Err(err)) => {
                Executor::stop(&executor, Err(err));
                sender.abort();
            }
            Some(Ok(block)) => {
                if is_dynamic_schema {
                    if let Some(schema) = interpreter.get_dynamic_schema().await {
                        info!(
                            "[HTTP-QUERY] Dynamic schema detected, updating schema to have {} fields",
                            schema.fields().len()
                        );
                        Executor::update_schema(&executor, schema);
                    }
                }
                Self::send_data_block(&mut sender, &executor, block)
                    .await
                    .with_context(make_error)?;
                while let Some(block) = data_stream.next().await {
                    match block {
                        Ok(block) => {
                            Self::send_data_block(&mut sender, &executor, block)
                                .await
                                .with_context(make_error)?;
                        }
                        Err(err) => {
                            sender.abort();
                            return Err(err.with_context(make_error()));
                        }
                    };
                }
                Executor::stop::<()>(&executor, Ok(()));
                sender.finish();
            }
        }
        Ok(())
    }

    async fn send_data_block(
        sender: &mut Sender,
        executor: &Arc<Mutex<Executor>>,
        block: DataBlock,
    ) -> Result<bool> {
        match sender.send(block).await {
            Ok(ok) => Ok(ok),
            Err(err) => {
                Executor::stop(executor, Err(err.clone()));
                sender.abort();
                Err(err)
            }
        }
    }

    fn apply_settings(ctx: &Arc<QueryContext>, block_sender: &mut Sender) -> Result<()> {
        let settings = ctx.get_settings();

        let spiller = if settings.get_enable_result_set_spilling()? {
            let temp_dir_manager = TempDirManager::instance();
            let disk_bytes_limit = settings.get_result_set_spilling_to_disk_bytes_limit()?;
            let enable_dio = settings.get_enable_dio()?;
            let disk_spill = temp_dir_manager
                .get_disk_spill_dir(disk_bytes_limit, &ctx.get_id())
                .map(|temp_dir| SpillerDiskConfig::new(temp_dir, enable_dio))
                .transpose()?;

            let location_prefix = ctx.query_id_spill_prefix();
            let config = SpillerConfig {
                spiller_type: SpillerType::ResultSet,
                location_prefix,
                disk_spill,
                use_parquet: settings.get_spilling_file_format()?.is_parquet(),
            };
            let op = DataOperator::instance().spill_operator();
            Some(LiteSpiller::new(op, config)?)
        } else {
            None
        };

        // set_var may change settings
        let format_settings = ctx.get_format_settings()?;
        block_sender.plan_ready(format_settings, spiller);
        Ok(())
    }
}
