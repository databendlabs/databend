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

// Logs from this module will show up as "[PRIVATE-TASKS] ...".
databend_common_tracing::register_module_tag!("[PRIVATE-TASKS]");

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Write;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_stream::stream;
use chrono::Utc;
use chrono_tz::Tz;
use cron::Schedule;
use databend_common_ast::ast::AlterTaskOptions;
use databend_common_ast::parser::ParseMode;
use databend_common_ast::parser::run_parser;
use databend_common_ast::parser::script::script_block;
use databend_common_ast::parser::tokenize_sql;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::Runtime;
use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_management::task::TaskError;
use databend_common_management::task::TaskMgr;
use databend_common_meta_api::kv_pb_api::decode_seqv;
use databend_common_meta_app::principal::ScheduleType;
use databend_common_meta_app::principal::State;
use databend_common_meta_app::principal::Status;
use databend_common_meta_app::principal::Task;
use databend_common_meta_app::principal::TaskRun;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::WarehouseOptions;
use databend_common_meta_app::principal::task::EMPTY_TASK_ID;
use databend_common_meta_app::principal::task::TaskMessage;
use databend_common_meta_app::principal::task::TaskMessageType;
use databend_common_meta_app::principal::task::TaskSql;
use databend_common_meta_app::principal::task_message_ident::TaskMessageIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_script::Executor;
use databend_common_script::compile_block;
use databend_common_sql::Planner;
use databend_common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;
use databend_common_users::UserApiProvider;
use databend_common_version::BUILD_INFO;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::protobuf::WatchRequest;
use databend_meta_client::types::protobuf::WatchResponse;
use futures::Stream;
use futures_util::TryStreamExt;
use futures_util::stream::BoxStream;
use itertools::Itertools;
use log::error;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::clusters::ClusterDiscovery;
use crate::interpreters::InterpreterFactory;
use crate::interpreters::common::QueryFinishHooks;
use crate::interpreters::util::ScriptClient;
use crate::meta_client_error;
use crate::meta_service_error;
use crate::schedulers::ServiceQueryExecutor;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextSettings;
use crate::task::meta::TaskMetaHandle;
use crate::task::session::create_session;
use crate::task::session::get_task_user;
use crate::task::subscription::Subscription;

pub type TaskMessageStream = BoxStream<'static, Result<(String, SeqV<TaskMessage>)>>;

const OVERLAPPING_EXECUTION: &str = "OVERLAPPING_EXECUTION";
const WHEN_CONDITION_EVALUATION_ERROR: &str = "WHEN_CONDITION_EVALUATION_ERROR";
const WHEN_CONDITION_FALSE: &str = "WHEN_CONDITION_FALSE";

/// The delays before watching the task messages again: from the min, doubled up to the max.
const WATCH_AGAIN_MIN_DELAY: Duration = Duration::from_secs(1);
const WATCH_AGAIN_MAX_DELAY: Duration = Duration::from_secs(30);

/// The delay before a schedule that failed starts again.
const SCHEDULE_RESTART_DELAY: Duration = Duration::from_secs(1);

/// A schedule that this node runs for a task.
struct ScheduledTask {
    task_id: u64,
    /// The seq of the `ScheduleTask` message that started this schedule.
    message_seq: u64,
    /// Cancelled to stop this schedule.
    token: CancellationToken,
    handle: JoinHandle<()>,
}

impl ScheduledTask {
    /// Whether the `ScheduleTask` message of `message_seq` started this schedule, and it still
    /// runs.
    fn runs_message(&self, message_seq: u64) -> bool {
        self.message_seq == message_seq && !self.handle.is_finished()
    }
}

/// Currently, query uses the watch in meta to imitate channel to obtain tasks. When task messages are sent to channels, they are stored in meta using TaskMessage::key.
/// TaskMessage::key is divided into only 4 types of keys that will overwrite each other to avoid repeated storage and repeated processing.
/// Whenever a new key is inserted for overwriting, each query will receive the corresponding key change and process it, thus realizing the channel
/// The init type key of watch is used to let the Service load the Schedule, and TaskService will delete the corresponding key (TaskMgr::accept) when processing Execute & After & Delete TaskMessage to avoid repeated processing
pub struct TaskService {
    initialized: AtomicBool,
    tenant: Tenant,
    meta_handle: TaskMetaHandle,
    _runtime: Arc<Runtime>,
}

impl TaskService {
    pub fn instance() -> Arc<TaskService> {
        GlobalInstance::get()
    }

    pub async fn prepare(&self) -> Result<()> {
        let prepare_key = format!("{}/task_run_prepare/lock", self.tenant.tenant_name());
        let _guard = self.meta_handle.acquire(&prepare_key, 0).await?;
        let create_db = "CREATE DATABASE IF NOT EXISTS system_task";
        self.execute_sql(None, create_db).await?;

        let create_task_run_table = "CREATE TABLE IF NOT EXISTS system_task.task_run(\
        task_id UINT64,\
        task_name TEXT NOT NULL,\
        query_text TEXT NOT NULL,\
        when_condition TEXT,
        after TEXT,\
        comment TEXT,\
        owner TEXT,
        owner_user TEXT,\
        warehouse_name TEXT,\
        using_warehouse_size TEXT,\
        schedule_type INTEGER,\
        interval INTEGER,\
        interval_secs INTEGER,\
        interval_milliseconds UINT64,\
        cron TEXT,\
        time_zone TEXT DEFAULT 'UTC',\
        run_id UINT64,\
        attempt_number INTEGER,\
        state TEXT NOT NULL DEFAULT 'SCHEDULED',\
        error_code BIGINT,\
        error_message TEXT,
        root_task_id UINT64,\
        scheduled_at TIMESTAMP DEFAULT NOW(),\
        completed_at TIMESTAMP,\
        next_scheduled_at TIMESTAMP DEFAULT NOW(),\
        error_integration TEXT,\
        status TEXT,\
        created_at TIMESTAMP,\
        updated_at TIMESTAMP,\
        session_params VARIANT,\
        last_suspended_at TIMESTAMP,\
        suspend_task_after_num_failures INTEGER\
        );";
        self.execute_sql(None, create_task_run_table).await?;

        let create_task_after_table = "CREATE TABLE IF NOT EXISTS system_task.task_after(\
        task_name STRING NOT NULL,\
        next_task STRING NOT NULL\
        );";
        self.execute_sql(None, create_task_after_table).await?;

        Ok(())
    }

    pub fn initialized(&self) {
        self.initialized.store(true, Ordering::SeqCst);
    }

    pub async fn init(cfg: &InnerConfig) -> Result<()> {
        let tenant = cfg.query.tenant_id.clone();
        let meta_store = MetaStoreProvider::new(cfg.meta.to_meta_grpc_client_conf())
            .create_default_meta_store()
            .await
            .map_err(|e| {
                ErrorCode::MetaServiceError(format!("Failed to create meta store: {}", e))
            })?;
        let meta_client = meta_store.inner().clone();
        let meta_handle = TaskMetaHandle::new(meta_client, cfg.query.node_id.clone());
        let runtime = Arc::new(Runtime::with_worker_threads(
            4,
            Some("task-worker".to_owned()),
        )?);

        let instance = TaskService {
            initialized: AtomicBool::new(false),
            tenant: cfg.query.tenant_id.clone(),
            meta_handle,
            _runtime: runtime.clone(),
        };
        GlobalInstance::set(Arc::new(instance));

        runtime.clone().spawn(async move {
            let task_service = TaskService::instance();
            loop {
                if !task_service.initialized.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                } else {
                    break;
                }
            }
            if let Err(err) = task_service.prepare().await {
                error!("prepare failed due to {}", err);
            }
            task_service.work(&tenant, runtime).await;
        });
        Ok(())
    }

    /// Handle the task messages as they come, for as long as the process runs.
    ///
    /// The watch of the task messages ends when the connection to the meta-service breaks, and
    /// when the meta-service leader changes: watch again, and keep the schedules this node runs.
    /// An error while handling one message is logged, and the next message is handled.
    async fn work(&self, tenant: &Tenant, runtime: Arc<Runtime>) {
        let mut scheduled_tasks: HashMap<String, ScheduledTask> = HashMap::new();
        let task_mgr = UserApiProvider::instance().task_api(tenant);

        let mut messages = Subscription::new(
            || self.subscribe(),
            WATCH_AGAIN_MIN_DELAY,
            WATCH_AGAIN_MAX_DELAY,
        );
        loop {
            let (key, message) = messages.next().await;
            if let Err(err) = self
                .handle_message(tenant, &runtime, &task_mgr, &mut scheduled_tasks, message)
                .await
            {
                error!("failed to handle task message {}: {}", key, err);
            }
        }
    }

    /// Handle one task message received from the watch stream.
    async fn handle_message(
        &self,
        tenant: &Tenant,
        runtime: &Arc<Runtime>,
        task_mgr: &TaskMgr,
        scheduled_tasks: &mut HashMap<String, ScheduledTask>,
        message: SeqV<TaskMessage>,
    ) -> Result<()> {
        let message_seq = message.seq;
        let task_message = message.data;
        let task_key = TaskMessageIdent::new(tenant, task_message.key());

        let is_delete = matches!(&task_message, TaskMessage::DeleteTask(_, _, _));
        if !is_delete {
            if let Some(WarehouseOptions {
                warehouse: Some(warehouse),
                ..
            }) = task_message.warehouse_options()
            {
                let current_warehouse = ClusterDiscovery::instance()
                    .single_node_cluster(&GlobalConfig::instance())
                    .await?
                    .get_warehouse_id()?;
                if warehouse != &current_warehouse {
                    return Ok(());
                }
            }
        }
        match task_message {
            // ScheduleTask is always monitored by all Query nodes, and ExecuteTask is sent serially to avoid repeated sending.
            TaskMessage::ScheduleTask(mut task) => {
                debug_assert!(task.schedule_options.is_some());
                if let Some(schedule_options) = &task.schedule_options {
                    // A ScheduleTask message stays until its task is dropped, so a new watch
                    // brings it again: keep the schedule it started, unless that has stopped.
                    if scheduled_tasks
                        .get(&task.task_name)
                        .is_some_and(|scheduled| scheduled.runs_message(message_seq))
                    {
                        return Ok(());
                    }
                    // clean old task if alter
                    if let Some(scheduled) = scheduled_tasks.remove(&task.task_name) {
                        scheduled.token.cancel();
                    }
                    match task.status {
                        Status::Suspended => return Ok(()),
                        Status::Started => (),
                    }

                    let token = CancellationToken::new();
                    let child_token = token.child_token();
                    let task_id = task.task_id;
                    let task_name = task.task_name.to_string();
                    let task_name_clone = task_name.clone();
                    let task_service = TaskService::instance();
                    let owner = Self::get_task_owner(&task, tenant).await?;

                    let fn_lock = async |task_service: &TaskService,
                                         key: &TaskMessageIdent,
                                         interval_millis: u64| {
                        task_service
                            .meta_handle
                            .acquire_with_guard(&format!("{}/lock", key), interval_millis)
                            .await
                    };

                    let handle = match schedule_options.schedule_type {
                        ScheduleType::IntervalType => {
                            let task_mgr = task_mgr.clone();
                            let mut duration =
                                Duration::from_secs(schedule_options.interval.unwrap() as u64);
                            if let Some(ms) = &schedule_options.milliseconds_interval {
                                duration += Duration::from_millis(*ms);
                            }

                            let name = format!("interval schedule of task {}", task_name);
                            let cancel = child_token.clone();
                            runtime
                                .spawn(async move {
                                    let fn_work = async move || {
                                        let committed_at = Utc::now();
                                        task.next_scheduled_at = Some(committed_at + duration);
                                        task.updated_at = committed_at;
                                        match Self::update_schedule(&task_mgr, &task).await {
                                            Ok(true) => {}
                                            Ok(false) => return Ok(()),
                                            // Stored again by the first tick.
                                            Err(err) => error!(
                                                "failed to store the next run of task {}: {}",
                                                task.task_name, err
                                            ),
                                        }
                                        loop {
                                            tokio::select! {
                                                _ = sleep(duration) => {
                                                    let Some(_guard) = fn_lock(&task_service, &task_key, duration.as_millis() as u64).await? else {
                                                        continue;
                                                    };
                                                    let committed_at = Utc::now();
                                                    task.next_scheduled_at = Some(committed_at + duration);
                                                    task.updated_at = committed_at;
                                                    if !Self::update_schedule(&task_mgr, &task).await? {
                                                        break;
                                                    }
                                                    if task_service.has_executing_task_run(&task.task_name).await? {
                                                        task_service.record_overlapping_skip(&task).await?;
                                                        continue;
                                                    }
                                                    if !Self::check_when_and_record(&task, &owner, &task_service).await? {
                                                        continue;
                                                    }
                                                    task_mgr.send(TaskMessage::ExecuteTask(task.clone())).await.map_err(meta_service_error)?;
                                                }
                                                _ = child_token.cancelled() => {
                                                    break;
                                                }
                                            }
                                        }
                                        Result::Ok(())
                                    };
                                    keep_running(&name, SCHEDULE_RESTART_DELAY, cancel, fn_work)
                                        .await;
                                })
                        }
                        ScheduleType::CronType => {
                            let task_mgr = task_mgr.clone();
                            // SAFETY: check on CreateTask
                            let cron_expr = schedule_options.cron.as_ref().unwrap();
                            let tz = schedule_options
                                .time_zone
                                .as_ref()
                                .map(|tz| {
                                    tz.parse::<Tz>().map_err(|error| {
                                        ErrorCode::InvalidTimezone(format!(
                                            "Invalid task timezone {tz}: {error}"
                                        ))
                                    })
                                })
                                .transpose()?
                                .unwrap_or(Tz::UCT);
                            let schedule = Schedule::from_str(cron_expr).unwrap();

                            let name = format!("cron schedule of task {}", task_name);
                            let cancel = child_token.clone();
                            runtime
                                .spawn(async move {
                                    let fn_work = async move || {
                                        let mut upcoming = schedule.upcoming(tz).peekable();

                                        while let Some(next_time) = upcoming.next() {
                                            let now = Utc::now();
                                            let duration = (next_time.with_timezone(&Utc) - now)
                                                .to_std()
                                                .unwrap_or(Duration::ZERO);

                                            task.next_scheduled_at = Some(next_time.with_timezone(&Utc));
                                            task.updated_at = now;
                                            match Self::update_schedule(&task_mgr, &task).await {
                                                Ok(true) => {}
                                                Ok(false) => break,
                                                // Stored again by the tick.
                                                Err(err) => error!(
                                                    "failed to store the next run of task {}: {}",
                                                    task.task_name, err
                                                ),
                                            }
                                            tokio::select! {
                                                _ = sleep(duration) => {
                                                    let Some(_guard) = fn_lock(&task_service, &task_key, duration.as_millis() as u64).await? else {
                                                        continue;
                                                    };
                                                    let committed_at = Utc::now();
                                                    let next_scheduled_at = upcoming
                                                        .peek()
                                                        .map(|next| next.with_timezone(&Utc));
                                                    task.next_scheduled_at = next_scheduled_at;
                                                    task.updated_at = committed_at;
                                                    if !Self::update_schedule(&task_mgr, &task).await? {
                                                        break;
                                                    }
                                                    if task_service.has_executing_task_run(&task.task_name).await? {
                                                        task_service.record_overlapping_skip(&task).await?;
                                                        continue;
                                                    }
                                                    if !Self::check_when_and_record(&task, &owner, &task_service).await? {
                                                        continue;
                                                    }
                                                    task_mgr.send(TaskMessage::ExecuteTask(task.clone())).await.map_err(meta_service_error)?;
                                                }
                                                _ = child_token.cancelled() => {
                                                    break;
                                                }
                                            }
                                        }
                                        Result::Ok(())
                                    };
                                    keep_running(&name, SCHEDULE_RESTART_DELAY, cancel, fn_work)
                                        .await;
                                })
                        }
                    };
                    let _ = scheduled_tasks.insert(task_name_clone, ScheduledTask {
                        task_id,
                        message_seq,
                        token,
                        handle,
                    });
                }
            }
            TaskMessage::ExecuteTask(task) => {
                if !task_mgr
                    .accept(&task_key)
                    .await
                    .map_err(meta_service_error)?
                {
                    return Ok(());
                }
                let task_name = task.task_name.clone();
                let task_service = TaskService::instance();
                let Some(execute_guard) = task_service
                    .meta_handle
                    .acquire_with_guard(
                        &format!(
                            "{}/execute/lock",
                            TaskMessage::key_with_type(TaskMessageType::Execute, &task_name)
                        ),
                        0,
                    )
                    .await?
                else {
                    return Ok(());
                };

                if task_service.has_executing_task_run(&task_name).await? {
                    task_service.record_overlapping_skip(&task).await?;
                    return Ok(());
                }
                let mut task_run = Self::new_task_run(&task);
                task_service.update_or_create_task_run(&task_run).await?;
                drop(execute_guard);

                let task_mgr = task_mgr.clone();
                let tenant = tenant.clone();
                let owner = Self::get_task_owner(&task, &tenant).await?;

                runtime.spawn(async move {
                    let mut fn_work = async move || {
                        while task_run.attempt_number >= 0 {
                            let task_result = Self::spawn_task(task.clone(), owner.clone()).await;

                            match task_result {
                                Ok(()) => {
                                    task_run.state = State::Succeeded;
                                    task_run.completed_at = Some(Utc::now());
                                    task_service.update_or_create_task_run(&task_run).await?;

                                    let mut stream =
                                        Box::pin(task_service.check_next_tasks(&task_name));

                                    while let Some(next_task) = stream.next().await {
                                        let next_task = next_task?;
                                        let next_task = task_mgr
                                            .describe_task(&next_task)
                                            .await??
                                            .ok_or_else(|| ErrorCode::UnknownTask(next_task))?;
                                        let next_owner =
                                            Self::get_task_owner(&next_task, &tenant).await?;
                                        if task_service
                                            .has_executing_task_run(&next_task.task_name)
                                            .await?
                                        {
                                            task_service
                                                .record_overlapping_skip(&next_task)
                                                .await?;
                                            continue;
                                        }
                                        if Self::check_when_and_record(
                                            &next_task,
                                            &next_owner,
                                            &task_service,
                                        )
                                        .await?
                                        {
                                            task_mgr
                                                .send(TaskMessage::ExecuteTask(next_task))
                                                .await
                                                .map_err(meta_service_error)?;
                                        }
                                    }
                                    break;
                                }
                                Err(err) => {
                                    task_run.state = State::Failed;
                                    task_run.completed_at = Some(Utc::now());
                                    task_run.attempt_number -= 1;
                                    task_run.error_code = err.code() as i64;
                                    task_run.error_message = Some(err.message());
                                    task_service.update_or_create_task_run(&task_run).await?;
                                    if task_run.attempt_number <= 0 {
                                        task_mgr
                                            .alter_task(&task.task_name, &AlterTaskOptions::Suspend)
                                            .await??;
                                        break;
                                    } else {
                                        task_run.run_id = Self::make_run_id();
                                        task_run.state = State::Executing;
                                        task_run.scheduled_at = Utc::now();
                                        task_run.completed_at = None;
                                        task_run.error_code = 0;
                                        task_run.error_message = None;
                                        task_service.update_or_create_task_run(&task_run).await?;
                                    }
                                }
                            }
                        }

                        Result::Ok(())
                    };
                    if let Err(err) = fn_work().await {
                        error!("execute failed due to {}", err);
                    }
                });
            }
            TaskMessage::DeleteTask(task_name, _, task_id) => {
                // Every node may still hold a local schedule for this task. Cancel it before
                // an unassigned node skips the shared metadata cleanup below.
                if task_id.is_none_or(|task_id| {
                    scheduled_tasks
                        .get(&task_name)
                        .is_some_and(|scheduled| scheduled.task_id == task_id)
                }) {
                    if let Some(scheduled) = scheduled_tasks.remove(&task_name) {
                        scheduled.token.cancel();
                    }
                }
                if self.create_context(None).await?.get_cluster().unassign {
                    return Ok(());
                }
                if task_mgr
                    .accept(&task_key)
                    .await
                    .map_err(meta_service_error)?
                {
                    self.clean_task_afters(&task_name).await?;
                    if let Some(task_id) = task_id {
                        self.cancel_open_task_runs(
                            &task_name,
                            task_id,
                            "task was dropped while execution status was still open",
                        )
                        .await?;
                    }
                }
                task_mgr
                    .accept(&TaskMessageIdent::new(
                        tenant,
                        TaskMessage::key_with_type(TaskMessageType::Schedule, &task_name),
                    ))
                    .await
                    .map_err(meta_service_error)?;
            }
            TaskMessage::AfterTask(task) => {
                if !task_mgr
                    .accept(&task_key)
                    .await
                    .map_err(meta_service_error)?
                {
                    return Ok(());
                }
                match task.status {
                    Status::Suspended => return Ok(()),
                    Status::Started => (),
                }
                self.update_task_afters(&task).await?;
            }
        }
        Ok(())
    }

    pub async fn subscribe(&self) -> Result<TaskMessageStream> {
        let (min, max) = TaskMessage::prefix_range();
        let left = TaskMessageIdent::new(&self.tenant, min).to_string_key();
        let right = TaskMessageIdent::new(&self.tenant, max).to_string_key();

        let watch = WatchRequest::new(left, Some(right)).with_initial_flush(true);
        let stream = self
            .meta_handle
            .meta_client()
            .watch_with_initialization(watch)
            .await
            .map_err(meta_client_error)?;

        Ok(Box::pin(stream.filter_map(|result| match result {
            Ok(resp) => match Self::decode(resp) {
                Ok(message) => message.map(Ok),
                // Skipped instead of returned: an error ends the watch, and every new watch would
                // end again at this message.
                Err(err) => {
                    error!("skip a task message that cannot be decoded: {}", err);
                    None
                }
            },
            Err(status) => Some(Err(ErrorCode::MetaServiceError(format!(
                "task watch-stream closed: {}",
                status
            )))),
        })))
    }

    fn decode(resp: WatchResponse) -> Result<Option<(String, SeqV<TaskMessage>)>> {
        let Some((key, _, Some(value))) = resp.unpack() else {
            return Ok(None);
        };
        let message = decode_seqv::<TaskMessage>(value, || format!("decode value of {}", key))
            .map_err(|e| ErrorCode::MetaServiceError(e.to_string()))?;

        Ok(Some((key, message)))
    }

    /// Store the next run of a schedule in its task. `Ok(false)` if the schedule must stop: its
    /// task was dropped, or no longer matches it.
    async fn update_schedule(task_mgr: &TaskMgr, task: &Task) -> Result<bool> {
        match task_mgr.update_task(task.clone()).await? {
            Ok(matches) => Ok(matches),
            // The task was dropped: stop, since a schedule that fails starts again.
            Err(TaskError::NotFound { .. }) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    async fn get_task_owner(task: &Task, tenant: &Tenant) -> Result<UserInfo> {
        UserApiProvider::instance()
            .get_user(
                tenant,
                UserIdentity::parse(&task.owner_user).map_err(|e| {
                    ErrorCode::MetaServiceError(format!("Failed to parse UserIdentity: {}", e))
                })?,
            )
            .await
    }

    async fn spawn_task(task: Task, user: UserInfo) -> Result<()> {
        let task_service = TaskService::instance();
        let context = task_service.create_task_context(user, &task).await?;
        // Task ownership remains on this node, while the SQL planner must see the complete
        // warehouse before constructing a distributed physical plan.
        context.set_cluster(context.get_warehouse_cluster().await?);

        match &task.task_sql {
            TaskSql::Sql(sql) => {
                task_service.execute_sql_in_context(context, sql).await?;
            }
            TaskSql::Script(sqls) => {
                task_service
                    .execute_script_sql_in_context(context, sqls)
                    .await?
            }
        }

        Ok(())
    }

    fn new_task_run(task: &Task) -> TaskRun {
        TaskRun {
            task: task.clone(),
            run_id: Self::make_run_id(),
            attempt_number: task.suspend_task_after_num_failures.unwrap_or(0) as i32,
            state: State::Executing,
            scheduled_at: Utc::now(),
            completed_at: None,
            error_code: 0,
            error_message: None,
            root_task_id: EMPTY_TASK_ID,
        }
    }

    fn new_terminal_task_run(
        task: &Task,
        state: State,
        error_code: i64,
        error_message: String,
    ) -> TaskRun {
        let now = Utc::now();
        TaskRun {
            task: task.clone(),
            run_id: Self::make_run_id(),
            attempt_number: 0,
            state,
            scheduled_at: now,
            completed_at: Some(now),
            error_code,
            error_message: Some(error_message),
            root_task_id: EMPTY_TASK_ID,
        }
    }

    async fn record_overlapping_skip(&self, task: &Task) -> Result<()> {
        let reason = format!("{OVERLAPPING_EXECUTION}: previous task run is still executing");
        let task_run = Self::new_terminal_task_run(task, State::Skipped, 0, reason);
        self.update_or_create_task_run(&task_run).await?;
        Ok(())
    }

    async fn check_when_and_record(
        task: &Task,
        user: &UserInfo,
        task_service: &Arc<TaskService>,
    ) -> Result<bool> {
        match Self::check_when(task, user, task_service).await {
            Ok(true) => Ok(true),
            Ok(false) => {
                let condition = task.when_condition.as_deref().unwrap_or_default();
                let reason =
                    format!("{WHEN_CONDITION_FALSE}: condition '{condition}' evaluated to false");
                let task_run = Self::new_terminal_task_run(task, State::Skipped, 0, reason);
                task_service.update_or_create_task_run(&task_run).await?;
                Ok(false)
            }
            Err(err) => {
                let reason = format!("{WHEN_CONDITION_EVALUATION_ERROR}: {}", err.message());
                let task_run =
                    Self::new_terminal_task_run(task, State::Failed, err.code() as i64, reason);
                task_service.update_or_create_task_run(&task_run).await?;
                Ok(false)
            }
        }
    }

    async fn check_when(
        task: &Task,
        user: &UserInfo,
        task_service: &Arc<TaskService>,
    ) -> Result<bool> {
        let Some(when_condition) = &task.when_condition else {
            return Ok(true);
        };
        let context = task_service.create_task_context(user.clone(), task).await?;
        let result = task_service
            .execute_sql_in_context(context, &format!("SELECT {when_condition}"))
            .await?;
        Ok(result
            .first()
            .and_then(|block| block.get_by_offset(0).index(0))
            .and_then(|scalar| {
                scalar
                    .as_boolean()
                    .cloned()
                    .map(Ok)
                    .or_else(|| scalar.as_string().map(|str| str.trim().parse::<bool>()))
            })
            .transpose()
            .map_err(|err| {
                ErrorCode::TaskWhenConditionNotMet(format!(
                    "when condition error for task: {}, {}",
                    task.task_name, err
                ))
            })?
            .unwrap_or(false))
    }

    pub async fn create_context(&self, other_user: Option<UserInfo>) -> Result<Arc<QueryContext>> {
        // only need run the sql on the current node
        let cluster_discovery = ClusterDiscovery::instance();
        let dummy_cluster = cluster_discovery
            .single_node_cluster(&GlobalConfig::instance())
            .await?;

        let (user, role) = if let Some(other_user) = other_user {
            (other_user, None)
        } else {
            (
                get_task_user(self.tenant.tenant_name(), &dummy_cluster.get_cluster_id()?),
                Some(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string()),
            )
        };

        let session = create_session(user, role).await?;
        session.create_query_context_with_cluster(dummy_cluster, &BUILD_INFO, None)
    }

    async fn create_task_context(&self, user: UserInfo, task: &Task) -> Result<Arc<QueryContext>> {
        let context = self.create_context(Some(user)).await?;
        Self::apply_task_session_params(&context, &task.session_params).await?;
        Ok(context)
    }

    async fn apply_task_session_params(
        context: &Arc<QueryContext>,
        session_params: &BTreeMap<String, String>,
    ) -> Result<()> {
        for (key, value) in session_params {
            if key.eq_ignore_ascii_case("database") {
                context.set_current_database(value.clone()).await?;
            } else {
                context
                    .get_settings()
                    .set_setting(key.to_lowercase(), value.clone())?;
            }
        }
        Ok(())
    }

    pub async fn has_executing_task_run(&self, task_name: &str) -> Result<bool> {
        let task_name = Self::sql_string_literal(task_name);
        let blocks = self
            .execute_sql(
                None,
                &format!(
                    "SELECT count(*) FROM system_task.task_run \
                    WHERE task_name = {task_name} \
                    AND state = 'EXECUTING' \
                    AND completed_at IS NULL;"
                ),
            )
            .await?;

        let count = blocks
            .first()
            .and_then(|block| block.get_by_offset(0).index(0))
            .and_then(|scalar| {
                scalar
                    .as_number()
                    .and_then(|number| number.as_u_int64())
                    .cloned()
            })
            .unwrap_or(0);

        Ok(count > 0)
    }

    pub async fn update_or_create_task_run(&self, task_run: &TaskRun) -> Result<()> {
        let state = match task_run.state {
            State::Scheduled => "SCHEDULED".to_string(),
            State::Executing => "EXECUTING".to_string(),
            State::Succeeded => "SUCCEEDED".to_string(),
            State::Failed => "FAILED".to_string(),
            State::Cancelled => "CANCELLED".to_string(),
            State::Skipped => "SKIPPED".to_string(),
        };
        let scheduled_at = task_run.scheduled_at.timestamp();
        let completed_at = task_run
            .completed_at
            .map(|time| time.timestamp().to_string())
            .unwrap_or_else(|| "null".to_string());
        let error_message = task_run
            .error_message
            .as_ref()
            .map(|s| Self::sql_string_literal(s))
            .unwrap_or_else(|| "null".to_string());
        let root_task_id = task_run.root_task_id;
        let task_name = Self::sql_string_literal(&task_run.task.task_name);

        let is_exists = self.execute_sql(None, &format!("UPDATE system_task.task_run SET run_id = {}, attempt_number = {}, state = '{}', scheduled_at = {}, completed_at = {}, error_code = {}, error_message = {}, root_task_id = {} WHERE task_name = {} AND run_id = {}", task_run.run_id, task_run.attempt_number, state, scheduled_at, completed_at, task_run.error_code, error_message, root_task_id, task_name, task_run.run_id)).await?
            .first()
            .and_then(|block| {
                block.get_by_offset(0).index(0).and_then(|s| s.as_number().and_then(|n| n.as_u_int64().cloned()))
            })
            .map(|v| v > 0)
            .unwrap_or(false);

        if !is_exists {
            self.execute_sql(None, &Self::task_run2insert(task_run)?)
                .await?;
        }
        Ok(())
    }

    pub fn check_next_tasks<'a>(
        &'a self,
        task_name: &'a str,
    ) -> impl Stream<Item = Result<String>> + '_ {
        stream! {
            let task_name = Self::sql_string_literal(task_name);
            let check = format!("
            WITH latest_task_run AS (
    SELECT
        task_name,
        state,
        completed_at
    FROM (
        SELECT
            task_name,
            state,
            completed_at,
            ROW_NUMBER() OVER (PARTITION BY task_name ORDER BY completed_at DESC) AS rn
        FROM system_task.task_run
        WHERE NOT (state = 'SKIPPED' AND COALESCE(error_message, '') LIKE 'OVERLAPPING_EXECUTION:%')
    ) ranked
    WHERE rn = 1
),
next_task_time AS (
    SELECT
        task_name AS next_task,
        completed_at
    FROM latest_task_run
)
SELECT DISTINCT ta.next_task
FROM system_task.task_after ta
WHERE ta.task_name = {task_name}
  AND NOT EXISTS (
    SELECT 1
    FROM system_task.task_after ta_dep
    LEFT JOIN latest_task_run tr
      ON ta_dep.task_name = tr.task_name
    LEFT JOIN next_task_time nt
      ON ta_dep.next_task = nt.next_task
    WHERE ta_dep.next_task = ta.next_task
      AND (
        tr.task_name IS NULL
        OR tr.state != 'SUCCEEDED'
        OR tr.completed_at IS NULL
        OR (nt.completed_at IS NOT NULL AND tr.completed_at <= nt.completed_at)
      )
  );");
            let blocks = self.execute_sql(None, &check).await?;
            for next_task in Self::next_task_names_from_blocks(&blocks) {
                yield Result::Ok(next_task);
            }
        }
    }

    fn next_task_names_from_blocks(blocks: &[DataBlock]) -> impl Iterator<Item = String> + '_ {
        blocks.iter().flat_map(|block| {
            let column = block.columns().first();
            (0..block.num_rows()).filter_map(move |row| {
                column.and_then(|column| {
                    column
                        .index(row)
                        .and_then(|scalar| scalar.as_string().map(|s| s.to_string()))
                })
            })
        })
    }

    pub async fn clean_task_afters(&self, task_name: &str) -> Result<()> {
        self.execute_sql(
            None,
            &format!(
                "DELETE FROM system_task.task_after WHERE next_task = {}",
                Self::sql_string_literal(task_name)
            ),
        )
        .await?;

        Ok(())
    }

    pub async fn cancel_open_task_runs(
        &self,
        task_name: &str,
        task_id: u64,
        error_message: &str,
    ) -> Result<u64> {
        let task_name = Self::sql_string_literal(task_name);
        let error_message = Self::sql_string_literal(error_message);

        let blocks = self
            .execute_sql(
                None,
                &format!(
                    "UPDATE system_task.task_run \
                SET state = 'CANCELLED', \
                    completed_at = {}, \
                    error_code = 0, \
                    error_message = {} \
                WHERE task_name = {} \
                    AND task_id <= {} \
                    AND state = 'EXECUTING' \
                    AND completed_at IS NULL;",
                    Utc::now().timestamp(),
                    error_message,
                    task_name,
                    task_id
                ),
            )
            .await?;

        let affected_rows = blocks
            .first()
            .and_then(|block| block.get_by_offset(0).index(0))
            .and_then(|scalar| {
                scalar
                    .as_number()
                    .and_then(|number| number.as_u_int64())
                    .cloned()
            })
            .unwrap_or(0);

        Ok(affected_rows)
    }

    pub async fn update_task_afters(&self, task: &Task) -> Result<()> {
        self.clean_task_afters(&task.task_name).await?;
        let values = task
            .after
            .iter()
            .map(|after| {
                format!(
                    "({}, {})",
                    Self::sql_string_literal(after),
                    Self::sql_string_literal(&task.task_name)
                )
            })
            .join(", ");
        self.execute_sql(
            None,
            &format!(
                "INSERT INTO system_task.task_after (task_name, next_task) VALUES {}",
                values
            ),
        )
        .await?;

        Ok(())
    }

    fn task_run2insert(task_run: &TaskRun) -> Result<String> {
        let task = &task_run.task;
        let task_name = Self::sql_string_literal(&task.task_name);
        let query_text = Self::sql_string_literal(&task.task_sql.query_text());
        let when_condition = Self::sql_optional_string(task.when_condition.as_deref());
        let after = if !task.after.is_empty() {
            Self::sql_string_literal(&task.after.join(", "))
        } else {
            "null".to_string()
        };
        let comment = Self::sql_optional_string(task.comment.as_deref());
        let owner = Self::sql_string_literal(&task.owner);
        let owner_user = Self::sql_string_literal(&task.owner_user);
        let warehouse_name = Self::sql_optional_string(
            task.warehouse_options
                .as_ref()
                .and_then(|w| w.warehouse.as_deref()),
        );
        let using_warehouse_size = Self::sql_optional_string(
            task.warehouse_options
                .as_ref()
                .and_then(|w| w.using_warehouse_size.as_deref()),
        );
        let cron = Self::sql_optional_string(
            task.schedule_options
                .as_ref()
                .and_then(|s| s.cron.as_deref()),
        );
        let time_zone = Self::sql_optional_string(
            task.schedule_options
                .as_ref()
                .and_then(|s| s.time_zone.as_deref()),
        );
        let state = Self::sql_string_literal(match task_run.state {
            State::Scheduled => "SCHEDULED",
            State::Executing => "EXECUTING",
            State::Succeeded => "SUCCEEDED",
            State::Failed => "FAILED",
            State::Cancelled => "CANCELLED",
            State::Skipped => "SKIPPED",
        });
        let error_message = Self::sql_optional_string(task_run.error_message.as_deref());
        let completed_at = task_run
            .completed_at
            .as_ref()
            .map(|d| d.timestamp().to_string())
            .unwrap_or_else(|| "null".to_string());
        let error_integration = Self::sql_optional_string(task.error_integration.as_deref());
        let status = Self::sql_string_literal(match task.status {
            Status::Suspended => "SUSPENDED",
            Status::Started => "STARTED",
        });
        let session_params =
            Self::sql_string_literal(&serde_json::to_string(&task.session_params)?);

        let sql = format!(
            "INSERT INTO system_task.task_run (\
            task_id,
            task_name,
            query_text,
            when_condition,
            after,
            comment,
            owner,
            owner_user,
            warehouse_name,
            using_warehouse_size,
            schedule_type,
            interval,
            interval_milliseconds,
            cron,
            time_zone,
            run_id,
            attempt_number,
            state,
            error_code,
            error_message,
            root_task_id,
            scheduled_at,
            completed_at,
            next_scheduled_at,
            error_integration,
            status,
            created_at,
            updated_at,
            session_params,
            last_suspended_at,
            suspend_task_after_num_failures
            ) values (
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {}
            );",
            task.task_id,
            task_name,
            query_text,
            when_condition,
            after,
            comment,
            owner,
            owner_user,
            warehouse_name,
            using_warehouse_size,
            task.schedule_options
                .as_ref()
                .map(|s| match s.schedule_type {
                    ScheduleType::IntervalType => "0".to_string(),
                    ScheduleType::CronType => "1".to_string(),
                })
                .unwrap_or_else(|| "null".to_string()),
            task.schedule_options
                .as_ref()
                .and_then(|s| s.interval)
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".to_string()),
            task.schedule_options
                .as_ref()
                .and_then(|s| s.milliseconds_interval)
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".to_string()),
            cron,
            time_zone,
            task_run.run_id,
            task_run.attempt_number,
            state,
            task_run.error_code,
            error_message,
            task_run.root_task_id,
            task_run.scheduled_at.timestamp(),
            completed_at,
            task.next_scheduled_at
                .as_ref()
                .map(|d| d.timestamp().to_string())
                .unwrap_or_else(|| "null".to_string()),
            error_integration,
            status,
            task.created_at.timestamp(),
            task.updated_at.timestamp(),
            session_params,
            task.last_suspended_at
                .as_ref()
                .map(|d| d.timestamp().to_string())
                .unwrap_or_else(|| "null".to_string()),
            task.suspend_task_after_num_failures
                .map(|s| s.to_string())
                .unwrap_or_else(|| "null".to_string())
        );
        Ok(sql)
    }

    fn sql_string_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    fn sql_optional_string(value: Option<&str>) -> String {
        value
            .map(Self::sql_string_literal)
            .unwrap_or_else(|| "null".to_string())
    }

    async fn execute_sql(&self, other_user: Option<UserInfo>, sql: &str) -> Result<Vec<DataBlock>> {
        let context = self.create_context(other_user).await?;
        self.execute_sql_in_context(context, sql).await
    }

    async fn execute_sql_in_context(
        &self,
        context: Arc<QueryContext>,
        sql: &str,
    ) -> Result<Vec<DataBlock>> {
        let mut planner = Planner::new_with_query_executor(
            context.clone(),
            Arc::new(ServiceQueryExecutor::new(QueryContext::create_from(
                context.as_ref(),
            ))),
        );
        let (plan, _) = planner.plan_sql(sql).await?;
        let executor = InterpreterFactory::get(context.clone(), &plan).await?;
        let stream = executor
            .execute_with_hooks(context, QueryFinishHooks::nested_with_hooks())
            .await?;
        stream.try_collect::<Vec<DataBlock>>().await
    }

    async fn execute_script_sql_in_context(
        &self,
        context: Arc<QueryContext>,
        sqls: &[String],
    ) -> Result<()> {
        let sql_dialect = context.get_settings().get_sql_dialect()?;
        let script = Self::script_sql_to_block(sqls);
        let tokens = tokenize_sql(&script)?;
        let ast = run_parser(
            &tokens,
            sql_dialect,
            ParseMode::Template,
            false,
            script_block,
        )?;
        let span = ast.span;
        let compiled = compile_block(ast)?;

        let client = ScriptClient {
            ctx: context.clone(),
        };
        let mut executor = Executor::load(span, client, compiled);
        let settings = context.get_settings();
        let script_max_steps = settings.get_script_max_steps()?;
        executor.run(script_max_steps as usize).await?;

        Ok(())
    }

    fn script_sql_to_block(sqls: &[String]) -> String {
        let mut script = String::from("BEGIN\n");
        for sql in sqls {
            let _ = writeln!(script, "{};", sql);
        }
        script.push_str("END;");
        script
    }

    fn make_run_id() -> u64 {
        Utc::now().timestamp_micros() as u64
    }
}

/// Run `schedule` until it ends without an error, or `cancel` fires while it waits to start
/// again.
///
/// A schedule fails on an error in one of its ticks, e.g. a meta-service error while the
/// meta-service leader changes, and must not end there: it starts again after `delay`.
async fn keep_running(
    name: &str,
    delay: Duration,
    cancel: CancellationToken,
    mut schedule: impl AsyncFnMut() -> Result<()>,
) {
    while let Err(err) = schedule().await {
        error!("{} failed: {}; start it again in {:?}", name, err, delay);
        tokio::select! {
            _ = sleep(delay) => {}
            _ = cancel.cancelled() => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use databend_common_base::runtime::spawn;
    use databend_common_exception::ErrorCode;
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;

    use super::ScheduledTask;
    use super::keep_running;

    fn scheduled_task(message_seq: u64, handle: JoinHandle<()>) -> ScheduledTask {
        ScheduledTask {
            task_id: 1,
            message_seq,
            token: CancellationToken::new(),
            handle,
        }
    }

    /// A new watch brings a `ScheduleTask` message again: the schedule it started goes on,
    /// unless that has stopped, and another message replaces the schedule.
    #[tokio::test]
    async fn test_scheduled_task_runs_message() {
        let running = scheduled_task(5, spawn(std::future::pending::<()>()));
        assert!(running.runs_message(5));
        assert!(!running.runs_message(6));

        let handle = spawn(async {});
        while !handle.is_finished() {
            tokio::task::yield_now().await;
        }
        let stopped = scheduled_task(5, handle);
        assert!(!stopped.runs_message(5));
    }

    /// A schedule that fails starts again until it ends without an error. Cancelling it while it
    /// waits to start again ends it.
    #[tokio::test]
    async fn test_keep_running() {
        let runs = AtomicUsize::new(0);
        keep_running(
            "schedule",
            Duration::from_millis(1),
            CancellationToken::new(),
            async || match runs.fetch_add(1, Ordering::Relaxed) {
                0 | 1 => Err(ErrorCode::MetaServiceError("meta leader changed")),
                _ => Ok(()),
            },
        )
        .await;
        assert_eq!(runs.load(Ordering::Relaxed), 3);

        let cancel = CancellationToken::new();
        cancel.cancel();
        let runs = AtomicUsize::new(0);
        keep_running("schedule", Duration::from_secs(3600), cancel, async || {
            runs.fetch_add(1, Ordering::Relaxed);
            Err(ErrorCode::MetaServiceError("meta leader changed"))
        })
        .await;
        assert_eq!(runs.load(Ordering::Relaxed), 1);
    }
}
