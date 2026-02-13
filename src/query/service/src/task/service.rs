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
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_stream::stream;
use chrono::DateTime;
use chrono::Utc;
use chrono_tz::Tz;
use cron::Schedule;
use databend_common_ast::ast::AlterTaskOptions;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::Runtime;
use databend_common_catalog::table_context::TableContext;
use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_meta_api::kv_pb_api::decode_seqv;
use databend_common_meta_app::principal::ScheduleOptions;
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
use databend_common_meta_app::principal::task_message_ident::TaskMessageIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_sql::Planner;
use databend_common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;
use databend_common_users::UserApiProvider;
use databend_common_version::BUILD_INFO;
use databend_meta_kvapi::kvapi::Key;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_types::protobuf::WatchRequest;
use databend_meta_types::protobuf::WatchResponse;
use futures::Stream;
use futures_util::TryStreamExt;
use futures_util::stream::BoxStream;
use itertools::Itertools;
use log::error;
use tokio::time::sleep;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::clusters::ClusterDiscovery;
use crate::interpreters::InterpreterFactory;
use crate::meta_client_error;
use crate::meta_service_error;
use crate::schedulers::ServiceQueryExecutor;
use crate::sessions::QueryContext;
use crate::task::meta::TaskMetaHandle;
use crate::task::session::create_session;
use crate::task::session::get_task_user;

pub type TaskMessageStream = BoxStream<'static, Result<(String, TaskMessage)>>;

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
            .create_meta_store::<DatabendRuntime>()
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
            if let Err(err) = task_service.work(&tenant, runtime).await {
                error!("prepare failed due to {}", err);
            }
        });
        Ok(())
    }

    async fn work(&self, tenant: &Tenant, runtime: Arc<Runtime>) -> Result<()> {
        let mut scheduled_tasks: HashMap<String, CancellationToken> = HashMap::new();
        let task_mgr = UserApiProvider::instance().task_api(tenant);

        let mut steam = self.subscribe().await?;

        while let Some(result) = steam.next().await {
            let (_, task_message) = result?;
            let task_key = TaskMessageIdent::new(tenant, task_message.key());

            if let Some(WarehouseOptions {
                warehouse: Some(warehouse),
                ..
            }) = task_message.warehouse_options()
            {
                if warehouse
                    != &self
                        .create_context(None)
                        .await?
                        .get_cluster()
                        .get_warehouse_id()?
                {
                    continue;
                }
            }
            match task_message {
                // ScheduleTask is always monitored by all Query nodes, and ExecuteTask is sent serially to avoid repeated sending.
                TaskMessage::ScheduleTask(mut task) => {
                    debug_assert!(task.schedule_options.is_some());
                    if let Some(schedule_options) = &task.schedule_options {
                        // clean old task if alter
                        if let Some(token) = scheduled_tasks.remove(&task.task_name) {
                            token.cancel();
                        }
                        match task.status {
                            Status::Suspended => continue,
                            Status::Started => (),
                        }

                        let token = CancellationToken::new();
                        let child_token = token.child_token();
                        let task_name = task.task_name.to_string();
                        let task_name_clone = task_name.clone();
                        let task_service = TaskService::instance();
                        let owner = Self::get_task_owner(&task, tenant).await?;

                        let fn_lock =
                            async |task_service: &TaskService,
                                   key: &TaskMessageIdent,
                                   interval_millis: u64| {
                                task_service
                                    .meta_handle
                                    .acquire_with_guard(&format!("{}/lock", key), interval_millis)
                                    .await
                            };

                        let fn_new_task_run = async |task_service: &TaskService, task: &Task| {
                            task_service
                                .update_or_create_task_run(&TaskRun {
                                    task: task.clone(),
                                    run_id: Self::make_run_id(),
                                    attempt_number: task
                                        .suspend_task_after_num_failures
                                        .unwrap_or(0)
                                        as i32,
                                    state: State::Scheduled,
                                    scheduled_at: Utc::now(),
                                    completed_at: None,
                                    error_code: 0,
                                    error_message: None,
                                    root_task_id: EMPTY_TASK_ID,
                                })
                                .await
                        };
                        match schedule_options.schedule_type {
                            ScheduleType::IntervalType => {
                                let task_mgr = task_mgr.clone();
                                let mut duration =
                                    Duration::from_secs(schedule_options.interval.unwrap() as u64);
                                if let Some(ms) = &schedule_options.milliseconds_interval {
                                    duration += Duration::from_millis(*ms);
                                }

                                runtime
                                    .spawn(async move {
                                        let mut fn_work = async move || {
                                            task.next_scheduled_at = Some(Utc::now() + duration);
                                            task_mgr.update_task(task.clone()).await??;
                                            loop {
                                                tokio::select! {
                                                    _ = sleep(duration) => {
                                                        let Some(_guard) = fn_lock(&task_service, &task_key, duration.as_millis() as u64).await? else {
                                                            continue;
                                                        };
                                                        if !Self::check_when(&task, &owner, &task_service).await? {
                                                            continue;
                                                        }
                                                        fn_new_task_run(&task_service, &task).await?;
                                                        task_mgr.send(TaskMessage::ExecuteTask(task.clone())).await.map_err(meta_service_error)?;
                                                    }
                                                    _ = child_token.cancelled() => {
                                                        break;
                                                    }
                                                }
                                            }
                                            Result::Ok(())
                                        };
                                        if let Err(err) = fn_work().await {
                                            error!("interval schedule failed due to {}", err);
                                        }
                                    });
                            }
                            ScheduleType::CronType => {
                                let task_mgr = task_mgr.clone();
                                // SAFETY: check on CreateTask
                                let cron_expr = schedule_options.cron.as_ref().unwrap();
                                let tz = schedule_options
                                    .time_zone
                                    .as_ref()
                                    .map(|tz| tz.parse::<Tz>())
                                    .transpose()?
                                    .unwrap_or(Tz::UCT);
                                let schedule = Schedule::from_str(cron_expr).unwrap();

                                runtime
                                    .spawn(async move {
                                        let mut fn_work = async move || {
                                            let upcoming = schedule.upcoming(tz);

                                            for next_time in upcoming {
                                                let now = Utc::now();
                                                let duration = (next_time.with_timezone(&Utc) - now)
                                                    .to_std()
                                                    .unwrap_or(Duration::ZERO);

                                                task.next_scheduled_at = Some(Utc::now() + duration);
                                                task_mgr.update_task(task.clone()).await??;
                                                tokio::select! {
                                                    _ = sleep(duration) => {
                                                        let Some(_guard) = fn_lock(&task_service, &task_key, duration.as_millis() as u64).await? else {
                                                            continue;
                                                        };
                                                        if !Self::check_when(&task, &owner, &task_service).await? {
                                                            continue;
                                                        }
                                                        fn_new_task_run(&task_service, &task).await?;
                                                        task_mgr.send(TaskMessage::ExecuteTask(task.clone())).await.map_err(meta_service_error)?;
                                                    }
                                                    _ = child_token.cancelled() => {
                                                        break;
                                                    }
                                                }
                                            }
                                            Result::Ok(())
                                        };
                                        if let Err(err) = fn_work().await {
                                            error!("cron schedule failed due to {}", err);
                                        }
                                    });
                            }
                        }
                        let _ = scheduled_tasks.insert(task_name_clone, token);
                    }
                }
                TaskMessage::ExecuteTask(task) => {
                    if !task_mgr
                        .accept(&task_key)
                        .await
                        .map_err(meta_service_error)?
                    {
                        continue;
                    }
                    let task_name = task.task_name.clone();
                    let task_service = TaskService::instance();

                    let mut task_run = task_service
                        .lasted_task_run(&task_name)
                        .await?
                        .unwrap_or_else(|| TaskRun {
                            task: task.clone(),
                            run_id: Self::make_run_id(),
                            attempt_number: task.suspend_task_after_num_failures.unwrap_or(0)
                                as i32,
                            state: State::Executing,
                            scheduled_at: Utc::now(),
                            completed_at: None,
                            error_code: 0,
                            error_message: None,
                            root_task_id: EMPTY_TASK_ID,
                        });
                    task_service.update_or_create_task_run(&task_run).await?;

                    let task_mgr = task_mgr.clone();
                    let tenant = tenant.clone();
                    let owner = Self::get_task_owner(&task, &tenant).await?;

                    runtime.spawn(async move {
                        let mut fn_work = async move || {
                            while task_run.attempt_number >= 0 {
                                let task_result =
                                    Self::spawn_task(task.clone(), owner.clone()).await;

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
                                            if Self::check_when(
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
                                        task_run.run_id = Self::make_run_id();
                                    }
                                }
                                task_mgr
                                    .alter_task(&task.task_name, &AlterTaskOptions::Suspend)
                                    .await??;
                            }

                            Result::Ok(())
                        };
                        if let Err(err) = fn_work().await {
                            error!("execute failed due to {}", err);
                        }
                    });
                }
                TaskMessage::DeleteTask(task_name, _) => {
                    if let Some(token) = scheduled_tasks.remove(&task_name) {
                        token.cancel();
                    }
                    if task_mgr
                        .accept(&task_key)
                        .await
                        .map_err(meta_service_error)?
                    {
                        self.clean_task_afters(&task_name).await?;
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
                        continue;
                    }
                    match task.status {
                        Status::Suspended => continue,
                        Status::Started => (),
                    }
                    self.update_task_afters(&task).await?;
                }
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

        Ok(Box::pin(stream.filter_map(|result| {
            result
                .map(Self::decode)
                .map_err(|_| ErrorCode::MetaServiceError("task watch-stream closed"))
                .flatten()
                .transpose()
        })))
    }

    fn decode(resp: WatchResponse) -> Result<Option<(String, TaskMessage)>> {
        let Some((key, _, Some(value))) = resp.unpack() else {
            return Ok(None);
        };
        let message = decode_seqv::<TaskMessage>(value, || format!("decode value of {}", key))
            .map_err(|e| ErrorCode::MetaServiceError(e.to_string()))?;

        Ok(Some((key, TaskMessage::clone(message.deref()))))
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

        task_service
            .execute_sql(Some(user), &task.query_text)
            .await?;

        Ok(())
    }

    async fn check_when(
        task: &Task,
        user: &UserInfo,
        task_service: &Arc<TaskService>,
    ) -> Result<bool> {
        let Some(when_condition) = &task.when_condition else {
            return Ok(true);
        };
        let result = task_service
            .execute_sql(Some(user.clone()), &format!("SELECT {when_condition}"))
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
        session.create_query_context_with_cluster(dummy_cluster, &BUILD_INFO)
    }

    pub async fn lasted_task_run(&self, task_name: &str) -> Result<Option<TaskRun>> {
        let blocks = self
            .execute_sql(
                None,
                &format!(
                    "SELECT
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
    FROM system_task.task_run WHERE task_name = '{task_name}' ORDER BY run_id DESC LIMIT 1;"
                ),
            )
            .await?;

        let Some(block) = blocks.first() else {
            return Ok(None);
        };
        if block.num_rows() == 0 {
            return Ok(None);
        }
        Ok(Self::block2task_run(block, 0))
    }

    pub async fn update_or_create_task_run(&self, task_run: &TaskRun) -> Result<()> {
        let state = match task_run.state {
            State::Scheduled => "SCHEDULED".to_string(),
            State::Executing => "EXECUTING".to_string(),
            State::Succeeded => "SUCCEEDED".to_string(),
            State::Failed => "FAILED".to_string(),
            State::Cancelled => "CANCELLED".to_string(),
        };
        let scheduled_at = task_run.scheduled_at.timestamp();
        let completed_at = task_run
            .completed_at
            .map(|time| time.timestamp().to_string())
            .unwrap_or_else(|| "null".to_string());
        let error_message = task_run
            .error_message
            .as_ref()
            .map(|s| format!("'{s}'"))
            .unwrap_or_else(|| "null".to_string());
        let root_task_id = task_run.root_task_id;

        let is_exists = self.execute_sql(None, &format!("UPDATE system_task.task_run SET run_id = {}, state = '{}', scheduled_at = {}, completed_at = {}, error_message = {}, root_task_id = {} WHERE task_name = '{}' AND run_id = {}", task_run.run_id, state, scheduled_at, completed_at, error_message, root_task_id, task_run.task.task_name, task_run.run_id)).await?
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
WHERE ta.task_name = '{task_name}'
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
            if let Some(next_task) = self.execute_sql(None, &check).await?.first().and_then(|block| block.columns()[0].index(0).and_then(|scalar| { scalar.as_string().map(|s| s.to_string()) })) {
                yield Result::Ok(next_task);
            }
        }
    }

    pub async fn clean_task_afters(&self, task_name: &str) -> Result<()> {
        self.execute_sql(
            None,
            &format!(
                "DELETE FROM system_task.task_after WHERE next_task = '{}'",
                task_name
            ),
        )
        .await?;

        Ok(())
    }

    pub async fn update_task_afters(&self, task: &Task) -> Result<()> {
        self.clean_task_afters(&task.task_name).await?;
        let values = task
            .after
            .iter()
            .map(|after| format!("('{}', '{}')", after, task.task_name))
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
                '{}',
                '{}',
                {},
                {},
                {},
                '{}',
                '{}',
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                '{}',
                {},
                {},
                {},
                {},
                {},
                {},
                {},
                '{}',
                {},
                {},
                {},
                {},
                {}
            );",
            task.task_id,
            task.task_name,
            task.query_text.replace('\'', "''"),
            task.when_condition
                .as_ref()
                .map(|s| format!("'{}'", s.replace('\'', "''")))
                .unwrap_or_else(|| "null".to_string()),
            if !task.after.is_empty() {
                format!("'{}'", task.after.join(", "))
            } else {
                "null".to_string()
            },
            task.comment
                .as_ref()
                .map(|s| format!("'{s}'"))
                .unwrap_or_else(|| "null".to_string()),
            task.owner,
            task.owner_user.replace('\'', "''"),
            task.warehouse_options
                .as_ref()
                .and_then(|w| w.warehouse.as_ref())
                .map(|s| format!("'{s}'"))
                .unwrap_or_else(|| "null".to_string()),
            task.warehouse_options
                .as_ref()
                .and_then(|w| w.using_warehouse_size.as_ref())
                .map(|s| format!("'{s}'"))
                .unwrap_or_else(|| "null".to_string()),
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
            task.schedule_options
                .as_ref()
                .and_then(|s| s.cron.as_ref())
                .map(|s| format!("'{s}'"))
                .unwrap_or_else(|| "null".to_string()),
            task.schedule_options
                .as_ref()
                .and_then(|s| s.time_zone.as_ref())
                .map(|s| format!("'{s}'"))
                .unwrap_or_else(|| "null".to_string()),
            task_run.run_id,
            task_run.attempt_number,
            match task_run.state {
                State::Scheduled => "SCHEDULED".to_string(),
                State::Executing => "EXECUTING".to_string(),
                State::Succeeded => "SUCCEEDED".to_string(),
                State::Failed => "FAILED".to_string(),
                State::Cancelled => "CANCELLED".to_string(),
            },
            task_run.error_code,
            task_run
                .error_message
                .as_ref()
                .map(|s| format!("'{s}'"))
                .unwrap_or_else(|| "null".to_string()),
            task_run.root_task_id,
            task_run.scheduled_at.timestamp(),
            task_run
                .completed_at
                .as_ref()
                .map(|d| d.to_string())
                .unwrap_or_else(|| "null".to_string()),
            task.next_scheduled_at
                .as_ref()
                .map(|d| d.timestamp().to_string())
                .unwrap_or_else(|| "null".to_string()),
            task.error_integration
                .as_ref()
                .map(|s| format!("'{s}'"))
                .unwrap_or_else(|| "null".to_string()),
            match task.status {
                Status::Suspended => "SUSPENDED".to_string(),
                Status::Started => "STARTED".to_string(),
            },
            task.created_at.timestamp(),
            task.updated_at.timestamp(),
            serde_json::to_string(&task.session_params).map(|s| format!("'{s}'"))?,
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

    fn block2task_run(block: &DataBlock, row: usize) -> Option<TaskRun> {
        let task_id = *block
            .get_by_offset(0)
            .index(row)?
            .as_number()?
            .as_u_int64()?;
        let task_name = block.get_by_offset(1).index(row)?.as_string()?.to_string();
        let query_text = block.get_by_offset(2).index(row)?.as_string()?.to_string();
        let when_condition = block
            .get_by_offset(3)
            .index(row)
            .and_then(|s| s.as_string().map(|s| s.to_string()));
        let after = block
            .get_by_offset(4)
            .index(row)?
            .as_string()?
            .split(", ")
            .map(str::to_string)
            .collect::<Vec<_>>();
        let comment = block
            .get_by_offset(5)
            .index(row)
            .and_then(|s| s.as_string().map(|s| s.to_string()));
        let owner = block.get_by_offset(6).index(row)?.as_string()?.to_string();
        let owner_user = block.get_by_offset(7).index(row)?.as_string()?.to_string();
        let warehouse_name = block
            .get_by_offset(8)
            .index(row)
            .and_then(|s| s.as_string().map(|s| s.to_string()));
        let using_warehouse_size = block
            .get_by_offset(9)
            .index(row)
            .and_then(|s| s.as_string().map(|s| s.to_string()));
        let schedule_type = block
            .get_by_offset(10)
            .index(row)
            .and_then(|s| s.as_number().and_then(|n| n.as_int32()).cloned());
        let interval = block
            .get_by_offset(11)
            .index(row)
            .and_then(|s| s.as_number().and_then(|n| n.as_int32()).cloned());
        let milliseconds_interval = block
            .get_by_offset(12)
            .index(row)
            .and_then(|s| s.as_number().and_then(|n| n.as_u_int64()).cloned());
        let cron = block
            .get_by_offset(13)
            .index(row)
            .and_then(|s| s.as_string().map(|s| s.to_string()));
        let time_zone = block
            .get_by_offset(14)
            .index(row)
            .and_then(|s| s.as_string().map(|s| s.to_string()));
        let run_id = *block
            .get_by_offset(15)
            .index(row)?
            .as_number()?
            .as_u_int64()?;
        let attempt_number = block
            .get_by_offset(16)
            .index(row)
            .and_then(|s| s.as_number().and_then(|n| n.as_int32()).cloned());
        let state = block.get_by_offset(17).index(row)?.as_string()?.to_string();
        let error_code = *block
            .get_by_offset(18)
            .index(row)?
            .as_number()?
            .as_int64()?;
        let error_message = block
            .get_by_offset(19)
            .index(row)
            .and_then(|s| s.as_string().map(|s| s.to_string()));
        let root_task_id = *block
            .get_by_offset(20)
            .index(row)?
            .as_number()?
            .as_u_int64()?;
        let scheduled_at = *block.get_by_offset(21).index(row)?.as_timestamp()?;
        let completed_at = block
            .get_by_offset(22)
            .index(row)
            .and_then(|s| s.as_timestamp().cloned());
        let next_scheduled_at = block
            .get_by_offset(23)
            .index(row)
            .and_then(|s| s.as_timestamp().cloned());
        let error_integration = block
            .get_by_offset(24)
            .index(row)
            .and_then(|s| s.as_string().map(|s| s.to_string()));
        let status = block.get_by_offset(25).index(row)?.as_string()?.to_string();
        let created_at = *block.get_by_offset(26).index(row)?.as_timestamp()?;
        let updated_at = *block.get_by_offset(27).index(row)?.as_timestamp()?;
        let session_params = block.get_by_offset(28).index(row).and_then(|s| {
            s.as_variant()
                .and_then(|bytes| serde_json::from_slice::<BTreeMap<String, String>>(bytes).ok())
        })?;
        let last_suspended_at = block
            .get_by_offset(29)
            .index(row)
            .and_then(|s| s.as_timestamp().cloned());

        let schedule_options = if let Some(s) = schedule_type {
            let schedule_type = match s {
                0 => ScheduleType::IntervalType,
                1 => ScheduleType::CronType,
                _ => {
                    return None;
                }
            };
            Some(ScheduleOptions {
                interval,
                cron,
                time_zone,
                schedule_type,
                milliseconds_interval,
            })
        } else {
            None
        };

        let warehouse_options = if warehouse_name.is_some() && using_warehouse_size.is_some() {
            Some(WarehouseOptions {
                warehouse: warehouse_name,
                using_warehouse_size,
            })
        } else {
            None
        };
        let task = Task {
            task_id,
            task_name,
            query_text,
            when_condition,
            after,
            comment,
            owner,
            owner_user,
            schedule_options,
            warehouse_options,
            next_scheduled_at: next_scheduled_at
                .and_then(|i| DateTime::<Utc>::from_timestamp(i, 0)),
            suspend_task_after_num_failures: attempt_number.map(|i| i as u64),
            error_integration,
            status: match status.as_str() {
                "SUSPENDED" => Status::Suspended,
                "STARTED" => Status::Started,
                _ => return None,
            },
            created_at: DateTime::<Utc>::from_timestamp(created_at, 0)?,
            updated_at: DateTime::<Utc>::from_timestamp(updated_at, 0)?,
            last_suspended_at: last_suspended_at
                .and_then(|i| DateTime::<Utc>::from_timestamp(i, 0)),
            session_params,
        };

        Some(TaskRun {
            task,
            run_id,
            attempt_number: attempt_number.unwrap_or_default(),
            state: match state.as_str() {
                "SCHEDULED" => State::Scheduled,
                "EXECUTING" => State::Executing,
                "SUCCEEDED" => State::Succeeded,
                "FAILED" => State::Failed,
                "CANCELLED" => State::Cancelled,
                _ => return None,
            },
            scheduled_at: DateTime::<Utc>::from_timestamp(scheduled_at, 0)?,
            completed_at: completed_at.and_then(|i| DateTime::<Utc>::from_timestamp(i, 0)),
            error_code,
            error_message,
            root_task_id,
        })
    }

    async fn execute_sql(&self, other_user: Option<UserInfo>, sql: &str) -> Result<Vec<DataBlock>> {
        let context = self.create_context(other_user).await?;

        let mut planner = Planner::new_with_query_executor(
            context.clone(),
            Arc::new(ServiceQueryExecutor::new(QueryContext::create_from(
                context.as_ref(),
            ))),
        );
        let (plan, _) = planner.plan_sql(sql).await?;
        let executor = InterpreterFactory::get(context.clone(), &plan).await?;
        let stream = executor.execute(context).await?;
        stream.try_collect::<Vec<DataBlock>>().await
    }

    fn make_run_id() -> u64 {
        Utc::now().timestamp_millis() as u64
    }
}
