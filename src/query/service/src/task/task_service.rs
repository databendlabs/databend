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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use chrono_tz::Tz;
use cron::Schedule;
use databend_common_ast::ast::AlterTaskOptions;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::cluster_info::Cluster;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_management::task::TaskChannel;
use databend_common_management::task::TaskMessage;
use databend_common_meta_app::principal::task::AfterTaskInfo;
use databend_common_meta_app::principal::task::EMPTY_TASK_ID;
use databend_common_meta_app::principal::AfterTaskState;
use databend_common_meta_app::principal::ScheduleOptions;
use databend_common_meta_app::principal::ScheduleType;
use databend_common_meta_app::principal::State;
use databend_common_meta_app::principal::Status;
use databend_common_meta_app::principal::Task;
use databend_common_meta_app::principal::TaskRun;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::WarehouseOptions;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_sql::Planner;
use databend_common_users::UserApiProvider;
use databend_common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;
use futures_util::lock::Mutex;
use futures_util::TryStreamExt;
use tokio::sync::mpsc::Receiver;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::interpreters::InterpreterFactory;
use crate::sessions::QueryContext;
use crate::task::meta::TaskMetaHandle;
use crate::task::session::create_session;
use crate::task::session::get_task_user;

pub struct TaskService {
    initialized: AtomicBool,
    interval: u64,
    tenant_id: String,
    node_id: String,
    cluster_id: String,
    meta_handle: TaskMetaHandle,
    _runtime: Arc<Runtime>,
}

impl TaskService {
    pub fn instance() -> Arc<TaskService> {
        GlobalInstance::get()
    }

    pub async fn prepare(&self) -> Result<()> {
        let prepare_key = format!("{}/task_run_prepare/lock", self.tenant_id);
        let _guard = self.meta_handle.acquire(&prepare_key, 0).await?;
        let create_db = "CREATE DATABASE IF NOT EXISTS system_task";
        self.execute_sql(None, create_db).await?;

        let create_table = "CREATE TABLE IF NOT EXISTS system_task.task_run(\
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
        self.execute_sql(None, create_table).await?;

        Ok(())
    }

    pub fn initialized(&self) {
        self.initialized.store(true, Ordering::SeqCst);
    }

    pub fn init(mut task_rx: Receiver<TaskMessage>, cfg: &InnerConfig) -> Result<()> {
        let tenant = cfg.query.tenant_id.clone();
        let meta_client = MetaGrpcClient::try_new(&cfg.meta.to_meta_grpc_client_conf())
            .map_err(|_e| ErrorCode::Internal("Create MetaClient failed for Task"))?;
        let meta_handle = TaskMetaHandle::new(meta_client, cfg.query.node_id.clone());
        let runtime = Arc::new(Runtime::with_worker_threads(
            4,
            Some("task-worker".to_owned()),
        )?);

        let instance = TaskService {
            initialized: AtomicBool::new(false),
            interval: 2,
            tenant_id: cfg.query.tenant_id.tenant_name().to_string(),
            node_id: cfg.query.node_id.clone(),
            cluster_id: cfg.query.cluster_id.clone(),
            meta_handle,
            _runtime: runtime.clone(),
        };
        GlobalInstance::set(Arc::new(instance));

        runtime.clone().try_spawn(async move {
            let task_service = TaskService::instance();
            loop {
                if !task_service.initialized.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                } else {
                    break;
                }
            }
            task_service.prepare().await?;

            let mut scheduled_tasks: HashMap<String, CancellationToken> = HashMap::new();
            let task_mgr = UserApiProvider::instance().task_api(&tenant);

            // If `task_c` is defined as `AFTER task_a, task_b`, then:
            //   - task_after_infos["task_c"] = AfterTaskInfo { afters: ["task_a, task_b"] }
            let mut task_after_infos = HashMap::<String, AfterTaskInfo>::new();
            //   - task_dep_infos["task_a"]["task_c"] = AfterTaskInfo { ... }
            //   - task_dep_infos["task_b"]["task_c"] = AfterTaskInfo { ... }
            let mut task_dep_infos = HashMap::<String, HashMap<String, AfterTaskInfo>>::new();
            // after task state => [task1 name, task2 name], if task succeeded then remove task name on after task state
            let task_deps = Arc::new(Mutex::new(HashMap::<AfterTaskInfo, AfterTaskState>::new()));

            while let Some(task) = task_rx.recv().await {
                match task {
                    TaskMessage::ScheduleTask(mut task) => {
                        debug_assert!(task.schedule_options.is_some());
                        if let Some(schedule_options) = &task.schedule_options {
                            // clean old task if alter
                            if let Some(token) = scheduled_tasks.remove(&task.task_name) {
                                token.cancel();
                            }
                            match task.status {
                                Status::Suspended => continue,
                                Status::Started => ()
                            }

                            let token = CancellationToken::new();
                            let child_token = token.child_token();
                            let task_name = task.task_name.to_string();
                            let task_service = TaskService::instance();

                            task_service.update_or_create_task_run(&TaskRun {
                                task: task.clone(),
                                run_id: Self::make_run_id(),
                                attempt_number: task.suspend_task_after_num_failures.unwrap_or(0) as i32,
                                state: State::Scheduled,
                                scheduled_at: Utc::now(),
                                completed_at: None,
                                error_code: 0,
                                error_message: None,
                                root_task_id: EMPTY_TASK_ID,
                            }).await?;

                            match schedule_options.schedule_type {
                                ScheduleType::IntervalType => {
                                    let task_mgr = task_mgr.clone();
                                    let mut duration = Duration::from_secs(schedule_options.interval.unwrap() as u64);
                                    if let Some(ms) = &schedule_options.milliseconds_interval {
                                        duration += Duration::from_millis(*ms);
                                    }

                                    runtime
                                        .spawn(async move {
                                            task.next_scheduled_at = Some(Utc::now() + duration);
                                            task_mgr.update_task(task.clone()).await??;
                                            loop {
                                                tokio::select! {
                                                    _ = sleep(duration) => {
                                                        let _ = TaskChannel::instance().send(TaskMessage::ExecuteTask(task.clone())).await;
                                                    }
                                                    _ = child_token.cancelled() => {
                                                        break;
                                                    }
                                                }
                                            }
                                            // TODO: log error
                                            Result::Ok(())
                                        });
                                }
                                ScheduleType::CronType => {
                                    // SAFETY: check on CreateTask
                                    let cron_expr = schedule_options.cron.as_ref().unwrap();
                                    let tz = schedule_options.time_zone.as_ref().unwrap().parse::<Tz>().unwrap();
                                    let schedule = Schedule::from_str(cron_expr).unwrap();

                                    runtime
                                        .spawn(async move {
                                            let upcoming = schedule.upcoming(tz);

                                            for next_time in upcoming {
                                                let now = Utc::now();
                                                let duration = (next_time.with_timezone(&Utc) - now)
                                                    .to_std()
                                                    .unwrap_or(Duration::ZERO);

                                                task.next_scheduled_at = Some(Utc::now() + duration);
                                                tokio::select! {
                                                    _ = sleep(duration) => {
                                                        let _ = TaskChannel::instance().send(TaskMessage::ExecuteTask(task.clone())).await;
                                                    }
                                                    _ = child_token.cancelled() => {
                                                        break;
                                                    }
                                                }
                                            }
                                        });
                                }
                            }
                            let _ = scheduled_tasks.insert(task_name, token);
                        }
                    }
                    TaskMessage::ExecuteTask(task) => {
                        let task_name = task.task_name.clone();
                        let task_service = TaskService::instance();

                        // TODO: Meta control query is executed serially through watch key

                        let mut task_run = task_service.lasted_task_run(&task_name).await?
                            .unwrap_or_else(|| TaskRun {
                                task: task.clone(),
                                run_id: Self::make_run_id(),
                                attempt_number: task.suspend_task_after_num_failures.unwrap_or(0) as i32,
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
                        let task_dep_infos = task_dep_infos.clone();
                        let task_deps = task_deps.clone();
                        let owner = Self::get_task_owner(&task, &tenant).await?;
                        runtime
                            .try_spawn(async move {
                                while task_run.attempt_number >= 0 {
                                    let task_result = Self::spawn_task(task.clone(), owner.clone()).await;

                                    match task_result {
                                        Ok(()) => {
                                            task_run.state = State::Succeeded;
                                            task_run.completed_at = Some(Utc::now());
                                            task_service.update_or_create_task_run(&task_run).await?;

                                            if let Some(info) = task_dep_infos.get(&task_name) {
                                                let mut guard = task_deps.lock().await;

                                                for (dep_name, dep_info) in info {
                                                    if let Some(after_state) = guard.get_mut(dep_info) {
                                                        if after_state.completed_task(&task_name) {
                                                            *after_state = AfterTaskState::from(dep_info);
                                                            let dep_task = task_mgr.describe_task(dep_name).await??
                                                                .ok_or_else(|| ErrorCode::UnknownTask(dep_name.clone()))?;

                                                            let _ = TaskChannel::instance().send(TaskMessage::ExecuteTask(dep_task)).await;
                                                        }
                                                    }
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
                                    task_mgr.alter_task(&task.task_name, &AlterTaskOptions::Suspend).await??;
                                }

                                // TODO: log error
                                Result::Ok(())
                            }, None)?;
                    }
                    TaskMessage::DeleteTask(task_name) => {
                        if let Some(deps) = task_dep_infos.get(&task_name) {
                            if !deps.is_empty() {
                                continue;
                                // TODO: return delete failed error
                            }
                        }
                        if let Some(token) = scheduled_tasks.remove(&task_name) {
                            token.cancel();
                        }
                    }
                    TaskMessage::AfterTask(task) => {
                        match task.status {
                            Status::Suspended => continue,
                            Status::Started => (),
                        }
                        // after info
                        if let Some(info) = task_after_infos.remove(&task.task_name) {
                            // dep info
                            for after in info.afters.iter() {
                                if let Some(dep_tasks) = task_dep_infos.get_mut(after) {
                                    dep_tasks.remove(&task.task_name);
                                }
                            }
                            task_deps
                                .lock()
                                .await
                                .remove(&info);
                        }
                        if task.after.is_empty() {
                            continue;
                        }
                        let task_name = task.task_name.clone();
                        let info = AfterTaskInfo::from(&task);
                        // after info
                        task_after_infos.insert(task_name.clone(), info.clone());
                        // dep info
                        for after_task in task.after.iter() {
                            task_dep_infos
                                .entry(after_task.clone())
                                .or_default()
                                .insert(task_name.clone(), info.clone());

                            task_deps
                                .lock()
                                .await
                                .insert(info.clone(), AfterTaskState::from(&info));
                        }
                    }
                }
            }

            // TODO: log error
            Result::Ok(())
        }, None)?;
        Ok(())
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

        if let Some(when_condition) = &task.when_condition {
            let result = task_service
                .execute_sql(Some(user.clone()), &format!("SELECT {when_condition}"))
                .await?;
            let is_met = result
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
                .unwrap_or(false);
            if !is_met {
                return Err(ErrorCode::TaskWhenConditionNotMet(format!(
                    "when condition not met for task: {}",
                    task.task_name
                )));
            }
        }
        task_service
            .execute_sql(Some(user), &task.query_text)
            .await?;

        Ok(())
    }

    pub async fn create_context(&self, other_user: Option<UserInfo>) -> Result<Arc<QueryContext>> {
        let (user, role) = if let Some(other_user) = other_user {
            (other_user, None)
        } else {
            (
                get_task_user(&self.tenant_id, &self.cluster_id),
                Some(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string()),
            )
        };
        let session = create_session(user, role).await?;
        // only need run the sql on the current node
        session.create_query_context_with_cluster(Arc::new(Cluster {
            unassign: false,
            local_id: self.node_id.clone(),
            nodes: vec![],
        }))
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
                .map(|s| format!("'{s}'").replace('\'', "''"))
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

        let mut planner = Planner::new(context.clone());
        let (plan, _) = planner.plan_sql(sql).await?;
        let executor = InterpreterFactory::get(context.clone(), &plan).await?;
        let stream = executor.execute(context).await?;
        stream.try_collect::<Vec<DataBlock>>().await
    }

    fn make_run_id() -> u64 {
        Utc::now().timestamp_millis() as u64
    }
}
