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
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use chrono_tz::Tz;
use cron::Schedule;
use databend_common_ast::ast::AlterTaskOptions;
use databend_common_base::runtime::GlobalQueryRuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::session_type::SessionType;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_management::task::TaskChannel;
use databend_common_management::task::TaskMessage;
use databend_common_meta_app::principal::task::AfterTaskInfo;
use databend_common_meta_app::principal::task::EMPTY_TASK_ID;
use databend_common_meta_app::principal::AfterTaskState;
use databend_common_meta_app::principal::ScheduleType;
use databend_common_meta_app::principal::State;
use databend_common_meta_app::principal::Status;
use databend_common_meta_app::principal::Task;
use databend_common_meta_app::principal::TaskRun;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_sql::Planner;
use databend_common_users::UserApiProvider;
use futures_util::lock::Mutex;
use futures_util::TryStreamExt;
use tokio::sync::mpsc::Receiver;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::interpreters::InterpreterFactory;
use crate::sessions::SessionManager;

pub struct TaskService;

impl TaskService {
    pub fn init(mut task_rx: Receiver<TaskMessage>, tenant: Tenant) -> Result<()> {
        GlobalQueryRuntime::instance()
            .runtime()
            .try_spawn(async move {
                let mut scheduled_tasks: HashMap<String, CancellationToken> = HashMap::new();
                let task_mgr = UserApiProvider::instance().task_api(&tenant);

                // If `task_c` is defined as `AFTER task_a, task_b`, then:
                //   - task_after_infos["task_c"] = AfterTaskInfo { afters: ["task_a, task_b"] }
                let mut task_after_infos = HashMap::<String, AfterTaskInfo>::new();
                //   - task_dep_infos["task_a"]["task_c"] = AfterTaskInfo { ... }
                //   - task_dep_infos["task_b"]["task_c"] = AfterTaskInfo { ... }
                let mut task_dep_infos = HashMap::<String, HashMap<String, AfterTaskInfo>>::new();
                // after task state => [task1 name, task2 name], if task succeeded then remove task name on after task state
                let task_deps= Arc::new(Mutex::new(HashMap::<AfterTaskInfo, AfterTaskState>::new()));

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
                                let task_mgr = task_mgr.clone();

                                task_mgr.update_task_run(TaskRun {
                                    task: task.clone(),
                                    run_id: Self::make_run_id(),
                                    attempt_number: task.suspend_task_after_num_failures.unwrap_or(0) as i32,
                                    state: State::Scheduled,
                                    scheduled_at: Utc::now(),
                                    completed_at: None,
                                    error_code: 0,
                                    error_message: None,
                                    root_task_id: EMPTY_TASK_ID,
                                }).await??;

                                match schedule_options.schedule_type {
                                    ScheduleType::IntervalType => {
                                        let mut duration = Duration::from_secs(schedule_options.interval.unwrap() as u64);
                                        if let Some(ms) = &schedule_options.milliseconds_interval {
                                            duration += Duration::from_millis(*ms);
                                        }

                                        GlobalQueryRuntime::instance()
                                            .runtime()
                                            .spawn(async move {
                                            task.next_scheduled_at = Some(Utc::now() + duration);
                                            loop {
                                                task_mgr.create_task(task.clone(), &CreateOption::CreateOrReplace).await??;
                                                tokio::select! {
                                                    _ = sleep(duration) => {
                                                        let _ = TaskChannel::instance().send(TaskMessage::ExecuteTask(task.clone()));
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

                                        GlobalQueryRuntime::instance()
                                            .runtime()
                                            .spawn(async move {
                                            let mut upcoming = schedule.upcoming(tz);

                                            for next_time in upcoming {
                                                let now = Utc::now();
                                                let duration = (next_time.with_timezone(&Utc) - now)
                                                    .to_std()
                                                    .unwrap_or(Duration::ZERO);

                                                task.next_scheduled_at = Some(Utc::now() + duration);
                                                tokio::select! {
                                                    _ = sleep(duration) => {
                                                        let _ = TaskChannel::instance().send(TaskMessage::ExecuteTask(task.clone()));
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

                            // TODO: Meta control query is executed serially through watch key

                            let mut task_run = task_mgr.lasted_task_run(&task_name).await??
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
                            task_mgr.update_task_run(task_run.clone()).await??;

                            let task_mgr = task_mgr.clone();
                            let tenant = tenant.clone();
                            let task_dep_infos = task_dep_infos.clone();
                            let task_deps = task_deps.clone();
                            let owner = Self::get_task_owner(&task, &tenant).await?;
                            GlobalQueryRuntime::instance()
                                .runtime()
                                .try_spawn(async move {
                                    while task_run.attempt_number >= 0 {
                                        let task_result = Self::spawn_task(task.clone(), owner.clone()).await;

                                        match task_result {
                                            Ok(()) => {
                                                task_run.state = State::Succeeded;
                                                task_run.completed_at = Some(Utc::now());
                                                task_mgr.update_task_run(task_run.clone()).await??;

                                                if let Some(info) = task_dep_infos.get(&task_name) {
                                                    let mut guard = task_deps.lock().await;

                                                    for (dep_name, dep_info) in info {
                                                        if let Some(after_state) = guard.get_mut(dep_info) {
                                                            if after_state.completed_task(&task_name) {
                                                                *after_state = AfterTaskState::from(dep_info);
                                                                let dep_task = task_mgr.describe_task(dep_name).await??
                                                                    .ok_or_else(|| ErrorCode::UnknownTask(dep_name.clone()))?;

                                                                let _ = TaskChannel::instance().send(TaskMessage::ExecuteTask(dep_task));
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
                                                task_mgr.update_task_run(task_run.clone()).await??;
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
                                    continue
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
                            let task_name =  task.task_name.clone();
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
        let session = SessionManager::instance()
            .create_session(SessionType::Local)
            .await?;
        session.set_authed_user(user, None).await?;
        let context = Arc::new(session).create_query_context().await?;

        let mut planner = Planner::new(context.clone());
        let (plan, _) = planner.plan_sql(&task.query_text).await?;
        let executor = InterpreterFactory::get(context.clone(), &plan).await?;
        let stream = executor.execute(context).await?;
        let _ = stream.try_collect::<Vec<DataBlock>>().await?;

        Ok(())
    }

    fn make_run_id() -> u64 {
        Utc::now().timestamp_millis() as u64
    }
}
