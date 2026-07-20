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

use std::str::FromStr;
use std::sync::Arc;

use chrono::Utc;
use chrono_tz::Tz;
use cron::Schedule;
use databend_common_ast::ast::AlterTaskOptions;
use databend_common_ast::ast::ScheduleOptions;
use databend_common_ast::ast::TaskSql;
use databend_common_meta_api::fetch_id;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::kv_pb_api::UpsertPB;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::principal::ScheduleType;
use databend_common_meta_app::principal::Status;
use databend_common_meta_app::principal::Task;
use databend_common_meta_app::principal::TaskIdent;
use databend_common_meta_app::principal::task;
use databend_common_meta_app::principal::task::EMPTY_TASK_ID;
use databend_common_meta_app::principal::task::TaskMessage;
use databend_common_meta_app::principal::task::TaskSql as MetaTaskSql;
use databend_common_meta_app::principal::task_message_ident::TaskMessageIdent;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::DirName;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::types::MatchSeq;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::With;
use futures::TryStreamExt;

use crate::task::errors::TaskApiError;
use crate::task::errors::TaskError;

#[derive(Clone)]
pub struct TaskMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    tenant: Tenant,
}

impl TaskMgr {
    pub fn create(kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>, tenant: &Tenant) -> Self {
        TaskMgr {
            kv_api,
            tenant: tenant.clone(),
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn create_task(
        &self,
        mut task: Task,
        create_option: &CreateOption,
    ) -> Result<Result<(), TaskError>, TaskApiError> {
        task.created_at = Utc::now();

        self.create_task_inner(task, create_option, false, None)
            .await
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn update_task(&self, task: Task) -> Result<Result<bool, TaskError>, TaskApiError> {
        let key = TaskIdent::new(&self.tenant, &task.task_name);
        loop {
            let Some(seq_task) = self.kv_api.get_pb(&key).await? else {
                return Ok(Err(TaskError::NotFound {
                    tenant: self.tenant.tenant_name().to_string(),
                    name: task.task_name,
                    context: "while updating task schedule".to_string(),
                }));
            };

            let seq = seq_task.seq;
            let mut current_task = seq_task.data;
            if current_task.task_id != task.task_id
                || current_task.status != Status::Started
                || current_task.schedule_options != task.schedule_options
                || current_task.warehouse_options != task.warehouse_options
            {
                return Ok(Ok(false));
            }

            current_task.next_scheduled_at = task.next_scheduled_at;
            if current_task.updated_at < task.updated_at {
                current_task.updated_at = task.updated_at;
            }

            let req = UpsertPB::update(key.clone(), current_task).with(MatchSeq::Exact(seq));
            let res = self.kv_api.upsert_pb(&req).await?;
            if res.is_changed() {
                return Ok(Ok(true));
            }
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn execute_task(
        &self,
        task_name: &str,
    ) -> Result<Result<(), TaskError>, TaskApiError> {
        let key = TaskIdent::new(&self.tenant, task_name);
        let Some(task) = self.kv_api.get_pb(&key).await? else {
            return Ok(Err(TaskError::NotFound {
                tenant: self.tenant.tenant_name().to_string(),
                name: task_name.to_string(),
                context: "while execute task".to_string(),
            }));
        };
        self.send(TaskMessage::ExecuteTask(Task::clone(&task)))
            .await?;

        Ok(Ok(()))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn alter_task(
        &self,
        task_name: &str,
        alter_options: &AlterTaskOptions,
    ) -> Result<Result<(), TaskError>, TaskApiError> {
        let key = TaskIdent::new(&self.tenant, task_name);
        let Some(task) = self.kv_api.get_pb(&key).await? else {
            return Ok(Err(TaskError::NotFound {
                tenant: self.tenant.tenant_name().to_string(),
                name: task_name.to_string(),
                context: "while alter task".to_string(),
            }));
        };
        let mut new_task = Task::clone(&task);
        new_task.updated_at = Utc::now();

        match alter_options {
            AlterTaskOptions::Resume => {
                new_task.status = Status::Started;
            }
            AlterTaskOptions::Suspend => {
                new_task.last_suspended_at = Some(Utc::now());
                new_task.status = Status::Suspended;
            }
            AlterTaskOptions::Set {
                schedule,
                comments,
                warehouse,
                suspend_task_after_num_failures,
                error_integration,
                session_parameters,
            } => {
                if let Some(schedule) = schedule {
                    new_task.schedule_options = Some(Self::make_schedule_options(schedule.clone()));
                }
                if let Some(comments) = comments {
                    new_task.comment = Some(comments.clone());
                }
                if let Some(warehouse) = warehouse {
                    new_task.warehouse_options =
                        Some(Self::make_warehouse_options(Some(warehouse.clone())));
                }
                if let Some(suspend_task_after_num_failures) = suspend_task_after_num_failures {
                    new_task.suspend_task_after_num_failures =
                        Some(*suspend_task_after_num_failures);
                }
                if let Some(error_integration) = error_integration {
                    new_task.error_integration = Some(error_integration.clone());
                }
                if let Some(session_parameters) = session_parameters {
                    new_task.session_params = session_parameters.clone();
                }
            }
            AlterTaskOptions::Unset { .. } => {
                todo!()
            }
            AlterTaskOptions::ModifyAs(sql) => {
                new_task.task_sql = Self::make_task_sql(sql);
            }
            AlterTaskOptions::ModifyWhen(sql) => {
                new_task.when_condition = Some(sql.to_string());
            }
            AlterTaskOptions::AddAfter(afters) => {
                if new_task.schedule_options.is_some() {
                    return Ok(Err(TaskError::ScheduleAndAfterConflict {
                        tenant: self.tenant.tenant_name().to_string(),
                        name: task_name.to_string(),
                    }));
                }
                for after in afters {
                    if new_task.after.contains(after) {
                        continue;
                    }
                    new_task.after.push(after.clone());
                }
            }
            AlterTaskOptions::RemoveAfter(afters) => {
                if new_task.schedule_options.is_some() {
                    return Ok(Err(TaskError::ScheduleAndAfterConflict {
                        tenant: self.tenant.tenant_name().to_string(),
                        name: task_name.to_string(),
                    }));
                }
                new_task.after.retain(|task| !afters.contains(task));
            }
        }
        let pre_schedule_message =
            Self::make_schedule_cancel_message_for_warehouse_move(&task, &new_task);
        if let Err(e) = self
            .create_task_inner(
                new_task,
                &CreateOption::CreateOrReplace,
                false,
                pre_schedule_message,
            )
            .await?
        {
            return Ok(Err(TaskError::NotFound {
                tenant: self.tenant.tenant_name().to_string(),
                name: task_name.to_string(),
                context: format!("while alter task: {}", e),
            }));
        }

        Ok(Ok(()))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn describe_task(
        &self,
        task_name: &str,
    ) -> Result<Result<Option<Task>, TaskError>, TaskApiError> {
        let key = TaskIdent::new(&self.tenant, task_name);
        let task = self.kv_api.get_pb(&key).await?;

        Ok(Ok(task.map(|task| Task::clone(&task))))
    }

    #[allow(clippy::useless_asref)]
    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn drop_task(&self, task_name: &str) -> Result<Option<Task>, MetaError> {
        let key = TaskIdent::new(&self.tenant, task_name);
        let req = UpsertPB::delete(key).with(MatchSeq::GE(1));
        let res = self.kv_api.upsert_pb(&req).await?;

        if res.is_changed() {
            let Some(task) = res.prev.as_ref().map(|prev| Task::clone(prev)) else {
                return Ok(None);
            };
            self.send(TaskMessage::DeleteTask(
                task_name.to_string(),
                task.warehouse_options.clone(),
                Some(task.task_id),
            ))
            .await?;

            Ok(Some(task))
        } else {
            Ok(None)
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn list_task(&self) -> Result<Vec<Task>, MetaError> {
        let key = DirName::new(TaskIdent::new(&self.tenant, ""));
        let strm = self
            .kv_api
            .list_pb_values(ListOptions::unlimited(&key))
            .await?;

        strm.try_collect().await
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn send(&self, message: TaskMessage) -> Result<(), MetaError> {
        let key = TaskMessageIdent::new(&self.tenant, message.key());
        let seq = MatchSeq::from(CreateOption::CreateOrReplace);

        let req = UpsertPB::insert(key, message).with(seq);
        let _ = self.kv_api.upsert_pb(&req).await?;

        Ok(())
    }

    /// mark the corresponding execute task as accepted and delete it from the queue
    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn accept(&self, key: &TaskMessageIdent) -> Result<bool, MetaError> {
        let req = UpsertPB::delete(key.clone()).with(MatchSeq::GE(1));
        let change = self.kv_api.upsert_pb(&req).await?;

        Ok(change.is_changed())
    }

    async fn create_task_inner(
        &self,
        mut task: Task,
        create_option: &CreateOption,
        without_schedule: bool,
        pre_schedule_message: Option<TaskMessage>,
    ) -> Result<Result<(), TaskError>, TaskApiError> {
        assert!(task.after.is_empty() || task.schedule_options.is_none());
        // check
        if let Some(schedule_options) = &task.schedule_options {
            match schedule_options.schedule_type {
                ScheduleType::IntervalType => (),
                ScheduleType::CronType => {
                    if let Some(tz) = &schedule_options.time_zone {
                        if let Err(e) = tz.parse::<Tz>() {
                            return Ok(Err(TaskError::InvalidTimezone {
                                tenant: self.tenant.tenant_name().to_string(),
                                name: task.task_name.to_string(),
                                reason: e.to_string(),
                            }));
                        }
                    }
                    if let Err(e) = Schedule::from_str(schedule_options.cron.as_ref().unwrap()) {
                        return Ok(Err(TaskError::InvalidCron {
                            tenant: self.tenant.tenant_name().to_string(),
                            name: task.task_name.to_string(),
                            reason: e.to_string(),
                        }));
                    }
                }
            }
        }

        let seq = MatchSeq::from(*create_option);
        if task.task_id == EMPTY_TASK_ID {
            task.task_id = fetch_id(self.kv_api.as_ref(), IdGenerator::task_id()).await?;
        }

        let key = TaskIdent::new(&self.tenant, &task.task_name);
        let req = UpsertPB::insert(key, task.clone()).with(seq);
        let res = self.kv_api.upsert_pb(&req).await?;

        if let CreateOption::Create = create_option {
            if res.prev.is_some() {
                let err = TaskError::Exists {
                    tenant: self.tenant.tenant_name().to_string(),
                    name: task.task_name.to_string(),
                    reason: "".to_string(),
                };
                return Ok(Err(err));
            }
        }
        if !res.is_changed() {
            return Ok(Ok(()));
        }
        if let Some(message) = pre_schedule_message {
            self.send(message).await?;
        }
        if !task.after.is_empty() {
            self.send(TaskMessage::AfterTask(task)).await?;
        } else if task.schedule_options.is_some() && !without_schedule {
            self.send(TaskMessage::ScheduleTask(task)).await?;
        }

        Ok(Ok(()))
    }

    pub fn make_schedule_options(opt: ScheduleOptions) -> task::ScheduleOptions {
        match opt {
            ScheduleOptions::IntervalSecs(secs, ms) => {
                task::ScheduleOptions {
                    interval: Some(secs as i32),
                    // none if ms is 0, else some ms
                    milliseconds_interval: if ms == 0 { None } else { Some(ms) },
                    cron: None,
                    time_zone: None,
                    schedule_type: task::ScheduleType::IntervalType,
                }
            }

            ScheduleOptions::CronExpression(expr, timezone) => task::ScheduleOptions {
                interval: None,
                milliseconds_interval: None,
                cron: Some(expr),
                time_zone: timezone,
                schedule_type: task::ScheduleType::CronType,
            },
        }
    }

    pub fn make_warehouse_options(opt: Option<String>) -> task::WarehouseOptions {
        task::WarehouseOptions {
            warehouse: opt,
            using_warehouse_size: None,
        }
    }

    fn make_schedule_cancel_message_for_warehouse_move(
        old_task: &Task,
        new_task: &Task,
    ) -> Option<TaskMessage> {
        if old_task.status != Status::Started
            || old_task.schedule_options.is_none()
            || new_task.schedule_options.is_none()
            || old_task.warehouse_options == new_task.warehouse_options
        {
            return None;
        }

        let mut cancel_task = old_task.clone();
        cancel_task.status = Status::Suspended;
        Some(TaskMessage::ScheduleTask(cancel_task))
    }

    pub fn make_task_sql(sql: &TaskSql) -> MetaTaskSql {
        match sql {
            TaskSql::SingleStatement(stmt) => MetaTaskSql::Sql(stmt.clone()),
            TaskSql::ScriptBlock(sqls) => MetaTaskSql::Script(sqls.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::Utc;
    use databend_common_meta_app::principal::ScheduleOptions;
    use databend_common_meta_app::principal::ScheduleType;
    use databend_common_meta_app::principal::Status;
    use databend_common_meta_app::principal::Task;
    use databend_common_meta_app::principal::WarehouseOptions;
    use databend_common_meta_app::principal::task::EMPTY_TASK_ID;
    use databend_common_meta_app::principal::task::TaskMessage;
    use databend_common_meta_app::principal::task::TaskSql as MetaTaskSql;

    use super::TaskMgr;

    #[test]
    fn test_make_schedule_cancel_message_for_warehouse_move() {
        let old_task = test_task(Status::Started, Some("old_wh"));
        let new_task = test_task(Status::Started, Some("new_wh"));

        let Some(TaskMessage::ScheduleTask(cancel_task)) =
            TaskMgr::make_schedule_cancel_message_for_warehouse_move(&old_task, &new_task)
        else {
            panic!("expected schedule cancel message");
        };

        assert_eq!(cancel_task.task_name, old_task.task_name);
        assert_eq!(cancel_task.status, Status::Suspended);
        assert_eq!(cancel_task.schedule_options, old_task.schedule_options);
        assert_eq!(cancel_task.warehouse_options, old_task.warehouse_options);
    }

    #[test]
    fn test_make_schedule_cancel_message_requires_started_scheduled_warehouse_move() {
        let old_task = test_task(Status::Started, Some("old_wh"));

        assert!(
            TaskMgr::make_schedule_cancel_message_for_warehouse_move(
                &old_task,
                &test_task(Status::Started, Some("old_wh"))
            )
            .is_none()
        );

        assert!(
            TaskMgr::make_schedule_cancel_message_for_warehouse_move(
                &test_task(Status::Suspended, Some("old_wh")),
                &test_task(Status::Suspended, Some("new_wh"))
            )
            .is_none()
        );

        let mut new_task = test_task(Status::Started, Some("new_wh"));
        new_task.schedule_options = None;
        assert!(
            TaskMgr::make_schedule_cancel_message_for_warehouse_move(&old_task, &new_task)
                .is_none()
        );
    }

    fn test_task(status: Status, warehouse: Option<&str>) -> Task {
        let now = Utc::now();

        Task {
            task_id: EMPTY_TASK_ID,
            task_name: "task_1".to_string(),
            task_sql: MetaTaskSql::Sql("select 1".to_string()),
            when_condition: None,
            after: vec![],
            comment: None,
            owner: "account_admin".to_string(),
            owner_user: "root".to_string(),
            schedule_options: Some(ScheduleOptions {
                interval: Some(60),
                cron: None,
                time_zone: None,
                schedule_type: ScheduleType::IntervalType,
                milliseconds_interval: None,
            }),
            warehouse_options: Some(WarehouseOptions {
                warehouse: warehouse.map(String::from),
                using_warehouse_size: None,
            }),
            next_scheduled_at: None,
            suspend_task_after_num_failures: None,
            error_integration: None,
            status,
            created_at: now,
            updated_at: now,
            last_suspended_at: None,
            session_params: BTreeMap::new(),
        }
    }
}
