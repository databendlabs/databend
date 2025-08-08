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
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::kv_pb_api::UpsertPB;
use databend_common_meta_api::txn_cond_eq_seq;
use databend_common_meta_api::txn_op_del;
use databend_common_meta_api::util::txn_put_pb;
use databend_common_meta_api::SchemaApi;
use databend_common_meta_app::principal::task;
use databend_common_meta_app::principal::task::TaskMessage;
use databend_common_meta_app::principal::task::TaskSucceededStateValue;
use databend_common_meta_app::principal::task_dependent_ident::TaskDependentIdent;
use databend_common_meta_app::principal::task_message_ident::TaskMessageIdent;
use databend_common_meta_app::principal::task_state_ident::TaskSucceededStateIdent;
use databend_common_meta_app::principal::ScheduleType;
use databend_common_meta_app::principal::Status;
use databend_common_meta_app::principal::Task;
use databend_common_meta_app::principal::TaskIdent;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_types::txn_condition::Target;
use databend_common_meta_types::ConditionResult;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::TxnCondition;
use databend_common_meta_types::TxnOp;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::With;
use futures::StreamExt;
use futures::TryStreamExt;
use seq_marked::SeqValue;

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

        self.create_task_inner(task, create_option, false).await
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn update_task(&self, task: Task) -> Result<Result<(), TaskError>, TaskApiError> {
        self.create_task_inner(task, &CreateOption::CreateOrReplace, true)
            .await
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
        let mut task = Task::clone(&task);
        task.updated_at = Utc::now();

        match alter_options {
            AlterTaskOptions::Resume => {
                task.status = Status::Started;
            }
            AlterTaskOptions::Suspend => {
                task.last_suspended_at = Some(Utc::now());
                task.status = Status::Suspended;
            }
            AlterTaskOptions::Set {
                schedule,
                comments,
                warehouse,
                suspend_task_after_num_failures,
                error_integration,
                session_parameters,
            } => {
                task.schedule_options = schedule.clone().map(Self::make_schedule_options);
                task.comment = comments.clone();
                task.warehouse_options = Some(Self::make_warehouse_options(warehouse.clone()));
                task.suspend_task_after_num_failures = *suspend_task_after_num_failures;
                task.error_integration = error_integration.clone();
                if let Some(session_parameters) = session_parameters {
                    task.session_params = session_parameters.clone();
                }
            }
            AlterTaskOptions::Unset { .. } => {
                todo!()
            }
            AlterTaskOptions::ModifyAs(sql) => {
                task.query_text = sql.to_string();
            }
            AlterTaskOptions::ModifyWhen(sql) => {
                task.when_condition = Some(sql.to_string());
            }
            AlterTaskOptions::AddAfter(afters) => {
                if task.schedule_options.is_some() {
                    return Ok(Err(TaskError::ScheduleAndAfterConflict {
                        tenant: self.tenant.tenant_name().to_string(),
                        name: task_name.to_string(),
                    }));
                }
                return self.add_after(&task.task_name, afters).await;
            }
            AlterTaskOptions::RemoveAfter(afters) => {
                if task.schedule_options.is_some() {
                    return Ok(Err(TaskError::ScheduleAndAfterConflict {
                        tenant: self.tenant.tenant_name().to_string(),
                        name: task_name.to_string(),
                    }));
                }
                return self.remove_after(&task.task_name, afters).await;
            }
        }
        if let Err(e) = self
            .create_task_inner(task, &CreateOption::CreateOrReplace, false)
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
        let strm = self.kv_api.list_pb_values(&key).await?;

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

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn add_after(
        &self,
        task_name: &str,
        new_afters: &[String],
    ) -> Result<Result<(), TaskError>, TaskApiError> {
        let mut request = TxnRequest::new(vec![], vec![]);
        let after_dependent_ident = TaskDependentIdent::new_after(&self.tenant, task_name);

        let after_seq_deps = self.kv_api.get_pb(&after_dependent_ident).await?;
        request.condition.push(txn_cond_eq_seq(
            &after_dependent_ident,
            after_seq_deps.seq(),
        ));

        let mut after_deps = after_seq_deps.unwrap_or_default();
        after_deps.0.extend(new_afters.iter().cloned());
        request
            .if_then
            .push(txn_put_pb(&after_dependent_ident, &after_deps)?);

        let before_dependent_idents = new_afters
            .iter()
            .map(|after| TaskDependentIdent::new_before(&self.tenant, after));
        for (before_dependent_ident, before_seq_deps) in
            self.kv_api.get_pb_vec(before_dependent_idents).await?
        {
            request.condition.push(txn_cond_eq_seq(
                &before_dependent_ident,
                before_seq_deps.seq(),
            ));

            let mut deps = before_seq_deps.unwrap_or_default();
            deps.0.insert(task_name.to_string());
            request
                .if_then
                .push(txn_put_pb(&before_dependent_ident, &deps)?);
        }
        let reply = self.kv_api.transaction(request).await?;

        if !reply.success {
            return Err(TaskApiError::SimultaneousUpdateTaskAfter {
                task_name: task_name.to_string(),
                after: new_afters.join(","),
            });
        }

        Ok(Ok(()))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn remove_after(
        &self,
        task_name: &str,
        remove_afters: &[String],
    ) -> Result<Result<(), TaskError>, TaskApiError> {
        let mut request = TxnRequest::new(vec![], vec![]);

        let after_dependent_ident = TaskDependentIdent::new_after(&self.tenant, task_name);
        let after_seq_deps = self.kv_api.get_pb(&after_dependent_ident).await?;
        request.condition.push(txn_cond_eq_seq(
            &after_dependent_ident,
            after_seq_deps.seq(),
        ));

        if let Some(mut deps) = after_seq_deps {
            for remove_after in remove_afters {
                deps.0.remove(remove_after);
            }
            request
                .if_then
                .push(txn_put_pb(&after_dependent_ident, &deps)?);
        }

        let before_dependent_idents = remove_afters
            .iter()
            .map(|after| TaskDependentIdent::new_before(&self.tenant, after));
        for (before_dependent_ident, before_seq_deps) in
            self.kv_api.get_pb_vec(before_dependent_idents).await?
        {
            request.condition.push(txn_cond_eq_seq(
                &before_dependent_ident,
                before_seq_deps.seq(),
            ));

            if let Some(mut deps) = before_seq_deps {
                deps.0.remove(task_name);
                request
                    .if_then
                    .push(txn_put_pb(&before_dependent_ident, &deps)?);
            }
        }
        let reply = self.kv_api.transaction(request).await?;

        if !reply.success {
            return Err(TaskApiError::SimultaneousUpdateTaskAfter {
                task_name: task_name.to_string(),
                after: remove_afters.join(","),
            });
        }

        Ok(Ok(()))
    }

    // Tips: Task Before only cleans up when dropping a task
    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn clean_task_state_and_dependents(
        &self,
        task_name: &str,
    ) -> Result<Result<(), TaskError>, TaskApiError> {
        let mut request = TxnRequest::new(vec![], vec![]);

        let task_after_ident = TaskDependentIdent::new_after(&self.tenant, task_name);
        let task_before_ident = TaskDependentIdent::new_before(&self.tenant, task_name);

        let mut target_idents = Vec::new();
        if let Some(task_after_dependent) = self.kv_api.get(&task_after_ident).await? {
            target_idents.extend(task_after_dependent.0.into_iter().map(|dependent_target| {
                TaskDependentIdent::new_before(&self.tenant, dependent_target)
            }));
        }
        if let Some(task_before_dependent) = self.kv_api.get(&task_before_ident).await? {
            target_idents.extend(task_before_dependent.0.into_iter().map(|dependent_target| {
                TaskDependentIdent::new_after(&self.tenant, dependent_target)
            }));
        }
        for (target_ident, seq_dep) in self.kv_api.get_pb_vec(target_idents).await? {
            request
                .condition
                .push(txn_cond_eq_seq(&target_ident, seq_dep.seq()));

            if let Some(mut deps) = seq_dep {
                deps.0.remove(task_name);
                request.if_then.push(txn_put_pb(&target_ident, &deps)?);
            }
        }
        request.if_then.push(TxnOp::delete(
            TaskDependentIdent::new_before(&self.tenant, task_name).to_string_key(),
        ));
        request.if_then.push(TxnOp::delete(
            TaskDependentIdent::new_after(&self.tenant, task_name).to_string_key(),
        ));
        let mut stream = self
            .kv_api
            .list_pb_keys(&DirName::new(TaskSucceededStateIdent::new(
                &self.tenant,
                task_name,
                "",
            )))
            .await?;

        while let Some(result) = stream.next().await {
            request.if_then.push(TxnOp::delete(result?.to_string_key()))
        }
        let _ = self.kv_api.transaction(request).await?;

        Ok(Ok(()))
    }

    /// Marks the given task as succeeded, and checks all tasks that depend on it.
    ///
    /// For each task that depends on the completed task (`task_name`), we check if all its
    /// predecessor tasks are also succeeded. If so, we mark the dependent task as *not succeeded*
    /// to prevent premature execution. Otherwise, we record the dependent task as *ready*
    /// for further processing.
    ///
    /// # Arguments
    /// - `task_name`: The name of the task that has just completed successfully.
    ///
    /// # Returns
    /// - `Vec<String>`: A list of dependent task names that are ready to proceed.
    ///
    /// # Behavior
    /// Assume:
    /// - `a` = given `task_name` (the completed task)
    /// - `b` = a task that has `a` in its `AFTER` list (i.e., depends on `a`)
    /// - `c` = other tasks in `b`'s `AFTER` list (other dependencies of `b`)
    ///
    /// 1. Retrieves all tasks (`b`) that have `a` in their `AFTER` list.
    /// 2. For each `b`:
    ///     - Check whether all its dependencies (`a` + `c`) have succeeded.
    ///     - If all dependencies are complete:
    ///         - Add `b` to the ready list.
    ///         - Set the status of its dependencies (`a` + `c`) to `not succeeded`.
    ///     - If not all dependencies of `b` are complete:
    ///         - Only mark `a` as succeeded.
    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn get_next_ready_tasks(
        &self,
        task_name: &str,
    ) -> Result<Result<Vec<String>, TaskError>, TaskApiError> {
        let task_before_ident = TaskDependentIdent::new_before(&self.tenant, task_name);

        let Some(task_before_dependent) = self.kv_api.get_pb(&task_before_ident).await? else {
            return Ok(Ok(Vec::new()));
        };
        let mut ready_tasks = Vec::new();

        let target_after_idents = task_before_dependent
            .0
            .iter()
            .map(|before| TaskDependentIdent::new_after(&self.tenant, before));
        for (target_after_ident, target_after_dependent) in
            self.kv_api.get_pb_vec(target_after_idents).await?
        {
            let Some(target_after_dependent) = target_after_dependent else {
                continue;
            };
            let target_after = &target_after_ident.name().source;
            let this_task_to_target_state =
                TaskSucceededStateIdent::new(&self.tenant, task_name, target_after);
            let mut request = TxnRequest::new(vec![], vec![]).with_else(vec![txn_put_pb(
                &this_task_to_target_state,
                &TaskSucceededStateValue,
            )?]);

            for before_target_after in target_after_dependent.0.iter() {
                let task_ident =
                    TaskSucceededStateIdent::new(&self.tenant, before_target_after, target_after);
                // Only care about the predecessors of this task's successor tasks, excluding this task itself.
                if before_target_after != task_name {
                    request.condition.push(TxnCondition {
                        key: task_ident.to_string_key(),
                        expected: ConditionResult::Gt as i32,
                        target: Some(Target::Seq(0)),
                    });
                }
                request.if_then.push(txn_op_del(&task_ident));
            }
            let reply = self.kv_api.transaction(request).await?;

            if reply.success {
                ready_tasks.push(target_after_ident.name().source.clone());
            }
        }
        Ok(Ok(ready_tasks))
    }

    async fn create_task_inner(
        &self,
        task: Task,
        create_option: &CreateOption,
        without_schedule: bool,
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
        if !task.after.is_empty() {
            if let Err(err) = self.add_after(&task.task_name, &task.after).await? {
                return Ok(Err(err));
            }
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
}
