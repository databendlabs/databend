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

use std::sync::Arc;
use std::time::Duration;

use databend_common_ast::ast::AlterTaskOptions;
use databend_common_ast::ast::ScheduleOptions;
use databend_common_ast::ast::TaskSql;
use databend_common_cloud_control::client_config::ClientConfig;
use databend_common_cloud_control::client_config::build_client_config;
use databend_common_cloud_control::client_config::make_request;
use databend_common_cloud_control::cloud_api::CloudControlApiProvider;
use databend_common_cloud_control::pb;
use databend_common_cloud_control::pb::AlterTaskRequest;
use databend_common_cloud_control::pb::CreateTaskRequest;
use databend_common_cloud_control::pb::DescribeTaskRequest;
use databend_common_cloud_control::pb::DropTaskRequest;
use databend_common_cloud_control::pb::ExecuteTaskRequest;
use databend_common_cloud_control::pb::ShowTasksRequest;
use databend_common_cloud_control::pb::WarehouseOptions;
use databend_common_cloud_control::pb::alter_task_request::AlterTaskType;
use databend_common_cloud_control::pb::schedule_options::ScheduleType;
use databend_common_cloud_control::task_utils::Task as CloudTask;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_management::task::TaskMgr;
use databend_common_meta_app::principal::Status;
use databend_common_meta_app::principal::task::EMPTY_TASK_ID;
use databend_common_settings::DefaultSettings;
use databend_common_settings::SettingScope;
use databend_common_sql::plans::AlterTaskPlan;
use databend_common_sql::plans::CreateTaskPlan;
use databend_common_sql::plans::DescribeTaskPlan;
use databend_common_sql::plans::DropTaskPlan;
use databend_common_sql::plans::ExecuteTaskPlan;
use databend_common_sql::plans::ShowTasksPlan;
use databend_common_users::UserApiProvider;
use itertools::Itertools;

use crate::TaskContext;
use crate::system_tables::private_task_to_cloud_task;

pub struct TaskInterpreterManager;

impl TaskInterpreterManager {
    pub fn build<C: TaskContext>(ctx: &C) -> Result<impl TaskInterpreter<C>> {
        if GlobalConfig::instance().task.on {
            LicenseManagerSwitch::instance()
                .check_enterprise_enabled(ctx.license_key(), Feature::PrivateTask)?;
            return Ok(TaskInterpreterImpl::Private(PrivateTaskInterpreter));
        }

        Ok(TaskInterpreterImpl::Cloud(CloudTaskInterpreter))
    }
}

enum TaskInterpreterImpl {
    Cloud(CloudTaskInterpreter),
    Private(PrivateTaskInterpreter),
}

#[async_trait::async_trait]
pub trait TaskInterpreter<C: TaskContext> {
    async fn create_task(&self, ctx: &Arc<C>, plan: &CreateTaskPlan) -> Result<()>;

    async fn execute_task(&self, ctx: &Arc<C>, plan: &ExecuteTaskPlan) -> Result<()>;

    async fn alter_task(&self, ctx: &Arc<C>, plan: &AlterTaskPlan) -> Result<()>;

    async fn describe_task(
        &self,
        ctx: &Arc<C>,
        plan: &DescribeTaskPlan,
    ) -> Result<Option<CloudTask>>;

    async fn drop_task(&self, ctx: &Arc<C>, plan: &DropTaskPlan) -> Result<()>;

    async fn show_tasks(&self, ctx: &Arc<C>, plan: &ShowTasksPlan) -> Result<Vec<CloudTask>>;
}

#[async_trait::async_trait]
impl<C: TaskContext> TaskInterpreter<C> for TaskInterpreterImpl {
    async fn create_task(&self, ctx: &Arc<C>, plan: &CreateTaskPlan) -> Result<()> {
        match self {
            TaskInterpreterImpl::Cloud(interpreter) => interpreter.create_task(ctx, plan).await,
            TaskInterpreterImpl::Private(interpreter) => interpreter.create_task(ctx, plan).await,
        }
    }

    async fn execute_task(&self, ctx: &Arc<C>, plan: &ExecuteTaskPlan) -> Result<()> {
        match self {
            TaskInterpreterImpl::Cloud(interpreter) => interpreter.execute_task(ctx, plan).await,
            TaskInterpreterImpl::Private(interpreter) => interpreter.execute_task(ctx, plan).await,
        }
    }

    async fn alter_task(&self, ctx: &Arc<C>, plan: &AlterTaskPlan) -> Result<()> {
        match self {
            TaskInterpreterImpl::Cloud(interpreter) => interpreter.alter_task(ctx, plan).await,
            TaskInterpreterImpl::Private(interpreter) => interpreter.alter_task(ctx, plan).await,
        }
    }

    async fn describe_task(
        &self,
        ctx: &Arc<C>,
        plan: &DescribeTaskPlan,
    ) -> Result<Option<CloudTask>> {
        match self {
            TaskInterpreterImpl::Cloud(interpreter) => interpreter.describe_task(ctx, plan).await,
            TaskInterpreterImpl::Private(interpreter) => interpreter.describe_task(ctx, plan).await,
        }
    }

    async fn drop_task(&self, ctx: &Arc<C>, plan: &DropTaskPlan) -> Result<()> {
        match self {
            TaskInterpreterImpl::Cloud(interpreter) => interpreter.drop_task(ctx, plan).await,
            TaskInterpreterImpl::Private(interpreter) => interpreter.drop_task(ctx, plan).await,
        }
    }

    async fn show_tasks(&self, ctx: &Arc<C>, plan: &ShowTasksPlan) -> Result<Vec<CloudTask>> {
        match self {
            TaskInterpreterImpl::Cloud(interpreter) => interpreter.show_tasks(ctx, plan).await,
            TaskInterpreterImpl::Private(interpreter) => interpreter.show_tasks(ctx, plan).await,
        }
    }
}

struct CloudTaskInterpreter;

impl CloudTaskInterpreter {
    fn validate_create_session_parameters(plan: &CreateTaskPlan) -> Result<()> {
        for key in plan.session_parameters.keys() {
            DefaultSettings::check_setting_scope(key, SettingScope::Session)?;
        }

        Ok(())
    }

    fn validate_alter_session_parameters(plan: &AlterTaskPlan) -> Result<()> {
        if let AlterTaskOptions::Set {
            session_parameters: Some(session_parameters),
            ..
        } = &plan.alter_options
        {
            for key in session_parameters.keys() {
                DefaultSettings::check_setting_scope(key, SettingScope::Session)?;
            }
        }

        Ok(())
    }

    fn build_create_request<C: TaskContext>(ctx: &C, plan: &CreateTaskPlan) -> CreateTaskRequest {
        let plan = plan.clone();
        let mut req = CreateTaskRequest {
            task_name: plan.task_name,
            tenant_id: plan.tenant.tenant_name().to_string(),
            query_text: String::new(),
            owner: ctx.current_role_identity(),
            comment: plan.comment,
            schedule_options: plan.schedule_opts.map(make_schedule_options),
            warehouse_options: Some(make_warehouse_options(plan.warehouse)),
            error_integration: plan.error_integration,
            task_sql_type: 0,
            suspend_task_after_num_failures: plan.suspend_task_after_num_failures.map(|x| x as i32),
            if_not_exist: plan.create_option.if_not_exist(),
            after: plan.after,
            when_condition: plan.when_condition,
            session_parameters: plan.session_parameters,
            script_sql: None,
        };

        match plan.sql {
            TaskSql::SingleStatement(stmt) => {
                req.task_sql_type = i32::from(pb::TaskSqlType::Sql);
                req.query_text = stmt;
            }
            TaskSql::ScriptBlock(ref sqls) => {
                req.task_sql_type = i32::from(pb::TaskSqlType::Script);
                req.query_text = format!("{}", plan.sql);
                req.script_sql = Some(pb::ScriptSql { sqls: sqls.clone() });
            }
        }

        req
    }

    fn build_alter_request<C: TaskContext>(ctx: &C, plan: &AlterTaskPlan) -> AlterTaskRequest {
        let plan = plan.clone();
        let mut req = AlterTaskRequest {
            task_name: plan.task_name,
            tenant_id: plan.tenant.tenant_name().to_string(),
            owner: ctx.current_role_identity(),
            alter_task_type: 0,
            if_exist: plan.if_exists,
            error_integration: None,
            task_sql_type: 0,
            query_text: None,
            comment: None,
            schedule_options: None,
            warehouse_options: None,
            suspend_task_after_num_failures: None,
            when_condition: None,
            add_after: vec![],
            remove_after: vec![],
            set_session_parameters: false,
            session_parameters: Default::default(),
            script_sql: None,
        };

        match plan.alter_options {
            AlterTaskOptions::Resume => req.alter_task_type = AlterTaskType::Resume as i32,
            AlterTaskOptions::Suspend => req.alter_task_type = AlterTaskType::Suspend as i32,
            AlterTaskOptions::Set {
                schedule,
                comments,
                warehouse,
                suspend_task_after_num_failures,
                error_integration,
                session_parameters,
            } => {
                req.alter_task_type = AlterTaskType::Set as i32;
                req.schedule_options = schedule.map(make_schedule_options);
                req.comment = comments;
                req.warehouse_options = warehouse.map(|w| WarehouseOptions {
                    warehouse: Some(w),
                    using_warehouse_size: None,
                });
                req.error_integration = error_integration;
                req.suspend_task_after_num_failures =
                    suspend_task_after_num_failures.map(|i| i as i32);
                if let Some(session_parameters) = session_parameters {
                    req.set_session_parameters = true;
                    req.session_parameters = session_parameters;
                }
            }
            AlterTaskOptions::Unset { .. } => todo!(),
            AlterTaskOptions::ModifyAs(sql) => {
                req.alter_task_type = AlterTaskType::ModifyAs as i32;
                match sql {
                    TaskSql::SingleStatement(stmt) => {
                        req.task_sql_type = i32::from(pb::TaskSqlType::Sql);
                        req.query_text = Some(stmt);
                    }
                    TaskSql::ScriptBlock(ref sqls) => {
                        req.task_sql_type = i32::from(pb::TaskSqlType::Script);
                        req.query_text = Some(format!("{}", sql));
                        req.script_sql = Some(pb::ScriptSql { sqls: sqls.clone() });
                    }
                }
            }
            AlterTaskOptions::AddAfter(tasks) => {
                req.alter_task_type = AlterTaskType::AddAfter as i32;
                req.add_after = tasks;
            }
            AlterTaskOptions::RemoveAfter(tasks) => {
                req.alter_task_type = AlterTaskType::RemoveAfter as i32;
                req.remove_after = tasks;
            }
            AlterTaskOptions::ModifyWhen(sql) => {
                req.alter_task_type = AlterTaskType::ModifyWhen as i32;
                req.when_condition = Some(sql.to_string());
            }
        }

        req
    }

    async fn build_show_tasks_request<C: TaskContext>(
        ctx: &C,
        plan: &ShowTasksPlan,
    ) -> Result<ShowTasksRequest> {
        Ok(ShowTasksRequest {
            tenant_id: plan.tenant.tenant_name().to_string(),
            name_like: String::new(),
            result_limit: 10000,
            owners: ctx.available_role_identities().await?,
            task_ids: vec![],
        })
    }
}

#[async_trait::async_trait]
impl<C: TaskContext> TaskInterpreter<C> for CloudTaskInterpreter {
    async fn create_task(&self, ctx: &Arc<C>, plan: &CreateTaskPlan) -> Result<()> {
        ensure_cloud_control_enabled("create task")?;
        Self::validate_create_session_parameters(plan)?;

        let cloud_api = CloudControlApiProvider::instance();
        let task_client = cloud_api.get_task_client();
        let req = Self::build_create_request(ctx.as_ref(), plan);
        let config = build_task_client_config(ctx.as_ref(), cloud_api.get_timeout())?;
        let req = make_request(req, config.clone());

        if plan.create_option.is_overriding() {
            let drop_req = DropTaskRequest {
                task_name: plan.task_name.clone(),
                tenant_id: plan.tenant.tenant_name().to_string(),
                if_exist: true,
            };
            let drop_req = make_request(drop_req, config);
            task_client.drop_task(drop_req).await?;
        }

        task_client.create_task(req).await?;
        Ok(())
    }

    async fn execute_task(&self, ctx: &Arc<C>, plan: &ExecuteTaskPlan) -> Result<()> {
        ensure_cloud_control_enabled("execute task")?;

        let cloud_api = CloudControlApiProvider::instance();
        let task_client = cloud_api.get_task_client();
        let req = ExecuteTaskRequest {
            task_name: plan.task_name.clone(),
            tenant_id: plan.tenant.tenant_name().to_string(),
        };
        let config = build_task_client_config(ctx.as_ref(), cloud_api.get_timeout())?;
        let req = make_request(req, config);
        task_client.execute_task(req).await?;
        Ok(())
    }

    async fn alter_task(&self, ctx: &Arc<C>, plan: &AlterTaskPlan) -> Result<()> {
        ensure_cloud_control_enabled("alter task")?;
        Self::validate_alter_session_parameters(plan)?;

        let cloud_api = CloudControlApiProvider::instance();
        let task_client = cloud_api.get_task_client();
        let req = Self::build_alter_request(ctx.as_ref(), plan);
        let config = build_task_client_config(ctx.as_ref(), cloud_api.get_timeout())?;
        let req = make_request(req, config);
        task_client.alter_task(req).await?;
        Ok(())
    }

    async fn describe_task(
        &self,
        ctx: &Arc<C>,
        plan: &DescribeTaskPlan,
    ) -> Result<Option<CloudTask>> {
        ensure_cloud_control_enabled("describe task")?;

        let cloud_api = CloudControlApiProvider::instance();
        let task_client = cloud_api.get_task_client();
        let req = DescribeTaskRequest {
            task_name: plan.task_name.clone(),
            tenant_id: plan.tenant.tenant_name().to_string(),
            if_exist: false,
        };
        let config = build_task_client_config(ctx.as_ref(), cloud_api.get_timeout())?;
        let req = make_request(req, config);
        let resp = task_client.describe_task(req).await?;
        resp.task.map(CloudTask::try_from).transpose()
    }

    async fn drop_task(&self, ctx: &Arc<C>, plan: &DropTaskPlan) -> Result<()> {
        ensure_cloud_control_enabled("drop task")?;

        let cloud_api = CloudControlApiProvider::instance();
        let task_client = cloud_api.get_task_client();
        let req = DropTaskRequest {
            task_name: plan.task_name.clone(),
            tenant_id: plan.tenant.tenant_name().to_string(),
            if_exist: plan.if_exists,
        };
        let config = build_task_client_config(ctx.as_ref(), cloud_api.get_timeout())?;
        let req = make_request(req, config);
        task_client.drop_task(req).await?;
        Ok(())
    }

    async fn show_tasks(&self, ctx: &Arc<C>, plan: &ShowTasksPlan) -> Result<Vec<CloudTask>> {
        ensure_cloud_control_enabled("show tasks")?;

        let cloud_api = CloudControlApiProvider::instance();
        let task_client = cloud_api.get_task_client();
        let req = Self::build_show_tasks_request(ctx.as_ref(), plan).await?;
        let config = build_task_client_config(ctx.as_ref(), cloud_api.get_timeout())?;
        let req = make_request(req, config);

        let resp = task_client.show_tasks(req).await?;
        resp.tasks
            .into_iter()
            .map(CloudTask::try_from)
            .try_collect()
    }
}

struct PrivateTaskInterpreter;

#[async_trait::async_trait]
impl<C: TaskContext> TaskInterpreter<C> for PrivateTaskInterpreter {
    async fn create_task(&self, ctx: &Arc<C>, plan: &CreateTaskPlan) -> Result<()> {
        let task = databend_common_meta_app::principal::Task {
            task_id: EMPTY_TASK_ID,
            task_name: plan.task_name.clone(),
            query_text: match &plan.sql {
                TaskSql::SingleStatement(s) => s.clone(),
                TaskSql::ScriptBlock(_) => format!("{}", plan.sql),
            },
            when_condition: plan.when_condition.clone(),
            after: plan.after.clone(),
            comment: plan.comment.clone(),
            owner: ctx.current_role_identity(),
            owner_user: ctx.current_user_encoded()?,
            schedule_options: plan
                .schedule_opts
                .clone()
                .map(TaskMgr::make_schedule_options),
            warehouse_options: Some(TaskMgr::make_warehouse_options(plan.warehouse.clone())),
            next_scheduled_at: None,
            suspend_task_after_num_failures: plan.suspend_task_after_num_failures,
            error_integration: plan.error_integration.clone(),
            status: Status::Suspended,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            last_suspended_at: None,
            session_params: plan.session_parameters.clone(),
        };

        UserApiProvider::instance()
            .task_api(&plan.tenant)
            .create_task(task, &plan.create_option)
            .await??;

        Ok(())
    }

    async fn execute_task(&self, _ctx: &Arc<C>, plan: &ExecuteTaskPlan) -> Result<()> {
        UserApiProvider::instance()
            .task_api(&plan.tenant)
            .execute_task(&plan.task_name)
            .await??;
        Ok(())
    }

    async fn alter_task(&self, _ctx: &Arc<C>, plan: &AlterTaskPlan) -> Result<()> {
        UserApiProvider::instance()
            .task_api(&plan.tenant)
            .alter_task(&plan.task_name, &plan.alter_options)
            .await??;
        Ok(())
    }

    async fn describe_task(
        &self,
        _ctx: &Arc<C>,
        plan: &DescribeTaskPlan,
    ) -> Result<Option<CloudTask>> {
        let task = UserApiProvider::instance()
            .task_api(&plan.tenant)
            .describe_task(&plan.task_name)
            .await??;
        task.map(private_task_to_cloud_task).transpose()
    }

    async fn drop_task(&self, _ctx: &Arc<C>, plan: &DropTaskPlan) -> Result<()> {
        UserApiProvider::instance()
            .task_api(&plan.tenant)
            .drop_task(&plan.task_name)
            .await
            .map_err(|e| ErrorCode::MetaServiceError(e.to_string()))?;
        Ok(())
    }

    async fn show_tasks(&self, _ctx: &Arc<C>, plan: &ShowTasksPlan) -> Result<Vec<CloudTask>> {
        let tasks = UserApiProvider::instance()
            .task_api(&plan.tenant)
            .list_task()
            .await
            .map_err(|e| ErrorCode::MetaServiceError(e.to_string()))?;

        tasks
            .into_iter()
            .map(private_task_to_cloud_task)
            .try_collect()
    }
}

fn make_schedule_options(
    opt: ScheduleOptions,
) -> databend_common_cloud_control::pb::ScheduleOptions {
    match opt {
        ScheduleOptions::IntervalSecs(secs, ms) => {
            databend_common_cloud_control::pb::ScheduleOptions {
                interval: Some(secs as i32),
                milliseconds_interval: if ms == 0 { None } else { Some(ms) },
                cron: None,
                time_zone: None,
                schedule_type: i32::from(ScheduleType::IntervalType),
            }
        }
        ScheduleOptions::CronExpression(expr, timezone) => {
            databend_common_cloud_control::pb::ScheduleOptions {
                interval: None,
                milliseconds_interval: None,
                cron: Some(expr),
                time_zone: timezone,
                schedule_type: i32::from(ScheduleType::CronType),
            }
        }
    }
}

fn make_warehouse_options(
    opt: Option<String>,
) -> databend_common_cloud_control::pb::WarehouseOptions {
    databend_common_cloud_control::pb::WarehouseOptions {
        warehouse: opt,
        using_warehouse_size: None,
    }
}

fn build_task_client_config<C: TaskContext>(ctx: &C, timeout: Duration) -> Result<ClientConfig> {
    let mut cfg = build_client_config(
        ctx.tenant_name(),
        ctx.current_user_display()?,
        ctx.query_id(),
        timeout,
    );
    cfg.add_task_version_info();
    Ok(cfg)
}

fn ensure_cloud_control_enabled(op: &str) -> Result<()> {
    if GlobalConfig::instance()
        .query
        .common
        .cloud_control_grpc_server_address
        .is_none()
    {
        return Err(ErrorCode::CloudControlNotEnabled(format!(
            "cannot {op} without cloud control enabled, please set cloud_control_grpc_server_address in config",
        )));
    }

    Ok(())
}
