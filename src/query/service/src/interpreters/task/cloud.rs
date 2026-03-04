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

use databend_common_ast::ast::AlterTaskOptions;
use databend_common_ast::ast::TaskSql;
use databend_common_catalog::table_context::TableContext;
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
use databend_common_cloud_control::task_utils;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_settings::DefaultSettings;
use databend_common_settings::SettingScope;
use databend_common_sql::plans::AlterTaskPlan;
use databend_common_sql::plans::CreateTaskPlan;
use databend_common_sql::plans::DescribeTaskPlan;
use databend_common_sql::plans::DropTaskPlan;
use databend_common_sql::plans::ExecuteTaskPlan;
use databend_common_sql::plans::ShowTasksPlan;

use crate::interpreters::common::get_task_client_config;
use crate::interpreters::common::make_schedule_options;
use crate::interpreters::common::make_warehouse_options;
use crate::interpreters::task::TaskInterpreter;
use crate::sessions::QueryContext;

pub(crate) struct CloudTaskInterpreter;

impl CloudTaskInterpreter {
    fn validate_create_session_parameters(plan: &CreateTaskPlan) -> Result<()> {
        let session_parameters = plan.session_parameters.clone();
        for (key, _) in session_parameters.iter() {
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
            for (key, _) in session_parameters.iter() {
                DefaultSettings::check_setting_scope(key, SettingScope::Session)?;
            }
        }
        Ok(())
    }

    fn build_create_request(ctx: &Arc<QueryContext>, plan: &CreateTaskPlan) -> CreateTaskRequest {
        let plan = plan.clone();
        let owner = ctx
            .get_current_role()
            .unwrap_or_default()
            .identity()
            .to_string();
        let mut req = CreateTaskRequest {
            task_name: plan.task_name,
            tenant_id: plan.tenant.tenant_name().to_string(),
            query_text: "".to_string(),
            owner,
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
                req.script_sql = Some(pb::ScriptSql { sqls: sqls.clone() })
            }
        }
        req
    }

    fn build_alter_request(ctx: &Arc<QueryContext>, plan: &AlterTaskPlan) -> AlterTaskRequest {
        let plan = plan.clone();
        let owner = ctx
            .get_current_role()
            .unwrap_or_default()
            .identity()
            .to_string();
        let mut req = AlterTaskRequest {
            task_name: plan.task_name,
            tenant_id: plan.tenant.tenant_name().to_string(),
            owner,
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
            AlterTaskOptions::Resume => {
                req.alter_task_type = AlterTaskType::Resume as i32;
            }
            AlterTaskOptions::Suspend => {
                req.alter_task_type = AlterTaskType::Suspend as i32;
            }
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
            AlterTaskOptions::Unset { .. } => {
                todo!()
            }
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
                        req.script_sql = Some(pb::ScriptSql { sqls: sqls.clone() })
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

    async fn build_show_tasks_request(
        ctx: &Arc<QueryContext>,
        plan: &ShowTasksPlan,
    ) -> Result<ShowTasksRequest> {
        let plan = plan.clone();
        let available_roles = ctx.get_current_session().get_all_available_roles().await?;
        let req = ShowTasksRequest {
            tenant_id: plan.tenant.tenant_name().to_string(),
            name_like: "".to_string(),
            result_limit: 10000, // TODO: use plan.limit pushdown
            owners: available_roles
                .into_iter()
                .map(|x| x.identity().to_string())
                .collect(),
            task_ids: vec![],
        };
        Ok(req)
    }
}

impl TaskInterpreter for CloudTaskInterpreter {
    async fn create_task(&self, ctx: &Arc<QueryContext>, plan: &CreateTaskPlan) -> Result<()> {
        let config = GlobalConfig::instance();
        if config
            .query
            .common
            .cloud_control_grpc_server_address
            .is_none()
        {
            return Err(ErrorCode::CloudControlNotEnabled(
                "cannot create task without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }
        Self::validate_create_session_parameters(plan)?;
        let cloud_api = CloudControlApiProvider::instance();
        let task_client = cloud_api.get_task_client();
        let req = Self::build_create_request(ctx, plan);
        let config = get_task_client_config(ctx.clone(), cloud_api.get_timeout())?;
        let req = make_request(req, config.clone());

        // cloud don't support create or replace, let's remove the task in previous
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

    async fn execute_task(&self, ctx: &Arc<QueryContext>, plan: &ExecuteTaskPlan) -> Result<()> {
        let config = GlobalConfig::instance();
        if config
            .query
            .common
            .cloud_control_grpc_server_address
            .is_none()
        {
            return Err(ErrorCode::CloudControlNotEnabled(
                "cannot execute task without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }
        let cloud_api = CloudControlApiProvider::instance();
        let task_client = cloud_api.get_task_client();
        let req = ExecuteTaskRequest {
            task_name: plan.task_name.clone(),
            tenant_id: plan.tenant.tenant_name().to_string(),
        };
        let config = get_task_client_config(ctx.clone(), cloud_api.get_timeout())?;
        let req = make_request(req, config);

        task_client.execute_task(req).await?;

        Ok(())
    }

    async fn alter_task(&self, ctx: &Arc<QueryContext>, plan: &AlterTaskPlan) -> Result<()> {
        let config = GlobalConfig::instance();
        if config
            .query
            .common
            .cloud_control_grpc_server_address
            .is_none()
        {
            return Err(ErrorCode::CloudControlNotEnabled(
                "cannot alter task without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }
        Self::validate_alter_session_parameters(plan)?;
        let cloud_api = CloudControlApiProvider::instance();
        let task_client = cloud_api.get_task_client();
        let req = Self::build_alter_request(ctx, plan);
        let config = get_task_client_config(ctx.clone(), cloud_api.get_timeout())?;
        let req = make_request(req, config);
        task_client.alter_task(req).await?;

        Ok(())
    }

    async fn describe_task(
        &self,
        ctx: &Arc<QueryContext>,
        plan: &DescribeTaskPlan,
    ) -> Result<Option<task_utils::Task>> {
        let config = GlobalConfig::instance();
        if config
            .query
            .common
            .cloud_control_grpc_server_address
            .is_none()
        {
            return Err(ErrorCode::CloudControlNotEnabled(
                "cannot describe task without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }
        let cloud_api = CloudControlApiProvider::instance();
        let task_client = cloud_api.get_task_client();
        let plan = plan.clone();
        let req = DescribeTaskRequest {
            task_name: plan.task_name,
            tenant_id: plan.tenant.tenant_name().to_string(),
            if_exist: false,
        };
        let config = get_task_client_config(ctx.clone(), cloud_api.get_timeout())?;
        let req = make_request(req, config);
        let resp = task_client.describe_task(req).await?;

        resp.task.map(task_utils::Task::try_from).transpose()
    }

    async fn drop_task(&self, ctx: &Arc<QueryContext>, plan: &DropTaskPlan) -> Result<()> {
        let config = GlobalConfig::instance();
        if config
            .query
            .common
            .cloud_control_grpc_server_address
            .is_none()
        {
            return Err(ErrorCode::CloudControlNotEnabled(
                "cannot drop task without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }
        let cloud_api = CloudControlApiProvider::instance();
        let task_client = cloud_api.get_task_client();
        let req = DropTaskRequest {
            task_name: plan.task_name.clone(),
            tenant_id: plan.tenant.tenant_name().to_string(),
            if_exist: plan.if_exists,
        };
        let config = get_task_client_config(ctx.clone(), cloud_api.get_timeout())?;
        let req = make_request(req, config);
        task_client.drop_task(req).await?;

        Ok(())
    }

    async fn show_tasks(
        &self,
        ctx: &Arc<QueryContext>,
        plan: &ShowTasksPlan,
    ) -> Result<Vec<task_utils::Task>> {
        let config = GlobalConfig::instance();
        if config
            .query
            .common
            .cloud_control_grpc_server_address
            .is_none()
        {
            return Err(ErrorCode::CloudControlNotEnabled(
                "cannot drop task without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }
        let cloud_api = CloudControlApiProvider::instance();
        let task_client = cloud_api.get_task_client();
        let req = Self::build_show_tasks_request(ctx, plan).await?;
        let config = get_task_client_config(ctx.clone(), cloud_api.get_timeout())?;
        let req = make_request(req, config);

        let resp = task_client.show_tasks(req).await?;
        resp.tasks.into_iter().map(|t| t.try_into()).try_collect()
    }
}
