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

use databend_common_ast::ast::TaskSql;
use databend_common_catalog::table_context::TableContext;
use databend_common_cloud_control::client_config::make_request;
use databend_common_cloud_control::cloud_api::CloudControlApiProvider;
use databend_common_cloud_control::pb;
use databend_common_cloud_control::pb::CreateTaskRequest;
use databend_common_cloud_control::pb::DropTaskRequest;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_settings::DefaultSettings;
use databend_common_settings::SettingScope;
use databend_common_sql::plans::CreateTaskPlan;

use crate::interpreters::common::get_task_client_config;
use crate::interpreters::common::make_schedule_options;
use crate::interpreters::common::make_warehouse_options;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct CreateTaskInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateTaskPlan,
}

impl CreateTaskInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateTaskPlan) -> Result<Self> {
        Ok(CreateTaskInterpreter { ctx, plan })
    }
}

impl CreateTaskInterpreter {
    fn build_request(&self) -> CreateTaskRequest {
        let plan = self.plan.clone();
        let owner = self
            .ctx
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

    fn validate_session_parameters(&self) -> Result<()> {
        let session_parameters = self.plan.session_parameters.clone();
        for (key, _) in session_parameters.iter() {
            DefaultSettings::check_setting_scope(key, SettingScope::Session)?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateTaskInterpreter {
    fn name(&self) -> &str {
        "CreateTaskInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let config = GlobalConfig::instance();
        if config.query.cloud_control_grpc_server_address.is_none() {
            return Err(ErrorCode::CloudControlNotEnabled(
                "cannot create task without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }
        self.validate_session_parameters()?;
        let cloud_api = CloudControlApiProvider::instance();
        let task_client = cloud_api.get_task_client();
        let req = self.build_request();
        let config = get_task_client_config(self.ctx.clone(), cloud_api.get_timeout())?;
        let req = make_request(req, config.clone());

        // cloud don't support create or replace, let's remove the task in previous
        if self.plan.create_option.is_overriding() {
            let drop_req = DropTaskRequest {
                task_name: self.plan.task_name.clone(),
                tenant_id: self.plan.tenant.tenant_name().to_string(),
                if_exist: true,
            };
            let drop_req = make_request(drop_req, config);
            task_client.drop_task(drop_req).await?;
        }

        task_client.create_task(req).await?;
        Ok(PipelineBuildResult::create())
    }
}
