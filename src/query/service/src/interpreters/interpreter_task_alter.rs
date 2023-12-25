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

use common_ast::ast::AlterTaskOptions;
use common_catalog::table_context::TableContext;
use common_cloud_control::cloud_api::CloudControlApiProvider;
use common_cloud_control::pb::alter_task_request::AlterTaskType;
use common_cloud_control::pb::AlterTaskRequest;
use common_cloud_control::pb::WarehouseOptions;
use common_cloud_control::task_client::make_request;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_sql::plans::AlterTaskPlan;

use crate::interpreters::common::get_client_config;
use crate::interpreters::common::make_schedule_options;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct AlterTaskInterpreter {
    ctx: Arc<QueryContext>,
    plan: AlterTaskPlan,
}

impl AlterTaskInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AlterTaskPlan) -> Result<Self> {
        Ok(AlterTaskInterpreter { ctx, plan })
    }
}

impl AlterTaskInterpreter {
    fn build_request(&self) -> AlterTaskRequest {
        let plan = self.plan.clone();
        let owner = self
            .ctx
            .get_current_role()
            .unwrap_or_default()
            .identity()
            .to_string();
        let mut req = AlterTaskRequest {
            task_name: plan.task_name,
            tenant_id: plan.tenant,
            owner,
            alter_task_type: 0,
            if_exist: plan.if_exists,

            query_text: None,
            comment: None,
            schedule_options: None,
            warehouse_options: None,
            suspend_task_after_num_failures: None,
            when_condition: None,
            add_after: vec![],
            remove_after: vec![],
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
            } => {
                req.alter_task_type = AlterTaskType::Set as i32;
                req.schedule_options = schedule.map(make_schedule_options);
                req.comment = comments;
                req.warehouse_options = warehouse.map(|w| WarehouseOptions {
                    warehouse: Some(w),
                    using_warehouse_size: None,
                });
                req.suspend_task_after_num_failures =
                    suspend_task_after_num_failures.map(|i| i as i32);
            }
            AlterTaskOptions::Unset { .. } => {
                todo!()
            }
            AlterTaskOptions::ModifyAs(sql) => {
                req.alter_task_type = AlterTaskType::ModifyAs as i32;
                req.query_text = Some(sql);
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
                req.when_condition = Some(sql);
            }
        }
        req
    }
}

#[async_trait::async_trait]
impl Interpreter for AlterTaskInterpreter {
    fn name(&self) -> &str {
        "AlterTaskInterpreter"
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let config = GlobalConfig::instance();
        if config.query.cloud_control_grpc_server_address.is_none() {
            return Err(ErrorCode::CloudControlNotEnabled(
                "cannot alter task without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }
        let cloud_api = CloudControlApiProvider::instance();
        let task_client = cloud_api.get_task_client();
        let req = self.build_request();
        let config = get_client_config(self.ctx.clone())?;
        let req = make_request(req, config);
        task_client.alter_task(req).await?;
        Ok(PipelineBuildResult::create())
    }
}
