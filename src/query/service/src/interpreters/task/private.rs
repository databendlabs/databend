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

use chrono::Utc;
use databend_common_ast::ast::TaskSql;
use databend_common_catalog::table_context::TableContext;
use databend_common_cloud_control::pb;
use databend_common_cloud_control::task_utils;
use databend_common_exception::Result;
use databend_common_management::task::TaskMgr;
use databend_common_meta_app::principal::task::EMPTY_TASK_ID;
use databend_common_meta_app::principal::Status;
use databend_common_meta_app::principal::Task;
use databend_common_sql::plans::AlterTaskPlan;
use databend_common_sql::plans::CreateTaskPlan;
use databend_common_sql::plans::DescribeTaskPlan;
use databend_common_sql::plans::DropTaskPlan;
use databend_common_sql::plans::ExecuteTaskPlan;
use databend_common_sql::plans::ShowTasksPlan;
use databend_common_users::UserApiProvider;

use crate::interpreters::task::TaskInterpreter;
use crate::sessions::QueryContext;

pub(crate) struct PrivateTaskInterpreter;

impl PrivateTaskInterpreter {
    fn task_trans(task: Task) -> Result<task_utils::Task> {
        Ok(task_utils::Task {
            task_id: task.task_id,
            task_name: task.task_name,
            query_text: task.query_text,
            condition_text: task.when_condition.unwrap_or_default(),
            after: task.after,
            comment: task.comment,
            owner: task.owner,
            schedule_options: task
                .schedule_options
                .map(|schedule_options| {
                    let options = pb::ScheduleOptions {
                        interval: schedule_options.interval,
                        cron: schedule_options.cron,
                        time_zone: schedule_options.time_zone,
                        schedule_type: schedule_options.schedule_type as i32,
                        milliseconds_interval: schedule_options.milliseconds_interval,
                    };
                    task_utils::format_schedule_options(&options)
                })
                .transpose()?,
            warehouse_options: task.warehouse_options.map(|warehouse_options| {
                pb::WarehouseOptions {
                    warehouse: warehouse_options.warehouse,
                    using_warehouse_size: warehouse_options.using_warehouse_size,
                }
            }),
            next_scheduled_at: task.next_scheduled_at,
            suspend_task_after_num_failures: task.suspend_task_after_num_failures.map(|i| i as i32),
            error_integration: task.error_integration,
            status: match task.status {
                Status::Suspended => task_utils::Status::Suspended,
                Status::Started => task_utils::Status::Started,
            },
            created_at: task.created_at,
            updated_at: task.updated_at,
            last_suspended_at: task.last_suspended_at,
            session_params: task.session_params,
        })
    }
}

impl TaskInterpreter for PrivateTaskInterpreter {
    async fn create_task(&self, ctx: &Arc<QueryContext>, plan: &CreateTaskPlan) -> Result<()> {
        let plan = plan.clone();

        let owner = ctx
            .get_current_role()
            .unwrap_or_default()
            .identity()
            .to_string();

        let task = databend_common_meta_app::principal::Task {
            task_id: EMPTY_TASK_ID,
            task_name: plan.task_name,
            query_text: match plan.sql {
                TaskSql::SingleStatement(s) => s,
                TaskSql::ScriptBlock(_) => format!("{}", plan.sql),
            },
            when_condition: plan.when_condition,
            after: plan.after.clone(),
            comment: plan.comment.clone(),
            owner,
            owner_user: ctx.get_current_user()?.identity().encode(),
            schedule_options: plan.schedule_opts.map(TaskMgr::make_schedule_options),
            warehouse_options: Some(TaskMgr::make_warehouse_options(plan.warehouse)),
            next_scheduled_at: None,
            suspend_task_after_num_failures: plan.suspend_task_after_num_failures,
            error_integration: plan.error_integration.clone(),
            status: Status::Suspended,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            last_suspended_at: None,
            session_params: plan.session_parameters.clone(),
        };
        UserApiProvider::instance()
            .create_task(&plan.tenant, task, &plan.create_option)
            .await?;

        Ok(())
    }

    async fn execute_task(&self, _ctx: &Arc<QueryContext>, plan: &ExecuteTaskPlan) -> Result<()> {
        UserApiProvider::instance()
            .execute_task(&plan.tenant, &plan.task_name)
            .await?;

        Ok(())
    }

    async fn alter_task(&self, _ctx: &Arc<QueryContext>, plan: &AlterTaskPlan) -> Result<()> {
        UserApiProvider::instance()
            .alter_task(&plan.tenant, &plan.task_name, &plan.alter_options)
            .await?;

        Ok(())
    }

    async fn describe_task(
        &self,
        _ctx: &Arc<QueryContext>,
        plan: &DescribeTaskPlan,
    ) -> Result<Option<task_utils::Task>> {
        let task = UserApiProvider::instance()
            .describe_task(&plan.tenant, &plan.task_name)
            .await?;
        task.map(Self::task_trans).transpose()
    }

    async fn drop_task(&self, _ctx: &Arc<QueryContext>, plan: &DropTaskPlan) -> Result<()> {
        UserApiProvider::instance()
            .drop_task(&plan.tenant, &plan.task_name)
            .await?;

        Ok(())
    }

    async fn show_tasks(
        &self,
        _ctx: &Arc<QueryContext>,
        plan: &ShowTasksPlan,
    ) -> Result<Vec<task_utils::Task>> {
        let tasks = UserApiProvider::instance().show_tasks(&plan.tenant).await?;

        tasks.into_iter().map(Self::task_trans).try_collect()
    }
}
