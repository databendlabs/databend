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
use databend_common_cloud_control::task_utils;
use databend_common_exception::Result;
use databend_common_management::task::TaskMgr;
use databend_common_meta_app::principal::Status;
use databend_common_meta_app::principal::task::EMPTY_TASK_ID;
use databend_common_sql::plans::AlterTaskPlan;
use databend_common_sql::plans::CreateTaskPlan;
use databend_common_sql::plans::DescribeTaskPlan;
use databend_common_sql::plans::DropTaskPlan;
use databend_common_sql::plans::ExecuteTaskPlan;
use databend_common_sql::plans::ShowTasksPlan;
use databend_common_storages_system::PrivateTasksTable;
use databend_common_users::UserApiProvider;

use crate::interpreters::task::TaskInterpreter;
use crate::sessions::QueryContext;

pub(crate) struct PrivateTaskInterpreter;

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
            .task_api(&plan.tenant)
            .create_task(task, &plan.create_option)
            .await??;

        Ok(())
    }

    async fn execute_task(&self, _ctx: &Arc<QueryContext>, plan: &ExecuteTaskPlan) -> Result<()> {
        UserApiProvider::instance()
            .task_api(&plan.tenant)
            .execute_task(&plan.task_name)
            .await??;

        Ok(())
    }

    async fn alter_task(&self, _ctx: &Arc<QueryContext>, plan: &AlterTaskPlan) -> Result<()> {
        UserApiProvider::instance()
            .task_api(&plan.tenant)
            .alter_task(&plan.task_name, &plan.alter_options)
            .await??;

        Ok(())
    }

    async fn describe_task(
        &self,
        _ctx: &Arc<QueryContext>,
        plan: &DescribeTaskPlan,
    ) -> Result<Option<task_utils::Task>> {
        let task = UserApiProvider::instance()
            .task_api(&plan.tenant)
            .describe_task(&plan.task_name)
            .await??;
        task.map(PrivateTasksTable::task_trans).transpose()
    }

    async fn drop_task(&self, _ctx: &Arc<QueryContext>, plan: &DropTaskPlan) -> Result<()> {
        UserApiProvider::instance()
            .task_api(&plan.tenant)
            .drop_task(&plan.task_name)
            .await?;

        Ok(())
    }

    async fn show_tasks(
        &self,
        _ctx: &Arc<QueryContext>,
        plan: &ShowTasksPlan,
    ) -> Result<Vec<task_utils::Task>> {
        let tasks = UserApiProvider::instance()
            .task_api(&plan.tenant)
            .list_task()
            .await?;

        tasks
            .into_iter()
            .map(PrivateTasksTable::task_trans)
            .try_collect()
    }
}
