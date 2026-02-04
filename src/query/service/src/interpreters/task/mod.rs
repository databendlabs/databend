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

use databend_common_catalog::table_context::TableContext;
use databend_common_cloud_control::task_utils;
use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_sql::plans::AlterTaskPlan;
use databend_common_sql::plans::CreateTaskPlan;
use databend_common_sql::plans::DescribeTaskPlan;
use databend_common_sql::plans::DropTaskPlan;
use databend_common_sql::plans::ExecuteTaskPlan;
use databend_common_sql::plans::ShowTasksPlan;

use crate::interpreters::task::cloud::CloudTaskInterpreter;
use crate::interpreters::task::private::PrivateTaskInterpreter;
use crate::sessions::QueryContext;

mod cloud;
mod private;

pub(crate) struct TaskInterpreterManager;

impl TaskInterpreterManager {
    pub fn build(ctx: &QueryContext) -> Result<TaskInterpreterImpl> {
        if GlobalConfig::instance().task.on {
            LicenseManagerSwitch::instance()
                .check_enterprise_enabled(ctx.get_license_key(), Feature::PrivateTask)?;
            return Ok(TaskInterpreterImpl::Private(PrivateTaskInterpreter));
        }
        Ok(TaskInterpreterImpl::Cloud(CloudTaskInterpreter))
    }
}

pub(crate) enum TaskInterpreterImpl {
    Cloud(CloudTaskInterpreter),
    Private(PrivateTaskInterpreter),
}

pub(crate) trait TaskInterpreter {
    async fn create_task(&self, ctx: &Arc<QueryContext>, plan: &CreateTaskPlan) -> Result<()>;

    async fn execute_task(&self, ctx: &Arc<QueryContext>, plan: &ExecuteTaskPlan) -> Result<()>;

    async fn alter_task(&self, ctx: &Arc<QueryContext>, plan: &AlterTaskPlan) -> Result<()>;

    async fn describe_task(
        &self,
        ctx: &Arc<QueryContext>,
        plan: &DescribeTaskPlan,
    ) -> Result<Option<task_utils::Task>>;

    async fn drop_task(&self, ctx: &Arc<QueryContext>, plan: &DropTaskPlan) -> Result<()>;

    #[allow(dead_code)]
    async fn show_tasks(
        &self,
        ctx: &Arc<QueryContext>,
        plan: &ShowTasksPlan,
    ) -> Result<Vec<task_utils::Task>>;
}

impl TaskInterpreter for TaskInterpreterImpl {
    async fn create_task(&self, ctx: &Arc<QueryContext>, plan: &CreateTaskPlan) -> Result<()> {
        match self {
            TaskInterpreterImpl::Cloud(interpreter) => interpreter.create_task(ctx, plan).await,
            TaskInterpreterImpl::Private(interpreter) => interpreter.create_task(ctx, plan).await,
        }
    }

    async fn execute_task(&self, ctx: &Arc<QueryContext>, plan: &ExecuteTaskPlan) -> Result<()> {
        match self {
            TaskInterpreterImpl::Cloud(interpreter) => interpreter.execute_task(ctx, plan).await,
            TaskInterpreterImpl::Private(interpreter) => interpreter.execute_task(ctx, plan).await,
        }
    }

    async fn alter_task(&self, ctx: &Arc<QueryContext>, plan: &AlterTaskPlan) -> Result<()> {
        match self {
            TaskInterpreterImpl::Cloud(interpreter) => interpreter.alter_task(ctx, plan).await,
            TaskInterpreterImpl::Private(interpreter) => interpreter.alter_task(ctx, plan).await,
        }
    }

    async fn describe_task(
        &self,
        ctx: &Arc<QueryContext>,
        plan: &DescribeTaskPlan,
    ) -> Result<Option<task_utils::Task>> {
        match self {
            TaskInterpreterImpl::Cloud(interpreter) => interpreter.describe_task(ctx, plan).await,
            TaskInterpreterImpl::Private(interpreter) => interpreter.describe_task(ctx, plan).await,
        }
    }

    async fn drop_task(&self, ctx: &Arc<QueryContext>, plan: &DropTaskPlan) -> Result<()> {
        match self {
            TaskInterpreterImpl::Cloud(interpreter) => interpreter.drop_task(ctx, plan).await,
            TaskInterpreterImpl::Private(interpreter) => interpreter.drop_task(ctx, plan).await,
        }
    }

    async fn show_tasks(
        &self,
        ctx: &Arc<QueryContext>,
        plan: &ShowTasksPlan,
    ) -> Result<Vec<task_utils::Task>> {
        match self {
            TaskInterpreterImpl::Cloud(interpreter) => interpreter.show_tasks(ctx, plan).await,
            TaskInterpreterImpl::Private(interpreter) => interpreter.show_tasks(ctx, plan).await,
        }
    }
}
