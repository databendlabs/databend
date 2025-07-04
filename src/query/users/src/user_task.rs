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

use databend_common_ast::ast::AlterTaskOptions;
use databend_common_exception::Result;
use databend_common_meta_app::principal::Task;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;

use crate::UserApiProvider;

impl UserApiProvider {
    // Add a new Task.
    #[async_backtrace::framed]
    pub async fn create_task(
        &self,
        tenant: &Tenant,
        task: Task,
        create_option: &CreateOption,
    ) -> Result<()> {
        let task_api = self.task_api(tenant);
        task_api.create_task(task, create_option).await??;
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn execute_task(&self, tenant: &Tenant, task_name: &str) -> Result<()> {
        let task_api = self.task_api(tenant);
        task_api.execute_task(task_name).await??;
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn alter_task(
        &self,
        tenant: &Tenant,
        task_name: &str,
        alter_options: &AlterTaskOptions,
    ) -> Result<()> {
        let task_api = self.task_api(tenant);
        task_api.alter_task(task_name, alter_options).await??;
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn describe_task(&self, tenant: &Tenant, task_name: &str) -> Result<Option<Task>> {
        let task_api = self.task_api(tenant);
        let task = task_api.describe_task(task_name).await??;
        Ok(task)
    }

    #[async_backtrace::framed]
    pub async fn drop_task(&self, tenant: &Tenant, task_name: &str) -> Result<()> {
        let task_api = self.task_api(tenant);
        task_api.drop_task(task_name).await?;

        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn show_tasks(&self, tenant: &Tenant) -> Result<Vec<Task>> {
        let task_api = self.task_api(tenant);
        let tasks = task_api.list_task().await?;

        Ok(tasks)
    }
}
