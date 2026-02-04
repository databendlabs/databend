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
use databend_common_exception::Result;
use databend_common_management::RoleApi;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_sql::plans::DropTaskPlan;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::interpreters::task::TaskInterpreter;
use crate::interpreters::task::TaskInterpreterManager;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct DropTaskInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropTaskPlan,
}

impl DropTaskInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropTaskPlan) -> Result<Self> {
        Ok(DropTaskInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropTaskInterpreter {
    fn name(&self) -> &str {
        "DropTaskInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        TaskInterpreterManager::build(&self.ctx)?
            .drop_task(&self.ctx, &self.plan)
            .await?;

        let role_api = UserApiProvider::instance().role_api(&self.plan.tenant);
        let owner_object = OwnershipObject::Task {
            name: self.plan.task_name.to_string(),
        };

        role_api.revoke_ownership(&owner_object).await?;
        RoleCacheManager::instance().invalidate_cache(&self.plan.tenant);

        Ok(PipelineBuildResult::create())
    }
}
