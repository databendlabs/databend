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
use databend_common_sql::plans::CreateUDFPlan;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct CreateUserUDFScript {
    ctx: Arc<QueryContext>,
    plan: CreateUDFPlan,
}

impl CreateUserUDFScript {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateUDFPlan) -> Result<Self> {
        Ok(CreateUserUDFScript { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateUserUDFScript {
    fn name(&self) -> &str {
        "CreateUserUDFScript"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "create_user_udf_execute");

        let plan = self.plan.clone();
        let tenant = self.ctx.get_tenant();
        let udf = plan.udf;
        let _ = UserApiProvider::instance()
            .add_udf(&tenant, udf, &plan.create_option)
            .await?;

        // Grant ownership as the current role
        if let Some(current_role) = self.ctx.get_current_role() {
            let role_api = UserApiProvider::instance().role_api(&tenant);
            role_api
                .grant_ownership(
                    &OwnershipObject::UDF {
                        name: self.plan.udf.name.clone(),
                    },
                    &current_role.name,
                )
                .await?;
            RoleCacheManager::instance().invalidate_cache(&tenant);
        }

        Ok(PipelineBuildResult::create())
    }
}
