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
use databend_common_meta_app::schema::TaggableObject;
use databend_common_sql::plans::DropUDFPlan;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use log::debug;

use crate::interpreters::Interpreter;
use crate::interpreters::cleanup_object_tags;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct DropUserUDFScript {
    ctx: Arc<QueryContext>,
    plan: DropUDFPlan,
}

impl DropUserUDFScript {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropUDFPlan) -> Result<Self> {
        Ok(DropUserUDFScript { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropUserUDFScript {
    fn name(&self) -> &str {
        "DropUserUDFScript"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "drop_user_udf_execute");

        let plan = self.plan.clone();
        let tenant = self.ctx.get_tenant();

        // we should do `drop ownership` after actually drop udf, and udf maybe not exists.
        // drop the ownership
        if UserApiProvider::instance()
            .exists_udf(&tenant, &self.plan.udf)
            .await?
        {
            let role_api = UserApiProvider::instance().role_api(&tenant);
            let owner_object = OwnershipObject::UDF {
                name: self.plan.udf.clone(),
            };

            role_api.revoke_ownership(&owner_object).await?;
            RoleCacheManager::instance().invalidate_cache(&tenant);
        }

        // TODO: if it is appropriate to return an ErrorCode that contains either meta-service error and UdfNotFound error?

        UserApiProvider::instance()
            .drop_udf(&tenant, plan.udf.as_str(), plan.if_exists)
            .await??;

        cleanup_object_tags(&tenant, TaggableObject::UDF {
            name: plan.udf.clone(),
        })
        .await?;

        Ok(PipelineBuildResult::create())
    }
}
