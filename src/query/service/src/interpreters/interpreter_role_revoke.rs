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
use databend_common_meta_app::principal::PrincipalIdentity;
use databend_common_sql::plans::RevokeRolePlan;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct RevokeRoleInterpreter {
    ctx: Arc<QueryContext>,
    plan: RevokeRolePlan,
}

impl RevokeRoleInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RevokeRolePlan) -> Result<Self> {
        Ok(RevokeRoleInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for RevokeRoleInterpreter {
    fn name(&self) -> &str {
        "RevokeRoleInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "revoke_role_execute");

        let plan = self.plan.clone();
        let tenant = self.ctx.get_tenant();
        match plan.principal {
            PrincipalIdentity::User(user) => {
                UserApiProvider::instance()
                    .revoke_role_from_user(&tenant, user, plan.role)
                    .await?;
            }
            PrincipalIdentity::Role(role) => {
                UserApiProvider::instance()
                    .revoke_role_from_role(&tenant, &role, &plan.role)
                    .await?;
            }
        }

        RoleCacheManager::instance()
            .force_reload(
                &tenant,
                self.ctx
                    .get_settings()
                    .get_enable_upgrade_meta_data_to_pb()?,
            )
            .await?;
        Ok(PipelineBuildResult::create())
    }
}
