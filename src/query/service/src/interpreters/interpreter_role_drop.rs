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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::plans::DropRolePlan;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use databend_common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;
use databend_common_users::BUILTIN_ROLE_PUBLIC;
use log::debug;
use log::warn;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct DropRoleInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropRolePlan,
}

impl DropRoleInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropRolePlan) -> Result<Self> {
        Ok(DropRoleInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropRoleInterpreter {
    fn name(&self) -> &str {
        "DropRoleInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "drop_role_execute");

        // TODO: add privilege check about DROP role
        let plan = self.plan.clone();
        let role_name = plan.role_name.clone();
        if role_name.to_lowercase() == BUILTIN_ROLE_ACCOUNT_ADMIN
            || role_name.to_lowercase() == BUILTIN_ROLE_PUBLIC
        {
            return Err(ErrorCode::IllegalRole(
                "Illegal Drop Role command. Can not drop built-in role [ account_admin | public ]",
            ));
        }
        let tenant = self.ctx.get_tenant();
        UserApiProvider::instance()
            .drop_role(&tenant, plan.role_name, plan.if_exists)
            .await?;

        let session = self.ctx.get_current_session();
        if let Some(current_role) = session.get_current_role() {
            if current_role.name == role_name {
                warn!(
                    "Will drop session current role {}, session current role will be set public role",
                    role_name
                );
                session.unset_current_role().await?;
            }
        }

        RoleCacheManager::instance().force_reload(&tenant).await?;
        Ok(PipelineBuildResult::create())
    }
}
