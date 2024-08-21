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
use databend_common_meta_app::principal::RoleInfo;
use databend_common_sql::plans::CreateRolePlan;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use databend_common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;
use databend_common_users::BUILTIN_ROLE_PUBLIC;
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct CreateRoleInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateRolePlan,
}

impl CreateRoleInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateRolePlan) -> Result<Self> {
        Ok(CreateRoleInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateRoleInterpreter {
    fn name(&self) -> &str {
        "CreateRoleInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "create_role_execute");

        // TODO: add privilege check about CREATE ROLE
        let plan = self.plan.clone();
        let role_name = plan.role_name;
        if role_name.to_lowercase() == BUILTIN_ROLE_ACCOUNT_ADMIN
            || role_name.to_lowercase() == BUILTIN_ROLE_PUBLIC
        {
            return Err(ErrorCode::IllegalRole(
                "Illegal Create Role command. Can not create built-in role [ account_admin | public ]",
            ));
        }

        let tenant = self.ctx.get_tenant();
        let enable_upgrade_meta_data_to_pb = self
            .ctx
            .get_settings()
            .get_enable_upgrade_meta_data_to_pb()?;
        let user_mgr = UserApiProvider::instance();
        user_mgr
            .add_role(
                &tenant,
                RoleInfo::new(&role_name),
                plan.if_not_exists,
                enable_upgrade_meta_data_to_pb,
            )
            .await?;
        RoleCacheManager::instance()
            .force_reload(&tenant, enable_upgrade_meta_data_to_pb)
            .await?;
        Ok(PipelineBuildResult::create())
    }
}
