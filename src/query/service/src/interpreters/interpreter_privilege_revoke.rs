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
use databend_common_meta_app::principal::PrincipalIdentity;
use databend_common_sql::plans::RevokePrivilegePlan;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use databend_common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;
use log::debug;

use crate::interpreters::common::validate_grant_object_exists;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct RevokePrivilegeInterpreter {
    ctx: Arc<QueryContext>,
    plan: RevokePrivilegePlan,
}

impl RevokePrivilegeInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RevokePrivilegePlan) -> Result<Self> {
        Ok(RevokePrivilegeInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for RevokePrivilegeInterpreter {
    fn name(&self) -> &str {
        "RevokePrivilegeInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "revoke_privilege_execute");

        let plan = self.plan.clone();

        for object in &plan.on {
            validate_grant_object_exists(&self.ctx, object).await?;
        }

        // TODO: check user existence
        // TODO: check privilege on granting on the grant object

        let tenant = self.ctx.get_tenant();
        let user_mgr = UserApiProvider::instance();

        match plan.principal {
            PrincipalIdentity::User(user) => {
                for object in plan.on {
                    user_mgr
                        .revoke_privileges_from_user(&tenant, user.clone(), object, plan.priv_types)
                        .await?;
                }
            }
            PrincipalIdentity::Role(role) => {
                if role == BUILTIN_ROLE_ACCOUNT_ADMIN {
                    return Err(ErrorCode::IllegalGrant(
                        "Illegal REVOKE command. Can not revoke built-in role [ account_admin ]",
                    ));
                }
                for object in plan.on {
                    user_mgr
                        .revoke_privileges_from_role(&tenant, &role, object, plan.priv_types)
                        .await?;
                }
                // grant_ownership and grant_privileges_to_role will modify the kv in meta.
                // So we need invalidate the role cache.
                RoleCacheManager::instance().invalidate_cache(&tenant);
            }
        }

        Ok(PipelineBuildResult::create())
    }
}
