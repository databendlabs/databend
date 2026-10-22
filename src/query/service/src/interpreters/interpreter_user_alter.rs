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
use databend_common_meta_app::principal::BUILTIN_ROLE_PUBLIC;
use databend_common_sql::plans::AlterUserPlan;
use databend_common_users::UserApiProvider;
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextQueryIdentity;
use crate::sessions::TableContextTableAccess;

#[derive(Debug)]
pub struct AlterUserInterpreter {
    ctx: Arc<QueryContext>,
    plan: AlterUserPlan,
}

impl AlterUserInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AlterUserPlan) -> Result<Self> {
        Ok(AlterUserInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AlterUserInterpreter {
    fn name(&self) -> &str {
        "AlterUserInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            debug!("ctx.id" = self.ctx.get_id().as_str(); "alter_user_execute");

            let plan = &self.plan;
            let tenant = self.ctx.get_tenant();
            let user_mgr = UserApiProvider::instance();
            if let Some(role) = &plan.default_role {
                user_mgr.get_role(&tenant, role.clone()).await?;
                // PUBLIC is implicitly available to every user. Other default roles
                // must already be granted; changing the default never adds a grant.
                if role != BUILTIN_ROLE_PUBLIC && !plan.user_info.grants.roles().contains(role) {
                    return Err(ErrorCode::InvalidRole(format!(
                        "Role '{}' is not granted to user {}",
                        role,
                        plan.user_info.identity().display()
                    )));
                }
            }
            if plan.change_auth || plan.change_user_option {
                user_mgr
                    .alter_user(&tenant, &plan.user_info, plan.seq)
                    .await?;
            }

            Ok(PipelineBuildResult::create())
        })
    }
}
