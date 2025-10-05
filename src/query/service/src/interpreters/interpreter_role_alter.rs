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
use databend_common_sql::plans::AlterRolePlan;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct AlterRoleInterpreter {
    ctx: Arc<QueryContext>,
    plan: AlterRolePlan,
}

impl AlterRoleInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AlterRolePlan) -> Result<Self> {
        Ok(AlterRoleInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AlterRoleInterpreter {
    fn name(&self) -> &str {
        "AlterRoleInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "alter_role_execute");

        let plan = self.plan.clone();
        let tenant = self.ctx.get_tenant();
        let user_mgr = UserApiProvider::instance();

        // Update role based on action
        match &plan.action {
            databend_common_sql::plans::AlterRoleAction::Comment(comment) => {
                match user_mgr
                    .update_role_comment(&tenant, &plan.role_name, comment.clone())
                    .await
                {
                    Ok(_) => {
                        // Force reload role cache after altering
                        RoleCacheManager::instance().force_reload(&tenant).await?;
                        Ok(PipelineBuildResult::create())
                    }
                    Err(e) => {
                        if e.code() == ErrorCode::UNKNOWN_ROLE && plan.if_exists {
                            Ok(PipelineBuildResult::create())
                        } else {
                            Err(e.add_message_back("(while updating role comment)"))
                        }
                    }
                }
            }
        }
    }
}
