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
use databend_common_sql::plans::SetRolePlan;
use databend_common_users::UserApiProvider;
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct SetRoleInterpreter {
    ctx: Arc<QueryContext>,
    plan: SetRolePlan,
}

impl SetRoleInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: SetRolePlan) -> Result<Self> {
        Ok(SetRoleInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for SetRoleInterpreter {
    fn name(&self) -> &str {
        "SetRoleInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "set_role_execute");

        let session = self.ctx.get_current_session();

        if self.plan.is_default {
            let role = session
                .validate_available_role(&self.plan.role_name)
                .await?;
            let current_user = self.ctx.get_current_user()?;
            UserApiProvider::instance()
                .update_user_default_role(
                    &self.ctx.get_tenant(),
                    current_user.identity(),
                    Some(role.name.clone()),
                )
                .await?;
        } else {
            session
                .set_current_role_checked(&self.plan.role_name)
                .await?;
        }
        Ok(PipelineBuildResult::create())
    }
}
