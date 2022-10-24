// Copyright 2021 Datafuse Labs.
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

use common_exception::Result;
use common_planner::plans::SetRolePlan;
use common_users::UserApiProvider;

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

    #[tracing::instrument(level = "debug", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let session = self.ctx.get_current_session();
        let current_user = session.get_current_user()?;

        let available_roles = session.get_all_available_roles().await?;
        let role = available_roles
            .iter()
            .find(|r| r.name == self.plan.role_name);
        match role {
            None => {
                let available_role_names = available_roles
                    .iter()
                    .map(|r| r.name.clone())
                    .collect::<Vec<_>>()
                    .join(",");
                return Err(common_exception::ErrorCode::InvalidRole(format!(
                    "Invalid role ({}) for {}, available: {}",
                    self.plan.role_name,
                    current_user.identity(),
                    available_role_names,
                )));
            }
            Some(role) => {
                if self.plan.is_default {
                    let current_user = self.ctx.get_current_user()?;
                    UserApiProvider::instance()
                        .update_user_default_role(
                            &self.ctx.get_tenant(),
                            current_user.identity(),
                            Some(role.name.clone()),
                        )
                        .await?;
                } else {
                    session.set_current_role(Some(role.clone()));
                }
            }
        }
        Ok(PipelineBuildResult::create())
    }
}
