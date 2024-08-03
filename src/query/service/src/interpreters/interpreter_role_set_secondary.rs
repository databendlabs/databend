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
use databend_common_sql::plans::SetSecondaryRolesPlan;
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct SetSecondaryRolesInterpreter {
    ctx: Arc<QueryContext>,
    plan: SetSecondaryRolesPlan,
}

impl SetSecondaryRolesInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: SetSecondaryRolesPlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for SetSecondaryRolesInterpreter {
    fn name(&self) -> &str {
        "SetSecondaryRolesInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "set_secondary_roles_execute");

        let session = self.ctx.get_current_session();

        let secondary_roles = match self.plan {
            SetSecondaryRolesPlan::None => Some(vec![]),
            SetSecondaryRolesPlan::All => None,
        };
        session.set_secondary_roles_checked(secondary_roles).await?;

        Ok(PipelineBuildResult::create())
    }
}
