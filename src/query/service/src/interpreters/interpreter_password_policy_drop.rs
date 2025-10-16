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
use databend_common_sql::plans::DropPasswordPolicyPlan;
use databend_common_users::UserApiProvider;
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct DropPasswordPolicyInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropPasswordPolicyPlan,
}

impl DropPasswordPolicyInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropPasswordPolicyPlan) -> Result<Self> {
        Ok(DropPasswordPolicyInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropPasswordPolicyInterpreter {
    fn name(&self) -> &str {
        "DropPasswordPolicyInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "drop_password_policy_execute");

        let plan = self.plan.clone();
        let tenant = self.ctx.get_tenant();

        let user_mgr = UserApiProvider::instance();
        user_mgr
            .drop_password_policy(&tenant, plan.name.as_str(), plan.if_exists)
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
