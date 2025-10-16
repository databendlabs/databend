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
use databend_common_sql::plans::DropNetworkPolicyPlan;
use databend_common_users::UserApiProvider;
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct DropNetworkPolicyInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropNetworkPolicyPlan,
}

impl DropNetworkPolicyInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropNetworkPolicyPlan) -> Result<Self> {
        Ok(DropNetworkPolicyInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropNetworkPolicyInterpreter {
    fn name(&self) -> &str {
        "DropNetworkPolicyInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "drop_network_policy_execute");

        let plan = self.plan.clone();
        let tenant = self.ctx.get_tenant();

        let global_network_policy = self
            .ctx
            .get_settings()
            .get_network_policy()
            .unwrap_or_default();
        if global_network_policy == plan.name {
            return Err(ErrorCode::NetworkPolicyIsUsedByUser(format!(
                "network policy `{}` is global network policy, can't be dropped",
                global_network_policy,
            )));
        }

        let user_mgr = UserApiProvider::instance();
        user_mgr
            .drop_network_policy(&tenant, plan.name.as_str(), plan.if_exists)
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
