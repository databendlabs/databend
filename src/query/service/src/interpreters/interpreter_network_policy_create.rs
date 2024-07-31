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

use chrono::Utc;
use databend_common_exception::Result;
use databend_common_meta_app::principal::NetworkPolicy;
use databend_common_sql::plans::CreateNetworkPolicyPlan;
use databend_common_users::UserApiProvider;
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct CreateNetworkPolicyInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateNetworkPolicyPlan,
}

impl CreateNetworkPolicyInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateNetworkPolicyPlan) -> Result<Self> {
        Ok(CreateNetworkPolicyInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateNetworkPolicyInterpreter {
    fn name(&self) -> &str {
        "CreateNetworkPolicyInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "create_network_policy_execute");

        let plan = self.plan.clone();
        let tenant = self.ctx.get_tenant();
        let user_mgr = UserApiProvider::instance();

        let network_policy = NetworkPolicy {
            name: plan.name,
            allowed_ip_list: plan.allowed_ip_list,
            blocked_ip_list: plan.blocked_ip_list,
            comment: plan.comment,
            create_on: Utc::now(),
            update_on: None,
        };
        user_mgr
            .add_network_policy(&tenant, network_policy, &plan.create_option)
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
