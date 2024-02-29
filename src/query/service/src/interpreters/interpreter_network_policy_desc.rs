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
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_sql::plans::DescNetworkPolicyPlan;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct DescNetworkPolicyInterpreter {
    ctx: Arc<QueryContext>,
    plan: DescNetworkPolicyPlan,
}

impl DescNetworkPolicyInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DescNetworkPolicyPlan) -> Result<Self> {
        Ok(DescNetworkPolicyInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DescNetworkPolicyInterpreter {
    fn name(&self) -> &str {
        "DescNetworkPolicyInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let user_mgr = UserApiProvider::instance();

        let network_policy = user_mgr
            .get_network_policy(&tenant, self.plan.name.as_str())
            .await?;

        let names = vec![network_policy.name.clone()];
        let allowed_ip_lists = vec![network_policy.allowed_ip_list.join(",").clone()];
        let blocked_ip_lists = vec![network_policy.blocked_ip_list.join(",").clone()];
        let comments = vec![network_policy.comment.clone()];

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(allowed_ip_lists),
            StringType::from_data(blocked_ip_lists),
            StringType::from_data(comments),
        ])])
    }
}
