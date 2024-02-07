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
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct ShowNetworkPoliciesInterpreter {
    ctx: Arc<QueryContext>,
}

impl ShowNetworkPoliciesInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>) -> Result<Self> {
        Ok(ShowNetworkPoliciesInterpreter { ctx })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowNetworkPoliciesInterpreter {
    fn name(&self) -> &str {
        "ShowNetworkPoliciesInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let user_mgr = UserApiProvider::instance();
        let network_policies = user_mgr.get_network_policies(&tenant).await?;

        let mut names = Vec::with_capacity(network_policies.len());
        let mut allowed_ip_lists = Vec::with_capacity(network_policies.len());
        let mut blocked_ip_lists = Vec::with_capacity(network_policies.len());
        let mut comments = Vec::with_capacity(network_policies.len());
        for network_policy in network_policies {
            names.push(network_policy.name.clone());
            allowed_ip_lists.push(network_policy.allowed_ip_list.join(",").clone());
            blocked_ip_lists.push(network_policy.blocked_ip_list.join(",").clone());
            comments.push(network_policy.comment.clone());
        }

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(allowed_ip_lists),
            StringType::from_data(blocked_ip_lists),
            StringType::from_data(comments),
        ])])
    }
}
