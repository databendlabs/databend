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

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::AlterUserPlan;
use common_planners::PlanNode;
use common_tracing::tracing;

use super::statement_create_user::DfAuthOption;
use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfAlterUser {
    pub if_current_user: bool,
    /// User name
    pub name: String,
    pub hostname: String,
    // None means no change to make
    pub auth_option: DfAuthOption,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfAlterUser {
    #[tracing::instrument(level = "debug", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let user_info = if self.if_current_user {
            ctx.get_current_user()?
        } else {
            ctx.get_user_manager()
                .get_user(&ctx.get_tenant(), &self.name, &self.hostname)
                .await?
        };

        let new_auth_info = user_info
            .auth_info
            .alter(&self.auth_option.auth_type, &self.auth_option.by_value)
            .map_err(ErrorCode::SyntaxException)?;

        let new_auth_info = if user_info.auth_info == new_auth_info {
            None
        } else {
            Some(new_auth_info)
        };

        Ok(AnalyzedResult::SimpleQuery(Box::new(PlanNode::AlterUser(
            AlterUserPlan {
                name: user_info.name.clone(),
                hostname: user_info.hostname,
                auth_info: new_auth_info,
            },
        ))))
    }
}
