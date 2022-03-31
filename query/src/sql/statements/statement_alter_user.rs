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
use common_meta_types::UserIdentity;
use common_planners::AlterUserPlan;
use common_planners::PlanNode;
use common_tracing::tracing;

use super::statement_create_user::DfAuthOption;
use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::statements::DfUserWithOption;

#[derive(Debug, Clone, PartialEq)]
pub struct DfAlterUser {
    pub if_current_user: bool,
    pub user: UserIdentity,
    // None means no change to make
    pub auth_option: Option<DfAuthOption>,
    pub with_options: Vec<DfUserWithOption>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfAlterUser {
    #[tracing::instrument(level = "debug", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let user_info = if self.if_current_user {
            ctx.get_current_user()?
        } else {
            ctx.get_user_manager()
                .get_user(&ctx.get_tenant(), self.user.clone())
                .await?
        };

        let new_auth_info = if let Some(auth_option) = &self.auth_option {
            let auth_info = user_info
                .auth_info
                .alter(&auth_option.auth_type, &auth_option.by_value)?;
            if user_info.auth_info == auth_info {
                None
            } else {
                Some(auth_info)
            }
        } else {
            None
        };

        let mut user_option = user_info.option.clone();
        for option in &self.with_options {
            option.apply(&mut user_option);
        }
        let new_user_option = if user_option == user_info.option {
            None
        } else {
            Some(user_option)
        };

        Ok(AnalyzedResult::SimpleQuery(Box::new(PlanNode::AlterUser(
            AlterUserPlan {
                user: user_info.identity(),
                auth_info: new_auth_info,
                user_option: new_user_option,
            },
        ))))
    }
}
