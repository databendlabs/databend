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
use common_meta_types::UserPrivilege;
use common_planners::PlanNode;
use common_planners::RevokePrivilegePlan;
use common_tracing::tracing;

use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::statements::DfGrantObject;

#[derive(Debug, Clone, PartialEq)]
pub struct DfRevokeStatement {
    pub username: String,
    pub hostname: String,
    pub priv_types: UserPrivilege,
    pub on: DfGrantObject,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfRevokeStatement {
    #[tracing::instrument(level = "info", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        Ok(AnalyzedResult::SimpleQuery(Box::new(
            PlanNode::RevokePrivilege(RevokePrivilegePlan {
                username: self.username.clone(),
                hostname: self.hostname.clone(),
                on: self.on.convert_to_grant_object(ctx),
                priv_types: self.priv_types,
            }),
        )))
    }
}
