// Copyright 2022 Datafuse Labs.
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
use common_planners::PlanNode;
use common_planners::RemoveUserStagePlan;
use common_tracing::tracing;

use super::parse_stage_location;
use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct DfRemoveStage {
    pub location: String,
    pub pattern: String,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfRemoveStage {
    #[tracing::instrument(level = "info", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let pattern = self.pattern.clone();
        let (stage, path) = parse_stage_location(&ctx, &self.location).await?;
        Ok(AnalyzedResult::SimpleQuery(Box::new(
            PlanNode::RemoveUserStage(RemoveUserStagePlan {
                stage,
                path,
                pattern,
            }),
        )))
    }
}
