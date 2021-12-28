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
use common_meta_types::FileFormat;
use common_meta_types::StageParams;
use common_meta_types::UserStageInfo;
use common_planners::CreateUserStagePlan;
use common_planners::PlanNode;
use common_tracing::tracing;

use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfCreateStage {
    pub if_not_exists: bool,
    pub stage_name: String,
    pub stage_params: StageParams,
    pub file_format: FileFormat,
    pub comments: String,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfCreateStage {
    #[tracing::instrument(level = "debug", skip(self, _ctx), fields(ctx.id = _ctx.get_id().as_str()))]
    async fn analyze(&self, _ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        Ok(AnalyzedResult::SimpleQuery(Box::new(
            PlanNode::CreateUserStage(CreateUserStagePlan {
                if_not_exists: self.if_not_exists,
                user_stage_info: UserStageInfo::new(
                    self.stage_name.as_str(),
                    self.comments.as_str(),
                    self.stage_params.clone(),
                    self.file_format.clone(),
                ),
            }),
        )))
    }
}
