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

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::OnErrorMode;
use common_meta_types::StageType;
use common_meta_types::UserStageInfo;
use common_planners::CreateUserStagePlan;
use common_planners::PlanNode;
use common_tracing::tracing;

use super::parse_copy_file_format_options;
use super::parse_uri_location;
use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct DfCreateUserStage {
    pub if_not_exists: bool,
    pub stage_name: String,

    pub location: String,
    pub credential_options: BTreeMap<String, String>,
    pub encryption_options: BTreeMap<String, String>,

    pub file_format_options: BTreeMap<String, String>,
    pub on_error: String,
    pub size_limit: String,
    pub validation_mode: String,
    pub comments: String,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfCreateUserStage {
    #[tracing::instrument(level = "info", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let mut stage_info = match self.location.is_empty() {
            true => UserStageInfo {
                stage_type: StageType::Internal,
                ..Default::default()
            },
            false => {
                let (stage_storage, _) = parse_uri_location(
                    &self.location,
                    &self.credential_options,
                    &self.encryption_options,
                )?;

                stage_storage
            }
        };
        stage_info.stage_name = self.stage_name.clone();

        if !self.file_format_options.is_empty() {
            stage_info.file_format_options =
                parse_copy_file_format_options(&self.file_format_options)?;
        }
        // Copy options.
        {
            // on_error.
            if !self.on_error.is_empty() {
                stage_info.copy_options.on_error =
                    OnErrorMode::from_str(&self.on_error).map_err(ErrorCode::SyntaxException)?;
            }

            // size_limit.
            if !self.size_limit.is_empty() {
                let size_limit = self.size_limit.parse::<usize>().map_err(|_e| {
                    ErrorCode::SyntaxException(format!(
                        "size_limit must be number, got: {}",
                        self.size_limit
                    ))
                })?;
                stage_info.copy_options.size_limit = size_limit;
            }
        }

        Ok(AnalyzedResult::SimpleQuery(Box::new(
            PlanNode::CreateUserStage(CreateUserStagePlan {
                if_not_exists: self.if_not_exists,
                tenant: ctx.get_tenant(),
                user_stage_info: stage_info,
            }),
        )))
    }
}
