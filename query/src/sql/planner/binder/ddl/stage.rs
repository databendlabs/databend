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

use std::str::FromStr;

use common_ast::ast::CreateStageStmt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::OnErrorMode;
use common_meta_types::StageType;
use common_meta_types::UserStageInfo;
use common_planners::CreateUserStagePlan;
use common_planners::ListPlan;

use crate::sql::binder::Binder;
use crate::sql::plans::Plan;
use crate::sql::statements::parse_copy_file_format_options;
use crate::sql::statements::parse_stage_location;
use crate::sql::statements::parse_uri_location;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_list_stage(
        &mut self,
        name: &str,
        pattern: &str,
    ) -> Result<Plan> {
        if !name.starts_with('@') {
            return Err(ErrorCode::SyntaxException(
                "Stage uri must be started with @, for example: '@stage_name[/<path>/]'",
            ));
        }
        let (stage, path) = parse_stage_location(&self.ctx, name).await?;
        let plan_node = ListPlan {
            path,
            stage,
            pattern: pattern.to_string(),
        };

        Ok(Plan::ListStage(Box::new(plan_node)))
    }

    pub(in crate::sql::planner::binder) async fn bind_create_stage(
        &mut self,
        stmt: &CreateStageStmt,
    ) -> Result<Plan> {
        let mut stage_info = match stmt.location.is_empty() {
            true => UserStageInfo {
                stage_type: StageType::Internal,
                ..Default::default()
            },
            false => {
                let (stage_storage, _) = parse_uri_location(
                    &stmt.location,
                    &stmt.credential_options,
                    &stmt.encryption_options,
                )?;

                stage_storage
            }
        };
        stage_info.stage_name = stmt.stage_name.clone();

        if !stmt.file_format_options.is_empty() {
            stage_info.file_format_options =
                parse_copy_file_format_options(&stmt.file_format_options)?;
        }
        // Copy options.
        {
            // on_error.
            if !stmt.on_error.is_empty() {
                stage_info.copy_options.on_error =
                    OnErrorMode::from_str(&stmt.on_error).map_err(ErrorCode::SyntaxException)?;
            }

            stage_info.copy_options.size_limit = stmt.size_limit;
        }

        Ok(Plan::CreateStage(Box::new(CreateUserStagePlan {
            if_not_exists: stmt.if_not_exists,
            tenant: self.ctx.get_tenant(),
            user_stage_info: stage_info,
        })))
    }
}
