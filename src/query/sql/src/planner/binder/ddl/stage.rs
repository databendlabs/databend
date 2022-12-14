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
use common_ast::ast::UriLocation;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::OnErrorMode;
use common_meta_types::UserStageInfo;

use super::super::copy::parse_copy_file_format_options;
use super::super::copy::parse_stage_location;
use crate::binder::location::parse_uri_location;
use crate::binder::Binder;
use crate::plans::CreateStagePlan;
use crate::plans::ListPlan;
use crate::plans::Plan;
use crate::plans::RemoveStagePlan;

impl<'a> Binder {
    pub(in crate::planner::binder) async fn bind_list_stage(
        &mut self,
        location: &str,
        pattern: &str,
    ) -> Result<Plan> {
        let stage_name = format!("@{location}");
        let (stage, path) = parse_stage_location(&self.ctx, stage_name.as_str()).await?;
        let plan_node = ListPlan {
            path,
            stage,
            pattern: pattern.to_string(),
        };

        Ok(Plan::ListStage(Box::new(plan_node)))
    }

    pub(in crate::planner::binder) async fn bind_remove_stage(
        &mut self,
        location: &str,
        pattern: &str,
    ) -> Result<Plan> {
        let stage_name = format!("@{location}");
        let (stage, path) = parse_stage_location(&self.ctx, stage_name.as_str()).await?;
        let plan_node = RemoveStagePlan {
            path,
            stage,
            pattern: pattern.to_string(),
        };

        Ok(Plan::RemoveStage(Box::new(plan_node)))
    }

    pub(in crate::planner::binder) async fn bind_create_stage(
        &mut self,
        stmt: &CreateStageStmt,
    ) -> Result<Plan> {
        let CreateStageStmt {
            if_not_exists,
            stage_name,
            location,
            file_format_options,
            on_error,
            size_limit,
            validation_mode: _,
            comments: _,
        } = stmt;

        let mut stage_info = match location {
            None => {
                if stage_name == "~" {
                    UserStageInfo::new_user_stage(&self.ctx.get_current_user()?.name)
                } else {
                    UserStageInfo::new_internal_stage(stage_name)
                }
            }
            Some(uri) => {
                let uri = UriLocation {
                    protocol: uri.protocol.clone(),
                    name: uri.name.clone(),
                    path: uri.path.clone(),
                    connection: uri.connection.clone(),
                };

                let (stage_storage, path) = parse_uri_location(&uri)?;

                UserStageInfo::new_external_stage(stage_storage, &path).with_stage_name(stage_name)
            }
        };

        if !file_format_options.is_empty() {
            stage_info.file_format_options = parse_copy_file_format_options(file_format_options)?;
        }
        // Copy options.
        {
            // on_error.
            if !on_error.is_empty() {
                stage_info.copy_options.on_error =
                    OnErrorMode::from_str(on_error).map_err(ErrorCode::SyntaxException)?;
            }

            stage_info.copy_options.size_limit = *size_limit;
        }

        Ok(Plan::CreateStage(Box::new(CreateStagePlan {
            if_not_exists: *if_not_exists,
            tenant: self.ctx.get_tenant(),
            user_stage_info: stage_info,
        })))
    }
}
