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
use common_legacy_planners::CreateUserStagePlan;
use common_legacy_planners::ListPlan;
use common_legacy_planners::RemoveUserStagePlan;
use common_meta_types::OnErrorMode;
use common_meta_types::StageType;
use common_meta_types::UserStageInfo;
use common_storage::parse_uri_location;
use common_storage::UriLocation;

use crate::sessions::TableContext;
use crate::sql::binder::Binder;
use crate::sql::plans::Plan;
use crate::sql::statements::parse_copy_file_format_options;
use crate::sql::statements::parse_stage_location;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_list_stage(
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

    pub(in crate::sql::planner::binder) async fn bind_remove_stage(
        &mut self,
        location: &str,
        pattern: &str,
    ) -> Result<Plan> {
        let stage_name = format!("@{location}");
        let (stage, path) = parse_stage_location(&self.ctx, stage_name.as_str()).await?;
        let plan_node = RemoveUserStagePlan {
            path,
            stage,
            pattern: pattern.to_string(),
        };

        Ok(Plan::RemoveStage(Box::new(plan_node)))
    }

    pub(in crate::sql::planner::binder) async fn bind_create_stage(
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
            None => UserStageInfo {
                stage_type: StageType::Internal,
                ..Default::default()
            },
            Some(uri) => {
                let uri = UriLocation {
                    protocol: uri.protocol.clone(),
                    name: uri.name.clone(),
                    path: uri.path.clone(),
                    connection: uri.connection.clone(),
                };

                // Make sure the behavior under older planner is the same
                // with before which always use the same endpoint with config.
                //
                // TODO: remove me while removing old planner
                // if self.ctx.get_settings().get_enable_planner_v2()? == 0 {
                //     if let StorageParams::S3(v) = self.ctx.get_config().storage.params {
                //         uri.connection
                //             .insert("endpoint_url".to_string(), v.endpoint_url);
                //     }
                // }

                let (stage_storage, path) = parse_uri_location(&uri)?;

                UserStageInfo::new_external_stage(stage_storage, &path)
            }
        };
        stage_info.stage_name = stage_name.clone();

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

        Ok(Plan::CreateStage(Box::new(CreateUserStagePlan {
            if_not_exists: *if_not_exists,
            tenant: self.ctx.get_tenant(),
            user_stage_info: stage_info,
        })))
    }
}
