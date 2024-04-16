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

use std::str::FromStr;

use databend_common_ast::ast::CreateStageStmt;
use databend_common_ast::ast::FileFormatOptions;
use databend_common_ast::ast::UriLocation;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::OnErrorMode;
use databend_common_meta_app::principal::StageInfo;
use databend_common_storage::init_operator;

use super::super::copy_into_table::resolve_stage_location;
use crate::binder::location::parse_storage_params_from_uri;
use crate::binder::Binder;
use crate::plans::CreateStagePlan;
use crate::plans::Plan;
use crate::plans::RemoveStagePlan;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_remove_stage(
        &mut self,
        location: &str,
        pattern: &str,
    ) -> Result<Plan> {
        let (stage, path) = resolve_stage_location(self.ctx.as_ref(), location).await?;
        let plan_node = RemoveStagePlan {
            path,
            stage,
            pattern: pattern.to_string(),
        };

        Ok(Plan::RemoveStage(Box::new(plan_node)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_stage(
        &mut self,
        stmt: &CreateStageStmt,
    ) -> Result<Plan> {
        let CreateStageStmt {
            create_option,
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
                    StageInfo::new_user_stage(&self.ctx.get_current_user()?.name)
                } else {
                    StageInfo::new_internal_stage(stage_name)
                }
            }
            Some(uri) => {
                let mut uri = UriLocation {
                    protocol: uri.protocol.clone(),
                    name: uri.name.clone(),
                    path: uri.path.clone(),
                    part_prefix: uri.part_prefix.clone(),
                    connection: uri.connection.clone(),
                };

                let stage_storage = parse_storage_params_from_uri(
                    &mut uri,
                    Some(self.ctx.as_ref()),
                    "when CREATE STAGE",
                )
                .await?;

                // Check the storage params via init operator.
                let _ = init_operator(&stage_storage).map_err(|err| {
                    ErrorCode::InvalidConfig(format!(
                        "Input storage config for stage is invalid: {err:?}"
                    ))
                })?;

                StageInfo::new_external_stage(stage_storage, true).with_stage_name(stage_name)
            }
        };

        if !file_format_options.is_empty() {
            stage_info.file_format_params =
                self.try_resolve_file_format(file_format_options).await?;
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
            create_option: *create_option,
            tenant: self.ctx.get_tenant(),
            stage_info,
        })))
    }

    #[async_backtrace::framed]
    pub(crate) async fn try_resolve_file_format(
        &self,
        options: &FileFormatOptions,
    ) -> Result<FileFormatParams> {
        let options = options.to_meta_ast();
        if let Some(name) = options.options.get("format_name") {
            self.ctx.get_file_format(name).await
        } else {
            FileFormatParams::try_from(options)
        }
    }
}
