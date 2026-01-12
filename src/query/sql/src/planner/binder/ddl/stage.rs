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

use databend_common_ast::ast::AlterStageAction;
use databend_common_ast::ast::AlterStageStmt;
use databend_common_ast::ast::AlterStageUnsetTarget;
use databend_common_ast::ast::CreateStageStmt;
use databend_common_ast::ast::FileFormatOptions;
use databend_common_ast::ast::UriLocation;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::FileFormatOptionsReader;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::StageInfo;
use databend_common_storage::init_stage_operator;

use super::super::copy_into_table::resolve_stage_location;
use crate::binder::Binder;
use crate::binder::insert::STAGE_PLACEHOLDER;
use crate::binder::location::parse_storage_params_from_uri;
use crate::plans::AlterStageActionPlan;
use crate::plans::AlterStagePlan;
use crate::plans::AlterStageSetPlan;
use crate::plans::AlterStageUnsetPlan;
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
            comments: _,
        } = stmt;

        if stage_name.to_lowercase() == STAGE_PLACEHOLDER {
            return Err(ErrorCode::InvalidArgument(format!(
                "Can not create stage with name `{STAGE_PLACEHOLDER}`, which is reserved"
            )));
        }

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
                    connection: uri.connection.clone(),
                };

                let stage_storage = parse_storage_params_from_uri(
                    &mut uri,
                    Some(self.ctx.as_ref()),
                    "when CREATE STAGE",
                )
                .await?;

                let stage_info =
                    StageInfo::new_external_stage(stage_storage, true).with_stage_name(stage_name);

                init_stage_operator(&stage_info).map_err(|err| {
                    ErrorCode::InvalidConfig(format!(
                        "Input storage config for stage is invalid: {err:?}"
                    ))
                })?;

                stage_info
            }
        };

        if !file_format_options.is_empty() {
            stage_info.file_format_params =
                self.try_resolve_file_format(file_format_options).await?;
        }

        Ok(Plan::CreateStage(Box::new(CreateStagePlan {
            create_option: create_option.clone().into(),
            tenant: self.ctx.get_tenant(),
            stage_info,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_alter_stage(
        &mut self,
        stmt: &AlterStageStmt,
    ) -> Result<Plan> {
        if stmt.stage_name == "~" {
            return Err(ErrorCode::StagePermissionDenied(
                "[SQL-BINDER] User stage (~) is not allowed to be altered",
            ));
        }

        let action = match &stmt.action {
            AlterStageAction::Set(options) => {
                let mut storage_params = None;
                if let Some(location) = options.location.as_ref() {
                    let mut uri = UriLocation {
                        protocol: location.protocol.clone(),
                        name: location.name.clone(),
                        path: location.path.clone(),
                        connection: location.connection.clone(),
                    };
                    let stage_storage = parse_storage_params_from_uri(
                        &mut uri,
                        Some(self.ctx.as_ref()),
                        "when ALTER STAGE",
                    )
                    .await?;
                    let stage_info = StageInfo::new_external_stage(stage_storage.clone(), true)
                        .with_stage_name(&stmt.stage_name);
                    init_stage_operator(&stage_info).map_err(|err| {
                        ErrorCode::InvalidConfig(format!(
                            "Input storage config for stage is invalid: {err:?}"
                        ))
                    })?;
                    storage_params = Some(stage_storage);
                }

                let mut set_plan = AlterStageSetPlan {
                    storage_params,
                    file_format: None,
                    comment: options.comment.clone(),
                };
                if !options.file_format.is_empty() {
                    set_plan.file_format =
                        Some(self.try_resolve_file_format(&options.file_format).await?);
                }

                if set_plan.storage_params.is_none()
                    && set_plan.file_format.is_none()
                    && set_plan.comment.is_none()
                {
                    return Err(ErrorCode::BadArguments(
                        "ALTER STAGE requires at least one option when SET is used",
                    ));
                }

                AlterStageActionPlan::Set(set_plan)
            }
            AlterStageAction::Unset(targets) => {
                let mut unset_plan = AlterStageUnsetPlan::default();
                for target in targets {
                    match target {
                        AlterStageUnsetTarget::FileFormat => unset_plan.file_format = true,
                        AlterStageUnsetTarget::Comment => unset_plan.comment = true,
                    }
                }
                AlterStageActionPlan::Unset(unset_plan)
            }
        };

        Ok(Plan::AlterStage(Box::new(AlterStagePlan {
            if_exists: stmt.if_exists,
            stage_name: stmt.stage_name.clone(),
            action,
        })))
    }

    #[async_backtrace::framed]
    pub(crate) async fn try_resolve_file_format(
        &self,
        options: &FileFormatOptions,
    ) -> Result<FileFormatParams> {
        let reader = FileFormatOptionsReader::from_ast(options);
        if let Some(name) = reader.options.get("format_name") {
            self.ctx.get_file_format(name).await
        } else {
            FileFormatParams::try_from_reader(reader, false)
        }
    }
}
