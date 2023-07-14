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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use common_catalog::plan::StageTableInfo;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataSchemaRef;
use common_expression::Scalar;
use common_meta_app::principal::StageInfo;
use common_storage::init_stage_operator;
use common_storage::StageFileInfo;
use tracing::info;

use crate::plans::Plan;

#[derive(PartialEq, Eq, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum ValidationMode {
    None,
    ReturnNRows(u64),
    ReturnErrors,
    ReturnAllErrors,
}

impl FromStr for ValidationMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s.to_uppercase().as_str() {
            "" => Ok(ValidationMode::None),
            "RETURN_ERRORS" => Ok(ValidationMode::ReturnErrors),
            "RETURN_ALL_ERRORS" => Ok(ValidationMode::ReturnAllErrors),
            v => {
                let rows_str = v.replace("RETURN_", "").replace("_ROWS", "");
                let rows = rows_str.parse::<u64>();
                match rows {
                    Ok(v) => Ok(ValidationMode::ReturnNRows(v)),
                    Err(_) => Err(format!(
                        "Unknown validation mode:{v:?}, must one of {{ RETURN_<n>_ROWS | RETURN_ERRORS | RETURN_ALL_ERRORS}}"
                    )),
                }
            }
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Debug, serde::Serialize, serde::Deserialize)]
pub enum CopyIntoTableMode {
    Insert { overwrite: bool },
    Replace,
    Copy,
}

/// To add a new field to this struct,
///
/// 1. If it is easy to derive `Serialize`, add it to `CopyIntoTableSerializable`.
/// 2. Else, add it to `CopyIntoTablePlan`, and modify `CopyInterpreter::build_physical_plan()` to serialize it.
#[derive(Clone)]
pub struct CopyIntoTablePlan {
    pub serializable_part: CopyIntoTableSerializable,
    pub query: Option<Box<Plan>>,

    pub enable_distributed: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CopyIntoTableSerializable {
    pub catalog_name: String,
    pub database_name: String,
    pub table_name: String,

    pub required_values_schema: DataSchemaRef, // ... into table(<columns>) ..  -> <columns>
    pub values_consts: Vec<Scalar>,            // (1, ?, 'a', ?) -> (1, 'a')
    pub required_source_schema: DataSchemaRef, // (1, ?, 'a', ?) -> (?, ?)

    pub write_mode: CopyIntoTableMode,
    pub validation_mode: ValidationMode,
    pub force: bool,
    pub stage_table_info: StageTableInfo,
}

fn set_and_log_status(ctx: &Arc<dyn TableContext>, status: &str) {
    ctx.set_status_info(status);
    info!(status);
}

impl CopyIntoTablePlan {
    pub async fn collect_files(&self, ctx: &Arc<dyn TableContext>) -> Result<Vec<StageFileInfo>> {
        set_and_log_status(ctx, "begin to list files");
        let start = Instant::now();

        let stage_table_info = &self.serializable_part.stage_table_info;
        let max_files = stage_table_info.stage_info.copy_options.max_files;
        let max_files = if max_files == 0 {
            None
        } else {
            Some(max_files)
        };

        let operator = init_stage_operator(&stage_table_info.stage_info)?;
        let all_source_file_infos = if operator.info().can_blocking() {
            if self.serializable_part.force {
                stage_table_info
                    .files_info
                    .blocking_list(&operator, false, max_files)
            } else {
                stage_table_info
                    .files_info
                    .blocking_list(&operator, false, None)
            }
        } else if self.serializable_part.force {
            stage_table_info
                .files_info
                .list(&operator, false, max_files)
                .await
        } else {
            stage_table_info
                .files_info
                .list(&operator, false, None)
                .await
        }?;

        let num_all_files = all_source_file_infos.len();

        info!("end to list files: got {} files", num_all_files);

        let need_copy_file_infos = if self.serializable_part.force {
            info!(
                "force mode, ignore file filtering. ({}.{})",
                &self.serializable_part.database_name, &self.serializable_part.table_name
            );
            all_source_file_infos
        } else {
            // Status.
            set_and_log_status(ctx, "begin to filter out copied files");
            let files = ctx
                .filter_out_copied_files(
                    &self.serializable_part.catalog_name,
                    &self.serializable_part.database_name,
                    &self.serializable_part.table_name,
                    &all_source_file_infos,
                    max_files,
                )
                .await?;

            info!("end filtering out copied files: {}", num_all_files);
            files
        };

        info!(
            "copy: read files with max_files={:?} finished, all:{}, need copy:{}, elapsed:{}",
            max_files,
            num_all_files,
            need_copy_file_infos.len(),
            start.elapsed().as_secs()
        );

        Ok(need_copy_file_infos)
    }
}

impl Debug for CopyIntoTablePlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let CopyIntoTablePlan {
            serializable_part:
                CopyIntoTableSerializable {
                    catalog_name,
                    database_name,
                    table_name,
                    validation_mode,
                    stage_table_info,
                    force,
                    ..
                },
            query,
            ..
        } = self;
        write!(
            f,
            "Copy into {catalog_name:}.{database_name:}.{table_name:}"
        )?;
        write!(f, ", validation_mode: {validation_mode:?}")?;
        write!(f, ", from: {stage_table_info:?}")?;
        write!(f, " force: {force}")?;
        write!(f, " query: {query:?}")?;
        Ok(())
    }
}

/// CopyPlan supports CopyIntoTable & CopyIntoStage
#[derive(Clone)]
pub enum CopyPlan {
    NoFileToCopy,
    IntoTable(CopyIntoTablePlan),
    IntoStage {
        stage: Box<StageInfo>,
        path: String,
        validation_mode: ValidationMode,
        from: Box<Plan>,
    },
}

impl Debug for CopyPlan {
    // Ignore the schema.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            CopyPlan::IntoTable(plan) => {
                write!(f, "{plan:?}")?;
            }
            CopyPlan::IntoStage {
                stage,
                path,
                validation_mode,
                ..
            } => {
                write!(f, "Copy into {stage:?}")?;
                write!(f, ", path: {path:?}")?;
                write!(f, ", validation_mode: {validation_mode:?}")?;
            }
            CopyPlan::NoFileToCopy => {
                write!(f, "No file to copy")?;
            }
        }
        Ok(())
    }
}
