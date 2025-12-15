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
use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table_context::FilteredCopyFiles;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::principal::COPY_MAX_FILES_COMMIT_MSG;
use databend_common_meta_app::principal::COPY_MAX_FILES_PER_COMMIT;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_metrics::storage::*;
use databend_common_storage::init_stage_operator;
use log::info;
use opendal::Operator;

use crate::plans::Plan;

#[derive(PartialEq, Eq, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum ValidationMode {
    None,
    ReturnNRows(u64),
    ReturnErrors,
    ReturnAllErrors,
}

impl Display for ValidationMode {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ValidationMode::None => write!(f, ""),
            ValidationMode::ReturnNRows(v) => write!(f, "RETURN_ROWS={v}"),
            ValidationMode::ReturnErrors => write!(f, "RETURN_ERRORS"),
            ValidationMode::ReturnAllErrors => write!(f, "RETURN_ALL_ERRORS"),
        }
    }
}

impl FromStr for ValidationMode {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, String> {
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

impl Display for CopyIntoTableMode {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            CopyIntoTableMode::Insert { overwrite } => {
                if *overwrite {
                    write!(f, "INSERT OVERWRITE")
                } else {
                    write!(f, "INSERT")
                }
            }
            CopyIntoTableMode::Replace => write!(f, "REPLACE"),
            CopyIntoTableMode::Copy => write!(f, "COPY"),
        }
    }
}

impl CopyIntoTableMode {
    pub fn is_overwrite(&self) -> bool {
        match self {
            CopyIntoTableMode::Insert { overwrite } => *overwrite,
            CopyIntoTableMode::Replace => false,
            CopyIntoTableMode::Copy => false,
        }
    }
}

#[derive(Clone)]
pub struct CopyIntoTablePlan {
    pub no_file_to_copy: bool,

    pub catalog_info: Arc<CatalogInfo>,
    pub database_name: String,
    pub table_name: String,
    pub from_attachment: bool,

    // given SQL: ... into table (c1, c2, c3, c4) values (1, ?, 'a', ?)
    // required_values_schema = (c1, c2, c3, c4)
    // required_source_schema = (c2, c4)
    // values_consts = [1, 'a']
    pub required_values_schema: DataSchemaRef,
    pub values_consts: Vec<Scalar>,
    pub required_source_schema: DataSchemaRef,

    pub write_mode: CopyIntoTableMode,
    pub validation_mode: ValidationMode,

    pub stage_table_info: StageTableInfo,
    pub query: Option<Box<Plan>>,
    // query may be Some even if is_transform=false
    pub is_transform: bool,

    // control by setting `enable_distributed_copy`
    // set in optimizer
    pub enable_distributed: bool,

    pub files_collected: bool,

    pub dedup_full_path: bool,
    pub path_prefix: Option<String>,
}

fn get_path_prefix(op: &Operator) -> String {
    let info = op.info();
    let p = format!("{}://{}{}", info.scheme(), info.name(), info.root());
    if p.ends_with('/') {
        p
    } else {
        format!("{}/", p)
    }
}

impl CopyIntoTablePlan {
    pub async fn collect_files(&mut self, ctx: &dyn TableContext) -> Result<()> {
        if self.files_collected {
            return Ok(());
        }
        self.files_collected = true;
        ctx.set_status_info("[COPY-PLANNER] Listing source files");
        let start = Instant::now();

        let stage_table_info = &self.stage_table_info;
        let max_files = stage_table_info.copy_into_table_options.max_files;
        let max_files = if max_files == 0 {
            None
        } else {
            Some(max_files)
        };

        let thread_num = ctx.get_settings().get_max_threads()? as usize;
        let operator = init_stage_operator(&stage_table_info.stage_info)?;
        let options = &stage_table_info.copy_into_table_options;
        let all_source_file_infos = if options.force {
            stage_table_info
                .files_info
                .list(&operator, thread_num, max_files)
                .await
        } else {
            stage_table_info
                .files_info
                .list(&operator, thread_num, None)
                .await
        }?;

        let num_all_files = all_source_file_infos.len();

        let end_get_all_source = Instant::now();
        let cost_get_all_files = end_get_all_source.duration_since(start).as_millis();
        metrics_inc_copy_collect_files_get_all_source_files_milliseconds(cost_get_all_files as u64);

        ctx.set_status_info(&format!(
            "[COPY-PLANNER] Listed {} source files, elapsed: {:?}",
            num_all_files,
            start.elapsed()
        ));

        let (need_copy_file_infos, duplicated) = if options.force {
            if !options.purge && all_source_file_infos.len() > COPY_MAX_FILES_PER_COMMIT {
                return Err(ErrorCode::Internal(format!(
                    "[COPY-PLANNER] {}",
                    COPY_MAX_FILES_COMMIT_MSG
                )));
            }
            info!(
                "[COPY-PLANNER] Force mode enabled, skipping file filtering for table {}.{}",
                &self.database_name, &self.table_name
            );
            (all_source_file_infos, vec![])
        } else {
            // Status.
            ctx.set_status_info("[COPY-PLANNER] Filtering out already copied files");

            let filter_start = Instant::now();
            if self.dedup_full_path {
                let prefix = get_path_prefix(&operator);
                self.path_prefix = Some(prefix.clone());
            };
            let FilteredCopyFiles {
                files_to_copy,
                duplicated_files,
            } = ctx
                .filter_out_copied_files(
                    self.catalog_info.catalog_name(),
                    &self.database_name,
                    &self.table_name,
                    &all_source_file_infos,
                    self.path_prefix.clone(),
                    max_files,
                )
                .await?;
            ctx.set_status_info(&format!(
                "[COPY-PLANNER] Filtered {} files, elapsed: {:?}",
                num_all_files,
                filter_start.elapsed()
            ));

            let end_filter_out = Instant::now();
            let cost_filter_out = end_filter_out
                .duration_since(end_get_all_source)
                .as_millis();
            metrics_inc_copy_filter_out_copied_files_entire_milliseconds(cost_filter_out as u64);

            (files_to_copy, duplicated_files)
        };

        let num_copied_files = need_copy_file_infos.len();
        let copied_bytes: u64 = need_copy_file_infos.iter().map(|i| i.size).sum();

        info!(
            "[COPY-PLANNER] File collection completed - max_files: {:?}, files to copy: {}, bytes: {}, duplicates skipped: {}, elapsed: {:?}",
            max_files,
            need_copy_file_infos.len(),
            copied_bytes,
            num_all_files - num_copied_files,
            start.elapsed()
        );

        if need_copy_file_infos.is_empty() {
            self.no_file_to_copy = true;
        }

        self.stage_table_info.files_to_copy = Some(need_copy_file_infos);
        self.stage_table_info.duplicated_files_detected = duplicated;

        Ok(())
    }
}

impl Debug for CopyIntoTablePlan {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let CopyIntoTablePlan {
            catalog_info,
            database_name,
            table_name,
            no_file_to_copy,
            validation_mode,
            stage_table_info,
            query,
            ..
        } = self;
        write!(
            f,
            "Copy into {:}.{database_name:}.{table_name:}",
            catalog_info.catalog_name()
        )?;
        write!(f, ", no_file_to_copy: {no_file_to_copy:?}")?;
        write!(f, ", validation_mode: {validation_mode:?}")?;
        write!(f, ", from: {stage_table_info:?}")?;
        write!(f, " query: {query:?}")?;
        Ok(())
    }
}

/// CopyPlan supports CopyIntoTable & CopyIntoStage
impl CopyIntoTablePlan {
    fn copy_into_table_schema() -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("File", DataType::String),
            DataField::new("Rows_loaded", DataType::Number(NumberDataType::Int32)),
            DataField::new("Errors_seen", DataType::Number(NumberDataType::Int32)),
            DataField::new(
                "First_error",
                DataType::Nullable(Box::new(DataType::String)),
            ),
            DataField::new(
                "First_error_line",
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int32))),
            ),
        ])
    }

    pub fn schema(&self) -> DataSchemaRef {
        if self.from_attachment {
            Arc::new(DataSchema::empty())
        } else {
            Self::copy_into_table_schema()
        }
    }
}
