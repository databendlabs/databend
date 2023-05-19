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

use common_catalog::plan::DataSourcePlan;
use common_expression::TableSchemaRef;
use common_meta_app::principal::StageInfo;
use common_meta_types::MetaId;
use common_storage::StageFileInfo;

use crate::plans::Plan;

#[derive(PartialEq, Eq, Clone, Debug)]
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

/// CopyPlan supports CopyIntoTable & CopyIntoStage
#[derive(Clone)]
pub enum CopyPlan {
    NoFileToCopy,
    IntoTable {
        catalog_name: String,
        database_name: String,
        table_name: String,
        table_id: MetaId,
        validation_mode: ValidationMode,
        from: Box<DataSourcePlan>,
        force: bool,
    },
    IntoTableWithTransform {
        catalog_name: String,
        database_name: String,
        table_name: String,
        table_id: MetaId,
        schema: Option<TableSchemaRef>,
        stage_info: Box<StageInfo>,
        validation_mode: ValidationMode,
        from: Box<Plan>,
        need_copy_file_infos: Vec<StageFileInfo>,
        force: bool,
    },
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
            CopyPlan::IntoTable {
                database_name,
                table_name,
                from,
                validation_mode,
                force,
                ..
            } => {
                write!(f, "Copy into {database_name:}.{table_name:}")?;
                write!(f, ", validation_mode: {validation_mode:?}")?;
                write!(f, ", from: {from:?}")?;
                write!(f, " force: {force}")?;
            }
            CopyPlan::IntoTableWithTransform {
                database_name,
                table_name,
                from,
                validation_mode,
                ..
            } => {
                write!(f, "Copy into {database_name:}.{table_name:}")?;
                write!(f, ", validation_mode: {validation_mode:?}")?;
                write!(f, ", from: {from:?}")?;
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
