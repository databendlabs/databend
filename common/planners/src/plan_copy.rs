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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::str::FromStr;

use common_datavalues::DataSchemaRef;
use common_meta_types::MetaId;

use crate::PlanNode;
use crate::ReadDataSourcePlan;
use crate::StageTableInfo;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub enum ValidationMode {
    None,
    ReturnNRows(u64),
    ReturnErrors,
    ReturnAllErrors,
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
                    Ok(v) => { Ok(ValidationMode::ReturnNRows(v)) }
                    Err(_) => {
                        Err(
                            format!("Unknown validation mode:{:?}, must one of {{ RETURN_<n>_ROWS | RETURN_ERRORS | RETURN_ALL_ERRORS}}", v)
                        )
                    }
                }
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone)]
pub struct CopyPlan {
    pub copy_mode: CopyMode,

    pub validation_mode: ValidationMode,
}

/// CopyPlan supports CopyIntoTable & CopyIntoStage
#[allow(clippy::large_enum_variant)]
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone)]
pub enum CopyMode {
    IntoTable {
        catalog_name: String,
        db_name: String,
        tbl_name: String,
        tbl_id: MetaId,
        files: Vec<String>,
        pattern: String,
        schema: DataSchemaRef,
        from: ReadDataSourcePlan,
    },

    IntoStage {
        stage_table_info: StageTableInfo,
        query: Box<PlanNode>,
    },
}

impl CopyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        match &self.copy_mode {
            CopyMode::IntoTable { schema, .. } => schema.clone(),
            CopyMode::IntoStage {
                stage_table_info, ..
            } => stage_table_info.schema.clone(),
        }
    }
}

impl Debug for CopyPlan {
    // Ignore the schema.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.copy_mode {
            CopyMode::IntoTable {
                db_name,
                tbl_name,
                files,
                pattern,
                from,
                ..
            } => {
                write!(f, "Copy into {:}.{:}", db_name, tbl_name)?;
                write!(f, ", {:?}", from)?;
                if !files.is_empty() {
                    write!(f, " ,files:{:?}", files)?;
                }
                if !pattern.is_empty() {
                    write!(f, " ,pattern:{:?}", pattern)?;
                }
                write!(f, " ,validation_mode:{:?}", self.validation_mode)?;
            }
            CopyMode::IntoStage {
                stage_table_info,
                query,
            } => {
                write!(f, "Copy into {:?}", stage_table_info)?;
                write!(f, ", query: {:?})", query)?;
            }
        }
        Ok(())
    }
}
