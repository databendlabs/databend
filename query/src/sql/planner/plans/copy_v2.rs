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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::str::FromStr;

use common_datavalues::DataSchemaRef;
use common_meta_types::MetaId;
use common_meta_types::UserStageInfo;
use common_planners::ReadDataSourcePlan;
use common_planners::StageTableInfo;

use crate::sql::plans::Plan;

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

/// CopyPlan supports CopyIntoTable & CopyIntoStage
#[derive(Clone)]
pub enum CopyPlanV2 {
    IntoTable {
        catalog_name: String,
        database_name: String,
        table_name: String,
        table_id: MetaId,
        files: Vec<String>,
        pattern: String,
        schema: DataSchemaRef,
        validation_mode: ValidationMode,
        from: ReadDataSourcePlan,
    },
    IntoStage {
        stage: UserStageInfo,
        path: String,
        validation_mode: ValidationMode,
        query: Box<Plan>,
    },
}

impl Debug for CopyPlanV2 {
    // Ignore the schema.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            CopyPlanV2::IntoTable {
                database_name,
                table_name,
                files,
                pattern,
                from,
                validation_mode,
                ..
            } => {
                write!(f, "Copy into {:}.{:}", database_name, table_name)?;
                if !files.is_empty() {
                    write!(f, ", files: {:?}", files)?;

                    if !pattern.is_empty() {
                        write!(f, ", pattern: {:?}", pattern)?;
                    }
                    write!(f, ", validation_mode: {:?}", validation_mode)?;
                    write!(f, ", {:?}", from)?;
                }
            }
            CopyPlanV2::IntoStage {
                stage,
                path,
                validation_mode,
                query,
            } => {
                write!(f, "Copy into {:?}", stage)?;
                write!(f, ", path: {:?}", path)?;
                write!(f, ", validation_mode: {:?}", validation_mode)?;
            }
        }
        Ok(())
    }
}
