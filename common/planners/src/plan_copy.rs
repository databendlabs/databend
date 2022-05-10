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

use crate::ReadDataSourcePlan;

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
    pub catalog_name: String,
    pub db_name: String,
    pub tbl_name: String,
    pub tbl_id: MetaId,
    pub schema: DataSchemaRef,
    pub from: ReadDataSourcePlan,
    pub validation_mode: ValidationMode,
    pub files: Vec<String>,
    pub pattern: String,
}

impl CopyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}

impl Debug for CopyPlan {
    // Ignore the schema.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Copy into {:}.{:}", self.db_name, self.tbl_name)?;
        write!(f, ", {:?}", self.from)?;
        if !self.files.is_empty() {
            write!(f, " ,files:{:?}", self.files)?;
        }
        if !self.pattern.is_empty() {
            write!(f, " ,pattern:{:?}", self.pattern)?;
        }
        write!(f, " ,validation_mode:{:?}", self.validation_mode)
    }
}
