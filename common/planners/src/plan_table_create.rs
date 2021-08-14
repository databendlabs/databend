// Copyright 2020 Datafuse Labs.
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

use std::collections::HashMap;

use common_datavalues::DataSchemaRef;

/// Types of files to parse as DataFrames
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TableEngineType {
    /// Newline-delimited JSON
    JSONEachRow,
    /// Apache Parquet columnar store
    Parquet,
    /// Comma separated values
    Csv,
    /// Null ENGINE
    Null,
    Memory,
}

impl ToString for TableEngineType {
    fn to_string(&self) -> String {
        match self {
            TableEngineType::JSONEachRow => "JSON".into(),
            TableEngineType::Parquet => "Parquet".into(),
            TableEngineType::Csv => "CSV".into(),
            TableEngineType::Null => "Null".into(),
            TableEngineType::Memory => "Memory".into(),
        }
    }
}

pub type TableOptions = HashMap<String, String>;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct CreateTablePlan {
    pub if_not_exists: bool,
    pub db: String,
    /// The table name
    pub table: String,
    /// The table schema
    pub schema: DataSchemaRef,
    /// The file type of physical file
    pub engine: TableEngineType,
    pub options: TableOptions,
}

impl CreateTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
