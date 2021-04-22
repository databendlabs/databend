// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use common_datavalues::DataSchemaRef;

/// Types of files to parse as DataFrames
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TableEngineType {
    /// Newline-delimited JSON
    JsonEachRaw,
    /// Apache Parquet columnar store
    Parquet,
    /// Comma separated values
    Csv,
    /// Null ENGINE
    Null
}

impl ToString for TableEngineType {
    fn to_string(&self) -> String {
        match self {
            TableEngineType::JsonEachRaw => "JSON".into(),
            TableEngineType::Parquet => "Parquet".into(),
            TableEngineType::Csv => "CSV".into(),
            TableEngineType::Null => "Null".into()
        }
    }
}

pub type TableOptions = HashMap<String, String>;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CreateTablePlan {
    pub if_not_exists: bool,
    pub db: String,
    /// The table name
    pub table: String,
    /// The table schema
    pub schema: DataSchemaRef,
    /// The file type of physical file
    pub engine: TableEngineType,
    pub options: TableOptions
}

impl CreateTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
