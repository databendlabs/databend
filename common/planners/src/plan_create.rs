// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use common_datavalues::DataSchemaRef;

/// Types of files to parse as DataFrames
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum EngineType {
    /// Newline-delimited JSON
    JsonEachRaw,
    /// Apache Parquet columnar store
    Parquet,
    /// Comma separated values
    Csv,
    /// Null ENGINE
    Null,
}

impl ToString for EngineType {
    fn to_string(&self) -> String {
        match self {
            EngineType::JsonEachRaw => "JSON".into(),
            EngineType::Parquet => "Parquet".into(),
            EngineType::Csv => "CSV".into(),
            EngineType::Null => "Null".into(),
        }
    }
}

pub type TableOptions = HashMap<String, String>;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct CreatePlan {
    pub if_not_exists: bool,
    pub db: String,
    /// The table name
    pub table: String,
    /// The table schema
    pub schema: DataSchemaRef,
    /// The file type of physical file
    pub engine: EngineType,
    pub options: TableOptions,
}

impl CreatePlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
