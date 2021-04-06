// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use common_datavalues::DataSchemaRef;

/// Database engine type.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DatabaseEngineType {
    Local,
    Remote,
}

impl ToString for DatabaseEngineType {
    fn to_string(&self) -> String {
        match self {
            DatabaseEngineType::Local => "Local".into(),
            DatabaseEngineType::Remote => "Remote".into(),
        }
    }
}

pub type DatabaseOptions = HashMap<String, String>;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct CreateDatabasePlan {
    pub if_not_exists: bool,
    pub db: String,
    pub schema: DataSchemaRef,
    pub engine: DatabaseEngineType,
    pub options: DatabaseOptions,
}

impl CreateDatabasePlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
