// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use fuse_query_datavalues::DataSchemaRef;

use crate::sql::EngineType;

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
