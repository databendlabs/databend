// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use crate::datavalues::DataSchemaRef;
use crate::error::FuseQueryResult;
use crate::planners::{ExpressionPlan, PlanNode};
use crate::sql::EngineType;
use std::collections::HashMap;

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
