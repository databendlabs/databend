// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;

use crate::Expression;
use crate::Extras;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct ScanPlan {
    // The name of the schema
    pub schema_name: String,
    // The schema of the source data
    pub table_schema: DataSchemaRef,
    pub table_args: Option<Expression>,
    pub projected_schema: DataSchemaRef,
    // Extras.
    pub extras: Extras,
}

impl ScanPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.projected_schema.clone()
    }

    pub fn empty() -> Self {
        Self {
            schema_name: "".to_string(),
            table_schema: Arc::new(DataSchema::empty()),
            projected_schema: Arc::new(DataSchema::empty()),
            table_args: None,
            extras: Extras::default(),
        }
    }
}
