// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;

use crate::Expression;
use crate::Extras;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct ScanPlan {
    // The name of the schema
    pub schema_name: String,
    pub table_id: MetaId,
    pub table_version: Option<MetaVersion>,
    // The schema of the source data
    pub table_schema: DataSchemaRef,
    pub table_args: Option<Expression>,
    pub projected_schema: DataSchemaRef,
    // Extras.
    pub push_downs: Extras,
}

impl ScanPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.projected_schema.clone()
    }

    pub fn with_table_id(table_id: u64, table_version: Option<u64>) -> ScanPlan {
        ScanPlan {
            schema_name: "".to_string(),
            table_id,
            table_version,
            table_schema: Arc::new(DataSchema::empty()),
            projected_schema: Arc::new(DataSchema::empty()),
            table_args: None,
            push_downs: Extras::default(),
        }
    }

    pub fn empty() -> Self {
        Self {
            schema_name: "".to_string(),
            table_id: 0,
            table_version: None,
            table_schema: Arc::new(DataSchema::empty()),
            projected_schema: Arc::new(DataSchema::empty()),
            table_args: None,
            push_downs: Extras::default(),
        }
    }
}
