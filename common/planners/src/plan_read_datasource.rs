// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;

use crate::Extras;
use crate::Partitions;
use crate::ScanPlan;
use crate::Statistics;

// TODO: Delete the scan plan field, but it depends on plan_parser:L394
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct ReadDataSourcePlan {
    pub db: String,
    pub table: String,
    pub schema: DataSchemaRef,
    pub parts: Partitions,
    pub statistics: Statistics,
    pub description: String,
    pub scan_plan: Arc<ScanPlan>,
    pub remote: bool,
}

impl ReadDataSourcePlan {
    pub fn empty() -> ReadDataSourcePlan {
        ReadDataSourcePlan {
            db: "".to_string(),
            table: "".to_string(),
            schema: Arc::from(DataSchema::empty()),
            parts: vec![],
            statistics: Statistics::default(),
            description: "".to_string(),
            scan_plan: Arc::new(ScanPlan::empty()),
            remote: false,
        }
    }

    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }

    /// Get the push downs.
    pub fn get_push_downs(&self) -> Extras {
        self.scan_plan.push_downs.clone()
    }
}
