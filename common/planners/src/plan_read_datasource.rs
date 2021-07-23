// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;

use crate::Extras;
use crate::Partitions;
use crate::ScanPlan;
use crate::Statistics;

// TODO: Delete the scan plan field, but it depends on plan_parser:L394
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct ReadDataSourcePlan {
    pub db: String,
    pub table: String,
    pub table_id: MetaId,
    pub table_version: Option<MetaVersion>,
    pub schema: DataSchemaRef,
    pub parts: Partitions,
    pub statistics: Statistics,
    pub description: String,
    pub scan_plan: Arc<ScanPlan>,
    pub remote: bool,
}

impl ReadDataSourcePlan {
    pub fn empty(table_id: u64, table_version: Option<u64>) -> ReadDataSourcePlan {
        ReadDataSourcePlan {
            db: "".to_string(),
            table: "".to_string(),
            table_id,
            table_version,
            schema: Arc::from(DataSchema::empty()),
            parts: vec![],
            statistics: Statistics::default(),
            description: "".to_string(),
            scan_plan: Arc::new(ScanPlan::with_table_id(table_id, table_version)),
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
