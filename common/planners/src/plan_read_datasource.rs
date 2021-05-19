// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;

use crate::Partitions;
use crate::Statistics;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct ReadDataSourcePlan {
    pub db: String,
    pub table: String,
    pub schema: DataSchemaRef,
    pub partitions: Partitions,
    pub statistics: Statistics,
    pub description: String
}

impl ReadDataSourcePlan {
    pub fn empty() -> ReadDataSourcePlan {
        ReadDataSourcePlan {
            db: "".to_string(),
            table: "".to_string(),
            schema: Arc::from(DataSchema::empty()),
            partitions: vec![],
            statistics: Statistics::default(),
            description: "".to_string()
        }
    }

    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
