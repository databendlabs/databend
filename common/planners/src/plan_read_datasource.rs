// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataSchemaRef;

use crate::{Partitions, Statistics};

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct ReadDataSourcePlan {
    pub db: String,
    pub table: String,
    pub schema: DataSchemaRef,
    pub partitions: Partitions,
    pub statistics: Statistics,
    pub description: String,
}

impl ReadDataSourcePlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
