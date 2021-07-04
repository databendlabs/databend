// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataSchemaRef;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct ShowCreateTablePlan {
    pub db: String,
    /// The table name
    pub table: String,
    /// The table schema
    pub schema: DataSchemaRef,
}

impl ShowCreateTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
