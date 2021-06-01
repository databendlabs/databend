// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct UseDatabasePlan {
    pub db: String,
}

impl UseDatabasePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}
