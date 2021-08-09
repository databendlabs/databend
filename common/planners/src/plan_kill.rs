// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct KillPlan {
    pub id: String,
    pub kill_connection: bool,
}

impl KillPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}
