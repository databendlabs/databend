// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataSchemaRef;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct EmptyPlan {
    pub schema: DataSchemaRef,
}

impl EmptyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
