// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::datavalues::DataSchemaRef;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct EmptyPlan {
    pub(crate) schema: DataSchemaRef,
}

impl EmptyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
