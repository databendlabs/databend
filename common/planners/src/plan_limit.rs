// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchemaRef;

use crate::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct LimitPlan {
    /// The limit
    pub n: usize,
    /// The logical plan
    pub input: Arc<PlanNode>,
}

impl LimitPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.input.schema()
    }
}
