// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchemaRef;

use crate::PlanNode;
use crate::PlannerResult;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct SelectPlan {
    pub input: Arc<PlanNode>,
}

impl SelectPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.input.schema()
    }

    pub fn input(&self) -> Arc<PlanNode> {
        self.input.clone()
    }

    pub fn set_input(&mut self, input: &PlanNode) -> PlannerResult<()> {
        self.input = Arc::new(input.clone());
        Ok(())
    }
}
