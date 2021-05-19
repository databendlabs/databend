// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchemaRef;

use crate::ExpressionAction;
use crate::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct HavingPlan {
    /// The predicate expression, which must have Boolean type.
    pub predicate: ExpressionAction,
    /// The incoming logical plan
    pub input: Arc<PlanNode>
}

impl HavingPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.input.schema()
    }

    pub fn input(&self) -> Arc<PlanNode> {
        self.input.clone()
    }

    pub fn set_input(&mut self, input: &PlanNode) {
        self.input = Arc::new(input.clone());
    }
}
