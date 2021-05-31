// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchemaRef;

use crate::Expression;
use crate::PlanNode;

/// Evaluates an arbitrary list of expressions (essentially a
/// SELECT with an expression list) on its input.
#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct ProjectionPlan {
    /// The list of expressions
    pub expr: Vec<Expression>,
    /// The schema description of the output
    pub schema: DataSchemaRef,
    /// The incoming logical plan
    pub input: Arc<PlanNode>
}

impl ProjectionPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }

    pub fn set_input(&mut self, node: &PlanNode) {
        self.input = Arc::new(node.clone());
    }
}
