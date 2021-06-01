// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchemaRef;

use crate::Expression;
use crate::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct AggregatorPartialPlan {
    pub group_expr: Vec<Expression>,
    pub aggr_expr: Vec<Expression>,
    pub schema: DataSchemaRef,
    pub input: Arc<PlanNode>,
}

impl AggregatorPartialPlan {
    pub fn set_input(&mut self, node: &PlanNode) {
        self.input = Arc::new(node.clone());
    }

    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
