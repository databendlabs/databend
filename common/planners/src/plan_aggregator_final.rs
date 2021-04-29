// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::Result;

use crate::ExpressionPlan;
use crate::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct AggregatorFinalPlan {
    pub aggr_expr: Vec<ExpressionPlan>,
    pub group_expr: Vec<ExpressionPlan>,
    pub schema: DataSchemaRef,
    pub input: Arc<PlanNode>
}

impl AggregatorFinalPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }

    pub fn input(&self) -> Arc<PlanNode> {
        self.input.clone()
    }

    pub fn set_input(&mut self, input: &PlanNode) -> Result<()> {
        self.input = Arc::new(input.clone());
        Ok(())
    }
}
