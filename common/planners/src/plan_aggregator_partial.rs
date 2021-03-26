// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchemaRef;

use crate::PlannerResult;
use crate::{ExpressionPlan, PlanNode};

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct AggregatorPartialPlan {
    pub group_expr: Vec<ExpressionPlan>,
    pub aggr_expr: Vec<ExpressionPlan>,
    pub input: Arc<PlanNode>,
}

impl AggregatorPartialPlan {
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
