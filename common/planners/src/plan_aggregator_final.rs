// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchemaRef;

use crate::ExpressionAction;
use crate::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct AggregatorFinalPlan {
    pub aggr_expr: Vec<ExpressionAction>,
    pub group_expr: Vec<ExpressionAction>,
    pub schema: DataSchemaRef,
    pub input: Arc<PlanNode>
}

impl AggregatorFinalPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
