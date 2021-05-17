// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchemaRef;

use crate::ExpressionAction;
use crate::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct ExpressionPlan {
    pub exprs: Vec<ExpressionAction>,
    pub schema: DataSchemaRef,
    pub input: Arc<PlanNode>,
    pub desc: String
}

impl ExpressionPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }

    pub fn child(&self) -> Arc<PlanNode> {
        self.input.clone()
    }

    pub fn set_child(&mut self, input: &PlanNode) {
        self.input = Arc::new(input.clone());
    }
}
