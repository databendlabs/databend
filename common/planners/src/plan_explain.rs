// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchemaRef;

use crate::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq)]
pub enum ExplainType {
    Syntax,
    Graph,
    Pipeline
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct ExplainPlan {
    pub typ: ExplainType,
    pub input: Arc<PlanNode>
}

impl ExplainPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.input.schema()
    }
}
