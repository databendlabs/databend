// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchemaRef;

use crate::{PlanNode, ExpressionAction};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub enum StageKind {
    Normal,
    Expansive,
    Convergent,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct StagePlan {
    pub kind: StageKind,
    pub input: Arc<PlanNode>,
    pub scatters_expr: ExpressionAction,
}

impl StagePlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.input.schema()
    }

    pub fn set_input(&mut self, node: &PlanNode) {
        self.input = Arc::new(node.clone());
    }
}
