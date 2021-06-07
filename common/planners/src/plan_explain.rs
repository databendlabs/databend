// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;

use crate::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq)]
pub enum ExplainType {
    Syntax,
    Graph,
    Pipeline,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct ExplainPlan {
    pub typ: ExplainType,
    pub input: Arc<PlanNode>,
}

impl ExplainPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![DataField::new("explain", DataType::Utf8, false)])
    }

    pub fn set_input(&mut self, node: &PlanNode) {
        self.input = Arc::new(node.clone());
    }
}
