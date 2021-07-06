// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;
use crate::{PlanNode, Expression};
use common_datavalues::DataSchemaRef;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct CreateSubQueriesSetsPlan {
    pub expressions: Vec<Expression>,
    pub input: Arc<PlanNode>,
}

impl CreateSubQueriesSetsPlan {
    pub fn schema(&self) -> DataSchemaRef {
        // TODO: merge header
        self.input.schema()
    }

    pub fn set_input(&mut self, node: &PlanNode) {
        self.input = Arc::new(node.clone());
    }
}