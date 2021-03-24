// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use crate::datavalues::DataSchemaRef;
use crate::error::FuseQueryResult;
use crate::planners::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq)]
pub enum DFExplainType {
    Syntax,
    Graph,
    Pipeline,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct ExplainPlan {
    pub typ: DFExplainType,
    pub input: Arc<PlanNode>,
}

impl ExplainPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.input.schema()
    }

    pub fn input(&self) -> Arc<PlanNode> {
        self.input.clone()
    }

    pub fn set_input(&mut self, input: &PlanNode) -> FuseQueryResult<()> {
        self.input = Arc::new(input.clone());
        Ok(())
    }
}
