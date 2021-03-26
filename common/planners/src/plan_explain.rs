// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use crate::datavalues::DataSchemaRef;
use crate::{PlanNode, PlannerResult};

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq)]
pub enum DfExplainType {
    Syntax,
    Graph,
    Pipeline,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct ExplainPlan {
    pub typ: DfExplainType,
    pub input: Arc<PlanNode>,
}

impl ExplainPlan {
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
