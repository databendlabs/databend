// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::sync::Arc;

use crate::datavalues::DataSchemaRef;
use crate::planners::PlanNode;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DFExplainType {
    Syntax,
    Graph,
    Pipeline,
}

#[derive(Clone)]
pub struct ExplainPlan {
    pub typ: DFExplainType,
    pub input: Arc<PlanNode>,
}

impl ExplainPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.input.schema()
    }
}
