// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

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
    pub plan: Box<PlanNode>,
}
