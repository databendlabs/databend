// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;
use std::sync::Arc;

use crate::contexts::FuseQueryContext;
use crate::error::FuseQueryResult;
use crate::planners::{FormatterSettings, PlanNode};

#[derive(Clone)]
pub struct ExplainPlan {
    plan: PlanNode,
}

impl ExplainPlan {
    pub fn build_plan(_ctx: Arc<FuseQueryContext>, plan: PlanNode) -> FuseQueryResult<PlanNode> {
        Ok(PlanNode::Explain(Box::new(ExplainPlan { plan })))
    }

    pub fn name(&self) -> &'static str {
        "ExplainPlan"
    }

    pub fn set_description(&mut self, _description: &str) {}

    pub fn format(&self, f: &mut fmt::Formatter, _setting: &mut FormatterSettings) -> fmt::Result {
        write!(f, "{:?}", self.plan)
    }
}

impl fmt::Debug for ExplainPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.plan)
    }
}
