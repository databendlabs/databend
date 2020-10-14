// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::error::Result;

use crate::planners::{EmptyPlan, IPlanNode};

pub struct PlanBuilder {
    nodes: Vec<Arc<dyn IPlanNode>>,
}

impl PlanBuilder {
    pub fn default() -> PlanBuilder {
        PlanBuilder { nodes: vec![] }
    }

    pub fn add(&mut self, node: Arc<dyn IPlanNode>) -> &mut PlanBuilder {
        self.nodes.push(node);
        self
    }

    pub fn build(&mut self) -> Result<Vec<Arc<dyn IPlanNode>>> {
        // Remove the empty plan node.
        let empty_name = EmptyPlan {}.name();

        self.nodes.retain(|x| x.name() != empty_name);
        Ok(self.nodes.clone())
    }
}
