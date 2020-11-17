// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::error::Result;
use crate::planners::PlanNode;

#[derive(Default)]
pub struct PlanBuilder {
    nodes: Vec<PlanNode>,
}

impl PlanBuilder {
    pub fn add(&mut self, node: PlanNode) -> &mut PlanBuilder {
        match node {
            PlanNode::Empty(_) => self,
            _ => {
                self.nodes.push(node);
                self
            }
        }
    }

    pub fn build(&mut self) -> Result<Vec<PlanNode>> {
        Ok(self.nodes.clone())
    }
}
