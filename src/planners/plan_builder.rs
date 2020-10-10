// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use super::*;

#[derive(Clone, PartialEq)]
pub struct PlanBuilder {
    nodes: Vec<PlanNode>,
}

impl PlanBuilder {
    pub fn default() -> PlanBuilder {
        PlanBuilder { nodes: vec![] }
    }

    pub fn add(&mut self, node: PlanNode) -> &mut PlanBuilder {
        self.nodes.push(node);
        self
    }

    pub fn build(&mut self) -> Result<Vec<PlanNode>> {
        // Remove the empty plan node.
        self.nodes.retain(|x| !matches!(x, PlanNode::Empty(_)));
        Ok(self.nodes.clone())
    }
}
