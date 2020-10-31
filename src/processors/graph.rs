// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::error::Result;

#[derive(Default, Clone)]
pub struct GraphNode {
    id: u32,
    edges: Vec<u32>,
}

impl GraphNode {
    pub fn new(id: u32) -> Self {
        GraphNode { id, edges: vec![] }
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn edges(&self) -> Vec<u32> {
        self.edges.clone()
    }

    pub fn add_edge(&mut self, next: u32) -> Result<()> {
        self.edges.push(next);
        Ok(())
    }
}

#[derive(Default)]
pub struct Graph {
    last_id: u32,
}

impl Graph {
    pub fn new_node(&mut self) -> GraphNode {
        let last = self.last_id;
        self.last_id += 1;
        GraphNode::new(last)
    }
}
