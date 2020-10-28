// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::error::Result;

#[derive(Default, Clone)]
pub struct EdgeNode {
    direct_edges: Vec<u32>,
    back_edges: Vec<u32>,
}

#[derive(Default, Clone)]
pub struct GraphNode {
    id: u32,
    edge: EdgeNode,
}

impl GraphNode {
    pub fn new(id: u32) -> Self {
        GraphNode {
            id,
            edge: Default::default(),
        }
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn add_direct_edge(&mut self, next: u32) -> Result<()> {
        self.edge.direct_edges.push(next);
        Ok(())
    }

    pub fn add_back_edge(&mut self, back: u32) -> Result<()> {
        self.edge.back_edges.push(back);
        Ok(())
    }
}

impl fmt::Debug for GraphNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for x in self.edge.back_edges.iter() {
            writeln!(f, "{}->{}", x, self.id)?;
        }
        write!(f, "")
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
