// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::error::Result;
use crate::processors::GraphNode;

pub struct OutputPort {
    pub node: GraphNode,
}

pub struct InputPort {
    pub node: GraphNode,
}

impl OutputPort {
    pub fn new(node: GraphNode) -> Self {
        OutputPort { node }
    }
}

impl InputPort {
    pub fn new(node: GraphNode) -> Self {
        InputPort { node }
    }
}

pub fn connect(output: &mut OutputPort, input: &mut InputPort) -> Result<()> {
    output.node.add_direct_edge(input.node.id())?;
    input.node.add_back_edge(output.node.id())
}
