// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::processors::{GraphNode, IProcessor, OutputPort};

pub struct SourceTransform {
    output: OutputPort,
}

impl SourceTransform {
    pub fn create(node: GraphNode) -> Box<dyn IProcessor> {
        Box::new(SourceTransform {
            output: OutputPort::new(node),
        })
    }
}

impl IProcessor for SourceTransform {
    fn get_output_ports(&mut self) -> Vec<&mut OutputPort> {
        vec![&mut self.output]
    }
}

impl fmt::Debug for SourceTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.output.node)
    }
}
