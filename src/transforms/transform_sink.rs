// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::processors::{GraphNode, IProcessor, InputPort};

pub struct SinkTransform {
    input: InputPort,
}

impl SinkTransform {
    pub fn create(node: GraphNode) -> Box<dyn IProcessor> {
        Box::new(SinkTransform {
            input: InputPort::new(node),
        })
    }
}

impl IProcessor for SinkTransform {
    fn get_input_ports(&mut self) -> Vec<&mut InputPort> {
        vec![&mut self.input]
    }
}

impl fmt::Debug for SinkTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.input.node)
    }
}
