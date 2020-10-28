// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::processors::{GraphNode, IProcessor, InputPort, OutputPort};

pub struct SimpleTransform {
    input: InputPort,
    output: OutputPort,
}

impl SimpleTransform {
    pub fn create(node: GraphNode) -> Box<dyn IProcessor> {
        Box::new(SimpleTransform {
            input: InputPort::new(node.clone()),
            output: OutputPort::new(node),
        })
    }
}

impl IProcessor for SimpleTransform {
    fn get_input_ports(&mut self) -> Vec<&mut InputPort> {
        vec![&mut self.input]
    }

    fn get_output_ports(&mut self) -> Vec<&mut OutputPort> {
        vec![&mut self.output]
    }
}

impl fmt::Debug for SimpleTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.input.node)
    }
}
