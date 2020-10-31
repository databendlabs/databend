// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;
use std::sync::Arc;

use crate::error::Result;
use crate::processors::{GraphNode, IProcessor, InputPort, OutputPort, Processors};

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
    fn id(&self) -> u32 {
        self.input.id()
    }

    fn input_port(&self) -> &InputPort {
        &self.input
    }

    fn output_port(&self) -> &OutputPort {
        &self.output
    }

    fn direct_edges(&self) -> Vec<u32> {
        self.output.edges()
    }

    fn back_edges(&self) -> Vec<u32> {
        self.input.edges()
    }

    fn work(&self, processors: Arc<Processors>) -> Result<()> {
        let parent = processors.get_processor(self.input.edges()[0])?;
        self.output.push(parent.output_port().pull()?)?;
        Ok(())
    }
}

impl fmt::Debug for SimpleTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "SimpleTransform: {:?}", self.output)
    }
}
