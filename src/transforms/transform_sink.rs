// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;
use std::sync::Arc;

use crate::error::Result;
use crate::processors::{GraphNode, IProcessor, InputPort, OutputPort, Processors};

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
    fn id(&self) -> u32 {
        self.input.id()
    }

    fn input_port(&self) -> &InputPort {
        &self.input
    }

    fn output_port(&self) -> &OutputPort {
        unimplemented!()
    }

    fn direct_edges(&self) -> Vec<u32> {
        unimplemented!()
    }

    fn back_edges(&self) -> Vec<u32> {
        self.input.edges()
    }

    fn work(&self, processors: Arc<Processors>) -> Result<()> {
        let parent = processors.get_processor(self.input.edges()[0])?;
        self.input.push(parent.output_port().pull()?)?;
        Ok(())
    }
}

impl fmt::Debug for SinkTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "SinkTransform: {:?}", self.input)
    }
}
