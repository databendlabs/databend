// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;
use std::sync::Arc;

use crate::datablocks::DataBlock;
use crate::error::Result;
use crate::processors::{GraphNode, IProcessor, InputPort, OutputPort, Processors};

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
    fn id(&self) -> u32 {
        self.output.id()
    }

    fn input_port(&self) -> &InputPort {
        unimplemented!()
    }

    fn output_port(&self) -> &OutputPort {
        &self.output
    }

    fn direct_edges(&self) -> Vec<u32> {
        self.output.edges()
    }

    fn back_edges(&self) -> Vec<u32> {
        vec![]
    }

    fn work(&self, _processors: Arc<Processors>) -> Result<()> {
        self.output.push(DataBlock::empty())?;
        Ok(())
    }
}

impl fmt::Debug for SourceTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "SourceTransform: {:?}", self.output)
    }
}
