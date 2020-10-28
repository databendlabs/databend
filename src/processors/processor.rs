// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::processors::{InputPort, OutputPort};

pub trait IProcessor: fmt::Debug {
    fn get_input_ports(&mut self) -> Vec<&mut InputPort> {
        unimplemented!()
    }

    fn get_output_ports(&mut self) -> Vec<&mut OutputPort> {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct Processors {
    processors: Vec<Box<dyn IProcessor>>,
}

impl Processors {
    pub fn add(&mut self, node: Box<dyn IProcessor>) {
        self.processors.push(node);
    }
}

impl fmt::Debug for Processors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f)?;
        for proc in self.processors.iter() {
            write!(f, "{:?}", proc)?;
        }
        write!(f, "")
    }
}
