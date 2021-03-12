// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use crate::processors::IProcessor;

#[derive(Clone)]
pub struct Pipe {
    processors: Vec<Arc<dyn IProcessor>>,
}

impl Pipe {
    pub fn create() -> Self {
        Pipe { processors: vec![] }
    }

    pub fn nums(&self) -> usize {
        self.processors.len()
    }

    pub fn name(&self) -> &str {
        self.processors[0].name()
    }

    pub fn processors(&self) -> Vec<Arc<dyn IProcessor>> {
        self.processors.clone()
    }

    pub fn processor_by_index(&self, index: usize) -> Arc<dyn IProcessor> {
        self.processors[index].clone()
    }

    /// The first processor of the pipe.
    pub fn first(&self) -> Arc<dyn IProcessor> {
        self.processors[0].clone()
    }

    pub fn add(&mut self, processor: Arc<dyn IProcessor>) {
        self.processors.push(processor);
    }
}
