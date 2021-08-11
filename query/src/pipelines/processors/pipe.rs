// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use crate::pipelines::processors::Processor;

#[derive(Clone)]
pub struct Pipe {
    processors: Vec<Arc<dyn Processor>>,
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

    pub fn processors(&self) -> Vec<Arc<dyn Processor>> {
        self.processors.clone()
    }

    pub fn processor_by_index(&self, index: usize) -> Arc<dyn Processor> {
        self.processors[index].clone()
    }

    /// The first processor of the pipe.
    pub fn first(&self) -> Arc<dyn Processor> {
        self.processors[0].clone()
    }

    pub fn add(&mut self, processor: Arc<dyn Processor>) {
        self.processors.push(processor);
    }
}
