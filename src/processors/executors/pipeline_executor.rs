// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::datablocks::DataBlock;
use crate::error::Result;
use crate::processors::Processors;

pub struct PipelineExecutor {
    processors: Arc<Processors>,
}

impl PipelineExecutor {
    pub fn create(processors: Arc<Processors>) -> Self {
        PipelineExecutor { processors }
    }

    pub fn execute(&self, _threads: u32) -> Result<()> {
        for x in self.processors.processors() {
            x.work(self.processors.clone())?;
        }
        Ok(())
    }

    pub fn cancel(&self) -> Result<()> {
        Ok(())
    }

    pub fn result(&self) -> Result<DataBlock> {
        self.processors.last_processor()?.input_port().pull()
    }
}
