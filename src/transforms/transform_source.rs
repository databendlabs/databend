// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_std::sync::Arc;
use async_trait::async_trait;

use crate::datablocks::DataBlock;
use crate::datastreams::{ChunkStream, DataBlockStream};
use crate::error::Result;
use crate::processors::IProcessor;

pub struct SourceTransform {
    data: Vec<DataBlock>,
}

impl SourceTransform {
    pub fn create(data: Vec<DataBlock>) -> Self {
        SourceTransform { data }
    }
}

#[async_trait]
impl IProcessor for SourceTransform {
    fn name(&self) -> &'static str {
        "SourceTransform"
    }

    fn connect_to(&mut self, _: Arc<dyn IProcessor>) {
        unimplemented!()
    }

    async fn execute(&self) -> Result<DataBlockStream> {
        Ok(Box::pin(ChunkStream::create(self.data.clone())))
    }
}
