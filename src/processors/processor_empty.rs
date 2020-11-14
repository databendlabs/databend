// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_std::sync::Arc;
use async_trait::async_trait;

use crate::datastreams::{ChunkStream, DataBlockStream};
use crate::error::Result;
use crate::processors::{FormatterSettings, IProcessor};

pub struct EmptyProcessor {}

impl EmptyProcessor {
    pub fn create() -> Self {
        EmptyProcessor {}
    }
}

#[async_trait]
impl IProcessor for EmptyProcessor {
    fn name(&self) -> &'static str {
        "EmptyProcessor"
    }

    fn connect_to(&mut self, _: Arc<dyn IProcessor>) {}

    async fn execute(&self) -> Result<DataBlockStream> {
        Ok(Box::pin(ChunkStream::create(vec![])))
    }

    fn format(
        &self,
        f: &mut std::fmt::Formatter,
        _setting: &mut FormatterSettings,
    ) -> std::fmt::Result {
        write!(f, "")
    }
}
