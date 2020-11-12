// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_std::sync::Arc;
use async_trait::async_trait;

use crate::error::Result;
use crate::processors::{FormatterSettings, IProcessor};
use crate::streams::{ChunkStream, DataBlockStream};

pub struct EmptyTransform {}

impl EmptyTransform {
    pub fn create() -> Self {
        EmptyTransform {}
    }
}

#[async_trait]
impl IProcessor for EmptyTransform {
    fn name(&self) -> &'static str {
        "EmptyTransform"
    }

    fn connect_to(&mut self, _: Arc<dyn IProcessor>) {}

    fn format(
        &self,
        f: &mut std::fmt::Formatter,
        _setting: &mut FormatterSettings,
    ) -> std::fmt::Result {
        write!(f, "")
    }

    async fn execute(&self) -> Result<DataBlockStream> {
        Ok(Box::pin(ChunkStream::create(vec![])))
    }
}
