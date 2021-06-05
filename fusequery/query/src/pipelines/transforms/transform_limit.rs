// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_exception::Result;
use common_streams::LimitStream;
use common_streams::SendableDataBlockStream;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;

pub struct LimitTransform {
    limit: usize,
    input: Arc<dyn Processor>,
}

impl LimitTransform {
    pub fn try_create(limit: usize) -> Result<Self> {
        Ok(LimitTransform {
            limit,
            input: Arc::new(EmptyProcessor::create()),
        })
    }
}

#[async_trait::async_trait]
impl Processor for LimitTransform {
    fn name(&self) -> &str {
        "LimitTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn Processor>) -> Result<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn Processor>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        Ok(Box::pin(LimitStream::try_create(
            self.input.execute().await?,
            self.limit,
        )?))
    }
}
