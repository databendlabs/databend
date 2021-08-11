// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_exception::Result;
use common_streams::SendableDataBlockStream;
use common_streams::SkipStream;
use common_streams::TakeStream;
use common_tracing::tracing;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;

pub struct LimitTransform {
    limit: Option<usize>,
    offset: usize,
    input: Arc<dyn Processor>,
}

impl LimitTransform {
    pub fn try_create(limit: Option<usize>, offset: usize) -> Result<Self> {
        Ok(LimitTransform {
            limit,
            offset,
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
        tracing::debug!("execute...");
        let input_stream = self.input.execute().await?;
        Ok(Box::pin(match (self.limit, self.offset) {
            (None, 0) => input_stream,
            (None, offset) => Box::pin(SkipStream::new(Box::pin(input_stream), offset)),
            (Some(limit), 0) => Box::pin(TakeStream::new(input_stream, limit)),
            (Some(limit), offset) => Box::pin(TakeStream::new(
                Box::pin(SkipStream::new(input_stream, offset)),
                limit,
            )),
        }))
    }
}
