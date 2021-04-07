// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use anyhow::Result;
use common_streams::{LimitStream, SendableDataBlockStream};

use crate::pipelines::processors::{EmptyProcessor, IProcessor};

pub struct LimitTransform {
    limit: usize,
    input: Arc<dyn IProcessor>,
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
impl IProcessor for LimitTransform {
    fn name(&self) -> &str {
        "LimitTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> Result<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
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
