// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;

use crate::datastreams::{LimitStream, SendableDataBlockStream};
use crate::error::FuseQueryResult;
use crate::processors::{EmptyProcessor, IProcessor};

pub struct LimitTransform {
    limit: usize,
    input: Arc<dyn IProcessor>,
}

impl LimitTransform {
    pub fn try_create(limit: usize) -> FuseQueryResult<Self> {
        Ok(LimitTransform {
            limit,
            input: Arc::new(EmptyProcessor::create()),
        })
    }
}

#[async_trait]
impl IProcessor for LimitTransform {
    fn name(&self) -> &str {
        "LimitTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        Ok(Box::pin(LimitStream::try_create(
            self.input.execute().await?,
            self.limit,
        )?))
    }
}
