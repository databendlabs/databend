// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;
use std::sync::Arc;

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
    fn name(&self) -> String {
        "LimitTransform".to_owned()
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        self.input = input;
        Ok(())
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        Ok(Box::pin(LimitStream::try_create(
            self.input.execute().await?,
            self.limit,
        )?))
    }
}
