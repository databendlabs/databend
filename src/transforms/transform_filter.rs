// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;
use std::sync::Arc;

use crate::datablocks::DataBlock;
use crate::datastreams::{ExpressionStream, SendableDataBlockStream};
use crate::error::FuseQueryResult;
use crate::functions::Function;
use crate::planners::ExpressionPlan;
use crate::processors::{EmptyProcessor, IProcessor};

pub struct FilterTransform {
    pub predicate: ExpressionPlan,
    input: Arc<dyn IProcessor>,
}

impl FilterTransform {
    pub fn create(predicate: ExpressionPlan) -> Self {
        FilterTransform {
            predicate,
            input: Arc::new(EmptyProcessor::create()),
        }
    }

    pub fn expression(block: DataBlock, _: Function) -> FuseQueryResult<DataBlock> {
        Ok(block)
    }
}

#[async_trait]
impl IProcessor for FilterTransform {
    fn name(&self) -> &'static str {
        "FilterTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) {
        self.input = input;
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        Ok(Box::pin(ExpressionStream::try_create(
            self.input.execute().await?,
            Some(self.predicate.to_function()?),
            FilterTransform::expression,
        )?))
    }
}
