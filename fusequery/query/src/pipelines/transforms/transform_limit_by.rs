// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_exception::Result;
use common_planners::Expression;
use common_streams::LimitByStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;

pub struct LimitByTransform {
    input: Arc<dyn IProcessor>,
    limit_by_exprs: Vec<Expression>,
    limit: usize,
}

impl LimitByTransform {
    pub fn create(limit: usize, limit_by_exprs: Vec<Expression>) -> Self {
        Self {
            input: Arc::new(EmptyProcessor::create()),
            limit,
            limit_by_exprs,
        }
    }
}

#[async_trait::async_trait]
impl IProcessor for LimitByTransform {
    fn name(&self) -> &str {
        "LimitByTransform"
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
        tracing::debug!("execute...");

        Ok(Box::pin(LimitByStream::try_create(
            self.input.execute().await?,
            self.limit,
            self.limit_by_exprs
                .iter()
                .map(|col| col.column_name())
                .collect(),
        )?))
    }
}
