// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::Expression;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use tokio_stream::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;
use crate::pipelines::transforms::ExpressionExecutor;

pub struct ProjectionTransform {
    executor: Arc<ExpressionExecutor>,
    input: Arc<dyn IProcessor>,
}

impl ProjectionTransform {
    pub fn try_create(
        input_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
        exprs: Vec<Expression>,
    ) -> Result<Self> {
        let executor = ExpressionExecutor::try_create(
            "projection executor",
            input_schema,
            output_schema,
            exprs,
            true,
        )?;

        Ok(ProjectionTransform {
            executor: Arc::new(executor),
            input: Arc::new(EmptyProcessor::create()),
        })
    }
}

#[async_trait::async_trait]
impl IProcessor for ProjectionTransform {
    fn name(&self) -> &str {
        "ProjectionTransform"
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
        tracing::info!("execute...");

        let executor = self.executor.clone();
        let input_stream = self.input.execute().await?;

        let executor_fn =
            |executor: Arc<ExpressionExecutor>, block: Result<DataBlock>| -> Result<DataBlock> {
                let block = block?;
                let start = Instant::now();
                let r = executor.execute(&block);
                let delta = start.elapsed();
                tracing::info!("Projection cost: {:?}", delta);
                r
            };

        let stream = input_stream
            .filter_map(move |v| executor_fn(executor.clone(), v).map(Some).transpose());

        Ok(Box::pin(stream))
    }
}
