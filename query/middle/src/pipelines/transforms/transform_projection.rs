// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
use crate::pipelines::processors::Processor;
use crate::pipelines::transforms::ExpressionExecutor;

pub struct ProjectionTransform {
    executor: ExpressionExecutor,
    input: Arc<dyn Processor>,
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
            executor,
            input: Arc::new(EmptyProcessor::create()),
        })
    }
}

#[async_trait::async_trait]
impl Processor for ProjectionTransform {
    fn name(&self) -> &str {
        "ProjectionTransform"
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

    #[tracing::instrument(level = "debug", name = "projection_execute", skip(self))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        tracing::debug!("execute...");

        let executor = self.executor.clone();
        let input_stream = self.input.execute().await?;

        let executor_fn =
            |executor: &ExpressionExecutor, block: Result<DataBlock>| -> Result<DataBlock> {
                let block = block?;
                let start = Instant::now();

                let r = executor.execute(&block);
                let delta = start.elapsed();
                tracing::debug!("Projection cost: {:?}", delta);
                r
            };

        let stream =
            input_stream.filter_map(move |v| executor_fn(&executor, v).map(Some).transpose());

        Ok(Box::pin(stream))
    }
}
