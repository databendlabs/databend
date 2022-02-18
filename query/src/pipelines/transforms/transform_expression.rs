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

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::Expression;
use common_streams::SendableDataBlockStream;
use tokio_stream::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;
use crate::pipelines::transforms::ExpressionExecutor;
/// Executes certain expressions over the block and append the result column to the new block.
/// Aims to transform a block to another format, such as add one or more columns against the Expressions.
///
/// Example:
/// SELECT (number+1) as c1, number as c2 from numbers_mt(10) ORDER BY c1,c2;
/// Expression transform will make two fields on the base field number:
/// Input block columns:
/// |number|
///
/// Append two columns:
/// |c1|c2|
///
/// So the final block:
/// |number|c1|c2|
pub struct ExpressionTransform {
    // The final schema(Build by plan_builder.expression).
    input: Arc<dyn Processor>,
    executor: ExpressionExecutor,
}

impl ExpressionTransform {
    pub fn try_create(
        input_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
        exprs: Vec<Expression>,
    ) -> Result<Self> {
        let executor = ExpressionExecutor::try_create(
            "expression executor",
            input_schema,
            output_schema,
            exprs,
            false,
        )?;
        executor.validate()?;

        Ok(ExpressionTransform {
            input: Arc::new(EmptyProcessor::create()),
            executor,
        })
    }
}

#[async_trait::async_trait]
impl Processor for ExpressionTransform {
    fn name(&self) -> &str {
        "ExpressionTransform"
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
        let executor = self.executor.clone();
        let input_stream = self.input.execute().await?;

        let executor_fn = |executor: &ExpressionExecutor,
                           block: Result<DataBlock>|
         -> Result<DataBlock> { executor.execute(&block?) };

        let stream =
            input_stream.filter_map(move |v| executor_fn(&executor, v).map(Some).transpose());

        Ok(Box::pin(stream))
    }
}
