// Copyright 2020 Datafuse Labs.
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
use std::convert::TryInto;
use std::sync::Arc;
use std::time::Instant;

use common_arrow::arrow;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::Expression;
use common_streams::CorrectWithSchemaStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use tokio_stream::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;
use crate::pipelines::transforms::ExpressionExecutor;

pub struct FilterTransform {
    schema: DataSchemaRef,
    input: Arc<dyn Processor>,
    executor: Arc<ExpressionExecutor>,
    predicate: Expression,
    having: bool,
}

impl FilterTransform {
    pub fn try_create(schema: DataSchemaRef, predicate: Expression, having: bool) -> Result<Self> {
        let mut fields = schema.fields().clone();
        fields.push(predicate.to_data_field(&schema)?);

        let executor = ExpressionExecutor::try_create(
            "filter executor",
            schema.clone(),
            DataSchemaRefExt::create(fields),
            vec![predicate.clone()],
            false,
        )?;
        executor.validate()?;

        Ok(FilterTransform {
            schema,
            input: Arc::new(EmptyProcessor::create()),
            executor: Arc::new(executor),
            predicate,
            having,
        })
    }
}

#[async_trait::async_trait]
impl Processor for FilterTransform {
    fn name(&self) -> &str {
        if self.having {
            return "HavingTransform";
        }
        "FilterTransform"
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
        let input_stream = self.input.execute().await?;
        let executor = self.executor.clone();
        let column_name = self.predicate.column_name();

        let execute_fn = |executor: Arc<ExpressionExecutor>,
                          column_name: &str,
                          block: Result<DataBlock>|
         -> Result<DataBlock> {
            tracing::debug!("execute...");
            let start = Instant::now();

            let block = block?;
            let filter_block = executor.execute(&block)?;
            let filter_array = filter_block.try_column_by_name(column_name)?.to_array()?;
            // Downcast to boolean array
            let filter_array = filter_array.cast_with_type(&DataType::Boolean)?;
            let filter_array = filter_array.bool()?.downcast_ref();
            // Convert to arrow record_batch

            let mut filter_exit_true = filter_array.values().chunks::<u64>();

            if !filter_exit_true.any(|p| p > 0) && !filter_exit_true.remainder_iter().any(|p| p) {
                return Ok(DataBlock::empty());
            }
            let batch = block.try_into()?;
            let batch = arrow::compute::filter::filter_record_batch(&batch, filter_array)?;

            let delta = start.elapsed();
            tracing::debug!("Filter cost: {:?}", delta);
            batch.try_into()
        };
        let stream =
            input_stream.filter_map(
                move |v| match execute_fn(executor.clone(), &column_name, v) {
                    Err(error) => Some(Err(error)),
                    Ok(data_block) if data_block.is_empty() => None,
                    Ok(data_block) => Some(Ok(data_block)),
                },
            );

        Ok(Box::pin(CorrectWithSchemaStream::new(
            Box::pin(stream),
            self.schema.clone(),
        )))
    }
}
