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

pub type HavingTransform = FilterTransform<true>;
pub type WhereTransform = FilterTransform<false>;

pub struct FilterTransform<const HAVING: bool> {
    schema: DataSchemaRef,
    input: Arc<dyn Processor>,
    executor: Arc<ExpressionExecutor>,
}

impl<const HAVING: bool> FilterTransform<HAVING> {
    pub fn try_create(schema: DataSchemaRef, predicate: Expression) -> Result<Self> {
        let predicate_executor = Self::expr_executor(&schema, &predicate)?;
        predicate_executor.validate()?;

        Ok(FilterTransform {
            schema,
            input: Arc::new(EmptyProcessor::create()),
            executor: Arc::new(predicate_executor),
        })
    }

    fn expr_executor(schema: &DataSchemaRef, expr: &Expression) -> Result<ExpressionExecutor> {
        let expr_field = expr.to_data_field(schema)?;
        let expr_schema = DataSchemaRefExt::create(vec![expr_field]);

        ExpressionExecutor::try_create(
            "filter expression executor",
            schema.clone(),
            expr_schema,
            vec![expr.clone()],
            false,
        )
    }

    fn filter(executor: Arc<ExpressionExecutor>, data: DataBlock) -> Result<DataBlock> {
        let filter_block = executor.execute(&data)?;
        DataBlock::filter_block(&data, filter_block.column(0))
    }

    fn filter_map(executor: Arc<ExpressionExecutor>, data: DataBlock) -> Option<Result<DataBlock>> {
        match Self::filter(executor, data) {
            Err(error) => Some(Err(error)),
            Ok(data_block) if data_block.is_empty() => None,
            Ok(data_block) => Some(Ok(data_block)),
        }
    }
}

#[async_trait::async_trait]
impl<const HAVING: bool> Processor for FilterTransform<HAVING> {
    fn name(&self) -> &str {
        match HAVING {
            true => "HavingTransform",
            false => "FilterTransform",
        }
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

    #[tracing::instrument(level = "debug", name = "filter_execute", skip(self))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let input_stream = self.input.execute().await?;
        let executor = self.executor.clone();

        let stream = input_stream.filter_map(move |data_block| match data_block {
            Ok(data_block) if data_block.is_empty() => None,
            Err(fail) => Some(Err(fail)),
            Ok(data_block) => {
                tracing::debug!("execute...");
                let start = Instant::now();
                let res = Self::filter_map(executor.clone(), data_block);
                tracing::debug!("Filter cost: {:?}", start.elapsed());
                res
            }
        });

        Ok(Box::pin(CorrectWithSchemaStream::new(
            Box::pin(stream),
            self.schema.clone(),
        )))
    }
}
