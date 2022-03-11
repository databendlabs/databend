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

use async_trait::async_trait;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::Expression;
use common_streams::CorrectWithSchemaStream;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use futures::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;
use crate::pipelines::transforms::transform_sort_partial::get_sort_descriptions;

pub struct SortMergeTransform {
    schema: DataSchemaRef,
    exprs: Vec<Expression>,
    limit: Option<usize>,
    input: Arc<dyn Processor>,
}

impl SortMergeTransform {
    pub fn try_create(
        schema: DataSchemaRef,
        exprs: Vec<Expression>,
        limit: Option<usize>,
    ) -> Result<Self> {
        Ok(SortMergeTransform {
            schema,
            exprs,
            limit,
            input: Arc::new(EmptyProcessor::create()),
        })
    }
}

#[async_trait]
impl Processor for SortMergeTransform {
    fn name(&self) -> &str {
        "SortMergeTransform"
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

    #[tracing::instrument(level = "debug", name = "sort_merge_execute", skip(self))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        tracing::debug!("execute...");

        let sort_columns_descriptions = get_sort_descriptions(&self.schema, &self.exprs)?;
        let mut blocks = vec![];
        let mut stream = self.input.execute().await?;

        while let Some(block) = stream.next().await {
            blocks.push(block?);
        }

        let results = match blocks.len() {
            0 => vec![],
            _ => vec![DataBlock::merge_sort_blocks(
                &blocks,
                &sort_columns_descriptions,
                self.limit,
            )?],
        };

        Ok(Box::pin(CorrectWithSchemaStream::new(
            Box::pin(DataBlockStream::create(self.schema.clone(), None, results)),
            self.schema.clone(),
        )))
    }
}
