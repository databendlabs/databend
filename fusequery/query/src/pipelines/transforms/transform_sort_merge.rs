// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::Expression;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;
use crate::pipelines::transforms::transform_sort_partial::get_sort_descriptions;

pub struct SortMergeTransform {
    schema: DataSchemaRef,
    exprs: Vec<ExpressionAction>,
    input: Arc<dyn IProcessor>
}

impl SortMergeTransform {
    pub fn try_create(schema: DataSchemaRef, exprs: Vec<ExpressionAction>) -> Result<Self> {
        Ok(SortMergeTransform {
            schema,
            exprs,
            input: Arc::new(EmptyProcessor::create())
        })
    }
}

#[async_trait]
impl IProcessor for SortMergeTransform {
    fn name(&self) -> &str {
        "SortMergeTransform"
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
                None
            )?]
        };

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            results
        )))
    }
}
