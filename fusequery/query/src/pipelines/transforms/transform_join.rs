// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::Arc;

use common_arrow::arrow::array::new_empty_array;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;

use crate::pipelines::processors::IProcessor;

pub struct NestedLoopJoinTransform {
    schema: DataSchemaRef,
    left: Arc<dyn IProcessor>,
    right: Arc<dyn IProcessor>
}

impl NestedLoopJoinTransform {
    pub fn try_create(
        schema: DataSchemaRef,
        left: Arc<dyn IProcessor>,
        right: Arc<dyn IProcessor>
    ) -> Result<Self> {
        Ok(NestedLoopJoinTransform {
            schema: schema.clone(),
            left: left.clone(),
            right: right.clone()
        })
    }

    async fn read_from_sink(sink: Arc<dyn IProcessor>) -> Result<Vec<DataBlock>> {
        let mut buffer = vec![];
        while let Some(block) = sink.execute().await?.next().await {
            buffer.push(block?);
        }
        Ok(buffer)
    }
}

#[async_trait::async_trait]
impl IProcessor for NestedLoopJoinTransform {
    fn name(&self) -> &str {
        "NestedLoopJoinTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> Result<()> {
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let read_left_task = tokio::task::spawn(Self::read_from_sink(self.left.clone()));
        let read_right_task = tokio::task::spawn(Self::read_from_sink(self.right.clone()));

        let left_buffer = read_left_task.await.unwrap()?;
        let right_buffer = read_right_task.await.unwrap()?;

        let result_columns = self
            .schema
            .fields()
            .into_iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect();
        let result_block = DataBlock::create(self.schema.clone(), result_columns.clone());

        let result = right_buffer
            .iter()
            .flat_map(|outer_block| {
                left_buffer.iter().flat_map(|inner_block| {
                    let mut joined_blocks = vec![];
                    for i in 0..inner_block.num_rows() {
                        let block = result_block.clone();
                        for j in 0..outer_block.num_rows() {
                            let columns = block.columns();
                        }
                    }
                    joined_blocks
                })
            })
            .collect();

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            result
        )))
    }
}
