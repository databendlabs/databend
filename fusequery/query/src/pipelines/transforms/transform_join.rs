// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
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

        // let result = left_buffer
        //     .into_iter()
        //     .map(|inner_block| {
        //         right_buffer
        //             .iter()
        //             .cloned()
        //             .map(|outer_block| {
        //                 let mut joined_blocks: Vec<DataBlock> = vec![];
        //                 for i in 0..inner_block.num_rows() {
        //                     let mut result_columns = vec![];
        //                     for column in inner_block.columns() {
        //                         let inner_value = DataValue::try_from_array(column, i).unwrap();
        //                         result_columns.push(
        //                             inner_value
        //                                 .to_array_with_size(outer_block.num_rows())
        //                                 .unwrap()
        //                         );
        //                     }
        //                     for column in outer_block.columns() {
        //                         result_columns.push(column.clone());
        //                     }
        //                     joined_blocks
        //                         .push(DataBlock::create(self.schema.clone(), result_columns));
        //                 }
        //                 joined_blocks
        //             })
        //             .flatten()
        //     })
        //     .flatten()
        //     .collect();

        let mut joined_blocks: Vec<DataBlock> = vec![];
        for outer_block in &right_buffer {
            for inner_block in &left_buffer {
                for i in 0..inner_block.num_rows() {
                    let mut result_columns = vec![];
                    for column in inner_block.columns() {
                        let inner_value = DataValue::try_from_array(column, i)?;
                        result_columns
                            .push(inner_value.to_array_with_size(outer_block.num_rows())?);
                    }
                    for column in outer_block.columns() {
                        result_columns.push(column.clone());
                    }
                    joined_blocks.push(DataBlock::create(self.schema.clone(), result_columns));
                }
            }
        }

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            joined_blocks
        )))
    }
}
