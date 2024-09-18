// Copyright 2021 Datafuse Labs
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

// Some variables and functions are named and designed with reference to ClickHouse.
// - https://github.com/ClickHouse/ClickHouse/blob/master/src/Processors/Transforms/WindowTransform.h
// - https://github.com/ClickHouse/ClickHouse/blob/master/src/Processors/Transforms/WindowTransform.cpp

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_transforms::processors::sort_merge;

use super::WindowPartitionBuffer;
use super::WindowPartitionMeta;
use super::WindowSpillSettings;
use crate::sessions::QueryContext;

#[derive(Debug, Clone, Copy)]
pub enum Step {
    Sync(SyncStep),
    Async(AsyncStep),
    Finish,
}

#[derive(Debug, Clone, Copy)]
pub enum SyncStep {
    Collect,
    Sort,
}

#[derive(Debug, Clone, Copy)]
pub enum AsyncStep {
    Spill,
    Restore,
}

pub struct TransformWindowPartitionCollect {
    inputs: Vec<Arc<InputPort>>,
    output: Arc<OutputPort>,

    restored_data_blocks: Vec<DataBlock>,
    output_data_blocks: VecDeque<DataBlock>,

    // The partition id is used to map the partition id to the new partition id.
    partition_id: Vec<usize>,
    // The buffer is used to control the memory usage of the window operator.
    buffer: WindowPartitionBuffer,

    // Sort variables.
    sort_desc: Vec<SortColumnDescription>,
    schema: DataSchemaRef,
    max_block_size: usize,
    sort_spilling_batch_bytes: usize,
    enable_loser_tree: bool,
    have_order_col: bool,

    // Event variables.
    step: Step,
    is_collect_finished: bool,
}

impl TransformWindowPartitionCollect {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: Arc<QueryContext>,
        processor_id: usize,
        num_processors: usize,
        num_partitions: usize,
        spill_settings: WindowSpillSettings,
        sort_desc: Vec<SortColumnDescription>,
        schema: DataSchemaRef,
        max_block_size: usize,
        sort_spilling_batch_bytes: usize,
        enable_loser_tree: bool,
        have_order_col: bool,
    ) -> Result<Self> {
        let inputs = (0..num_processors).map(|_| InputPort::create()).collect();
        let output = OutputPort::create();

        // Calculate the partition ids collected by the processor.
        let partitions: Vec<usize> = (0..num_partitions)
            .filter(|&partition| partition % num_processors == processor_id)
            .collect();

        // Map each partition id to new partition id.
        let mut partition_id = vec![0; num_partitions];
        for (new_partition_id, partition) in partitions.iter().enumerate() {
            partition_id[*partition] = new_partition_id;
        }

        // Create the window partition buffer.
        let buffer =
            WindowPartitionBuffer::new(ctx, partitions.len(), max_block_size, spill_settings)?;

        Ok(Self {
            inputs,
            output,
            output_data_blocks: VecDeque::new(),
            restored_data_blocks: Vec::new(),
            partition_id,
            buffer,
            sort_desc,
            schema,
            max_block_size,
            sort_spilling_batch_bytes,
            enable_loser_tree,
            have_order_col,
            step: Step::Sync(SyncStep::Collect),
            is_collect_finished: false,
        })
    }

    pub fn into_pipe_item(self) -> PipeItem {
        let inputs = self.inputs.clone();
        let outputs = vec![self.output.clone()];
        let processor_ptr = ProcessorPtr::create(Box::new(self));
        PipeItem::create(processor_ptr, inputs, outputs)
    }

    fn next_step(&mut self, step: Step) -> Result<Event> {
        let event = match step {
            Step::Sync(_) => Event::Sync,
            Step::Async(_) => Event::Async,
            Step::Finish => {
                for input in self.inputs.iter() {
                    input.finish();
                }
                self.output.finish();
                Event::Finished
            }
        };
        self.step = step;
        Ok(event)
    }

    fn collect(&mut self) -> Result<Event> {
        let mut finished_input = 0;
        for input in self.inputs.iter() {
            if input.is_finished() {
                finished_input += 1;
                continue;
            }

            if input.has_data() {
                Self::collect_data_block(
                    input.pull_data().unwrap()?,
                    &self.partition_id,
                    &mut self.buffer,
                );
            }

            if input.is_finished() {
                finished_input += 1;
            } else {
                input.set_need_data();
            }
        }

        if finished_input == self.inputs.len() {
            self.is_collect_finished = true;
        }

        if self.need_spill() {
            return self.next_step(Step::Async(AsyncStep::Spill));
        }

        if self.is_collect_finished {
            self.next_step(Step::Async(AsyncStep::Restore))
        } else {
            Ok(Event::NeedData)
        }
    }

    fn output(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            return self.next_step(Step::Finish);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if self.need_spill() {
            return self.next_step(Step::Async(AsyncStep::Spill));
        }

        if let Some(data_block) = self.output_data_blocks.pop_front() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if !self.buffer.is_empty() {
            self.next_step(Step::Async(AsyncStep::Restore))
        } else {
            self.next_step(Step::Finish)
        }
    }
}

#[async_trait::async_trait]
impl Processor for TransformWindowPartitionCollect {
    fn name(&self) -> String {
        "TransformWindowPartitionCollect".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        match self.step {
            Step::Sync(sync_step) => match sync_step {
                SyncStep::Collect => self.collect(),
                SyncStep::Sort => self.output(),
            },
            Step::Async(async_step) => match async_step {
                AsyncStep::Spill => {
                    if self.need_spill() {
                        self.next_step(Step::Async(AsyncStep::Spill))
                    } else if !self.is_collect_finished {
                        self.collect()
                    } else {
                        self.output()
                    }
                }
                AsyncStep::Restore => {
                    if !self.restored_data_blocks.is_empty() {
                        self.next_step(Step::Sync(SyncStep::Sort))
                    } else {
                        self.next_step(Step::Finish)
                    }
                }
            },
            Step::Finish => Ok(Event::Finished),
        }
    }

    fn process(&mut self) -> Result<()> {
        match self.step {
            Step::Sync(SyncStep::Sort) => {
                let restored_data_blocks = std::mem::take(&mut self.restored_data_blocks);

                let data_blocks = restored_data_blocks
                    .into_iter()
                    .map(|data_block| DataBlock::sort(&data_block, &self.sort_desc, None))
                    .collect::<Result<Vec<_>>>()?;

                let sorted_data_blocks = sort_merge(
                    self.schema.clone(),
                    self.max_block_size,
                    self.sort_desc.clone(),
                    data_blocks,
                    self.sort_spilling_batch_bytes,
                    self.enable_loser_tree,
                    self.have_order_col,
                )?;

                self.output_data_blocks.extend(sorted_data_blocks);
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match &self.step {
            Step::Async(AsyncStep::Spill) => self.buffer.spill().await?,
            Step::Async(AsyncStep::Restore) => {
                self.restored_data_blocks = self.buffer.restore().await?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}

impl TransformWindowPartitionCollect {
    fn collect_data_block(
        data_block: DataBlock,
        partition_ids: &[usize],
        buffer: &mut WindowPartitionBuffer,
    ) {
        if let Some(meta) = data_block
            .get_owned_meta()
            .and_then(WindowPartitionMeta::downcast_from)
        {
            for (partition_id, data_block) in meta.partitioned_data.into_iter() {
                let partition_id = partition_ids[partition_id];
                buffer.add_data_block(partition_id, data_block);
            }
        }
    }

    fn need_spill(&mut self) -> bool {
        self.buffer.need_spill()
    }
}
