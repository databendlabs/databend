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
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_settings::Settings;

use super::WindowPartitionMeta;
use super::window_partition_buffer_v2::WindowPartitionBufferV2;
use crate::pipelines::processors::transforms::DataProcessorStrategy;
use crate::sessions::QueryContext;

#[derive(Debug, Clone, Copy)]
enum Step {
    Collect,
    Process,
    Spill,
    Restore,
    Finish,
}

pub struct TransformWindowPartitionCollect<S: DataProcessorStrategy> {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    restored_data_blocks: Vec<DataBlock>,
    output_data_blocks: VecDeque<DataBlock>,

    // The partition id is used to map the partition id to the new partition id.
    index_map: Vec<usize>,
    // The buffer is used to control the memory usage of the window operator.
    buffer: WindowPartitionBufferV2,

    strategy: S,

    // Event variables.
    step: Step,
    is_collect_finished: bool,
}

impl<S: DataProcessorStrategy> TransformWindowPartitionCollect<S> {
    pub fn new(
        ctx: Arc<QueryContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        settings: &Settings,
        processor_id: usize,
        num_processors: usize,
        num_partitions: usize,
        memory_settings: MemorySettings,
        strategy: S,
    ) -> Result<Self> {
        // Calculate the partition ids collected by the processor.
        let partitions: Vec<usize> = (0..num_partitions)
            .filter(|&partition| partition % num_processors == processor_id)
            .collect();

        // Map each partition id to new partition index.
        let mut index_map = vec![0; num_partitions];
        for (index, partition) in partitions.iter().enumerate() {
            index_map[*partition] = index;
        }

        let prefix = ctx.query_id_spill_prefix();
        let writer_pool_bytes = settings
            .get_spill_writer_memory_pool_size_mb()?
            .saturating_mul(1024 * 1024);

        // Create the window partition buffer.
        let sort_block_size = settings.get_window_partition_sort_block_size()? as usize;
        let buffer = WindowPartitionBufferV2::new(
            prefix,
            writer_pool_bytes,
            partitions.len(),
            sort_block_size,
            memory_settings,
        )?;

        Ok(Self {
            input,
            output,
            index_map,
            buffer,
            strategy,
            is_collect_finished: false,
            output_data_blocks: VecDeque::new(),
            restored_data_blocks: Vec::new(),
            step: Step::Collect,
        })
    }

    fn next_step(&mut self, step: Step) -> Result<Event> {
        let event = match step {
            Step::Finish => {
                self.input.finish();
                self.output.finish();
                Event::Finished
            }
            _ => Event::Sync,
        };
        self.step = step;
        Ok(event)
    }

    fn collect(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return self.next_step(Step::Finish);
        }

        // First check. flush memory data to external storage if need
        if self.need_spill() {
            return self.next_step(Step::Spill);
        }

        if self.input.has_data() {
            Self::collect_data_block(
                self.input.pull_data().unwrap()?,
                &self.index_map,
                &mut self.buffer,
            );
        }

        // Check again. flush memory data to external storage if need
        if self.need_spill() {
            return self.next_step(Step::Spill);
        }

        if self.input.is_finished() {
            self.is_collect_finished = true;
            return self.next_step(Step::Restore);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn output(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            return self.next_step(Step::Finish);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if self.need_spill() {
            return self.next_step(Step::Spill);
        }

        if let Some(data_block) = self.output_data_blocks.pop_front() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        match self.buffer.is_empty() {
            true => self.next_step(Step::Finish),
            false => self.next_step(Step::Restore),
        }
    }
}

impl<S: DataProcessorStrategy> Processor for TransformWindowPartitionCollect<S> {
    fn name(&self) -> String {
        format!("TransformWindowPartitionCollect({})", S::NAME)
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        // (collect <--> spill) -> (process <--> restore) -> finish
        match self.step {
            Step::Collect => self.collect(),
            Step::Spill => {
                if self.is_collect_finished {
                    self.step = Step::Process;
                    self.output()
                } else {
                    // collect data again.
                    self.step = Step::Collect;
                    self.collect()
                }
            }
            Step::Process => self.output(),
            Step::Restore => {
                if self.restored_data_blocks.is_empty() {
                    self.next_step(Step::Finish)
                } else {
                    self.next_step(Step::Process)
                }
            }
            Step::Finish => Ok(Event::Finished),
        }
    }

    fn process(&mut self) -> Result<()> {
        match self.step {
            Step::Process => {
                let restored_data_blocks = std::mem::take(&mut self.restored_data_blocks);
                let processed_blocks = self.strategy.process_data_blocks(restored_data_blocks)?;
                self.output_data_blocks.extend(processed_blocks);
                Ok(())
            }
            Step::Spill => self.buffer.spill(),
            Step::Restore => {
                self.restored_data_blocks = self.buffer.restore()?;
                Ok(())
            }
            _ => unreachable!(),
        }
    }
}

impl<S: DataProcessorStrategy> TransformWindowPartitionCollect<S> {
    fn collect_data_block(
        data_block: DataBlock,
        index_map: &[usize],
        buffer: &mut WindowPartitionBufferV2,
    ) {
        if let Some(meta) = data_block
            .get_owned_meta()
            .and_then(WindowPartitionMeta::downcast_from)
        {
            for (id, data_block) in meta.partitioned_data {
                buffer.add_data_block(index_map[id], data_block);
            }
        }
    }

    fn need_spill(&mut self) -> bool {
        self.buffer.need_spill()
    }
}
