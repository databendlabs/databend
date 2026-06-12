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
use super::window_partition_buffer::WindowPartitionBuffer;
use crate::pipelines::processors::transforms::DataProcessorStrategy;
use crate::sessions::QueryContext;

/// Two-phase state machine:
/// - Collect: pull input data into buffer, spill when memory pressure is high.
/// - Output: restore partitions one by one, process (sort), and push downstream.
#[derive(Debug, Clone, Copy)]
enum Step {
    Collect,
    Output,
}

pub struct TransformWindowPartitionCollect<S: DataProcessorStrategy> {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data_blocks: VecDeque<DataBlock>,

    index_map: Vec<usize>,
    buffer: WindowPartitionBuffer,
    strategy: S,
    step: Step,
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
        let partitions: Vec<usize> = (0..num_partitions)
            .filter(|&partition| partition % num_processors == processor_id)
            .collect();

        let mut index_map = vec![0; num_partitions];
        for (index, partition) in partitions.iter().enumerate() {
            index_map[*partition] = index;
        }

        let prefix = ctx.query_id_spill_prefix();
        let writer_pool_bytes = settings
            .get_spill_writer_memory_pool_size_mb()?
            .saturating_mul(1024 * 1024);

        let sort_block_size = settings.get_window_partition_sort_block_size()? as usize;
        let buffer = WindowPartitionBuffer::new(
            ctx.clone(),
            prefix,
            writer_pool_bytes,
            partitions.len(),
            sort_block_size,
            memory_settings,
        )?;

        Ok(Self {
            input,
            output,
            output_data_blocks: VecDeque::new(),
            index_map,
            buffer,
            strategy,
            step: Step::Collect,
        })
    }

    fn collect_data_block(&mut self, data_block: DataBlock) {
        if let Some(meta) = data_block
            .get_owned_meta()
            .and_then(WindowPartitionMeta::downcast_from)
        {
            for (id, block) in meta.partitioned_data {
                self.buffer.add_data_block(self.index_map[id], block);
            }
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
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        match self.step {
            Step::Collect => {
                if self.input.has_data() {
                    return Ok(Event::Sync);
                }

                if self.input.is_finished() {
                    self.step = Step::Output;
                    return Ok(Event::Sync);
                }

                self.input.set_need_data();
                Ok(Event::NeedData)
            }
            Step::Output => {
                if !self.output.can_push() {
                    return Ok(Event::NeedConsume);
                }

                if let Some(block) = self.output_data_blocks.pop_front() {
                    self.output.push_data(Ok(block));
                    return Ok(Event::NeedConsume);
                }

                if self.buffer.is_empty() {
                    self.output.finish();
                    return Ok(Event::Finished);
                }

                Ok(Event::Sync)
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        match self.step {
            Step::Collect => {
                let data_block = self.input.pull_data().unwrap()?;
                self.collect_data_block(data_block);

                // Spill if memory pressure is high.
                while self.buffer.need_spill() {
                    self.buffer.spill()?;
                }
                Ok(())
            }
            Step::Output => {
                // Restore next non-empty partition and process it.
                let restored = self.buffer.restore()?;
                if !restored.is_empty() {
                    let processed = self.strategy.process_data_blocks(restored)?;
                    self.output_data_blocks.extend(processed);
                }
                Ok(())
            }
        }
    }
}
