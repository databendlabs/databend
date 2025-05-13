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

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_settings::Settings;
use databend_common_storage::DataOperator;

use crate::pipelines::processors::transforms::CompactStrategy;
use crate::pipelines::processors::transforms::DataProcessorStrategy;
use crate::pipelines::processors::transforms::WindowPartitionBuffer;
use crate::pipelines::processors::transforms::WindowPartitionMeta;
use crate::sessions::QueryContext;
use crate::spillers::Spiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerDiskConfig;
use crate::spillers::SpillerType;

enum State {
    Collect,
    Flush,
    Spill,
    Restore,
    Compact(Vec<DataBlock>),
}

pub struct TransformHilbertCollect {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    immediate_output_blocks: Vec<(usize, DataBlock)>,
    output_data_blocks: VecDeque<DataBlock>,

    // The partition id is used to map the partition id to the new partition id.
    partition_id: Vec<usize>,
    partition_sizes: Vec<usize>,
    // The buffer is used to control the memory usage of the window operator.
    buffer: WindowPartitionBuffer,

    compact_strategy: CompactStrategy,
    max_block_size: usize,
    // Event variables.
    state: State,
}

impl TransformHilbertCollect {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: Arc<QueryContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        settings: &Settings,
        processor_id: usize,
        num_processors: usize,
        num_partitions: usize,
        memory_settings: MemorySettings,
        disk_spill: Option<SpillerDiskConfig>,
        max_block_rows: usize,
        max_block_size: usize,
    ) -> Result<Self> {
        // Calculate the partition ids collected by the processor.
        let partitions: Vec<usize> = (0..num_partitions)
            .filter(|&partition| partition % num_processors == processor_id)
            .collect();

        // Map each partition id to new partition id.
        let mut partition_id = vec![0; num_partitions];
        for (new_partition_id, partition) in partitions.iter().enumerate() {
            partition_id[*partition] = new_partition_id;
        }

        let location_prefix = ctx.query_id_spill_prefix();
        let spill_config = SpillerConfig {
            spiller_type: SpillerType::Window,
            location_prefix,
            disk_spill,
            use_parquet: settings.get_spilling_file_format()?.is_parquet(),
        };

        // Create an inner `Spiller` to spill data.
        let operator = DataOperator::instance().spill_operator();
        let spiller = Spiller::create(ctx, operator, spill_config)?;

        // Create the window partition buffer.
        let buffer =
            WindowPartitionBuffer::new(spiller, partitions.len(), max_block_rows, memory_settings)?;

        Ok(Self {
            input,
            output,
            partition_id,
            buffer,
            immediate_output_blocks: vec![],
            partition_sizes: vec![0; num_partitions],
            max_block_size,
            compact_strategy: CompactStrategy::new(max_block_rows, max_block_size),
            output_data_blocks: VecDeque::new(),
            state: State::Collect,
        })
    }
}

#[async_trait::async_trait]
impl Processor for TransformHilbertCollect {
    fn name(&self) -> String {
        "TransformHilbertCollect".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::Compact(_)) {
            return Ok(Event::Sync);
        }

        if matches!(self.state, State::Flush | State::Spill | State::Restore) {
            return Ok(Event::Async);
        }

        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data_blocks.pop_front() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.need_spill() {
            self.state = State::Spill;
            return Ok(Event::Async);
        }

        if !self.immediate_output_blocks.is_empty() {
            self.state = State::Flush;
            return Ok(Event::Async);
        }

        if self.input.is_finished() {
            if !self.buffer.is_empty() {
                self.state = State::Restore;
                return Ok(Event::Async);
            }

            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.input.has_data() {
            self.collect_data_block()?;

            if self.need_spill() {
                self.state = State::Spill;
                return Ok(Event::Async);
            }

            if !self.immediate_output_blocks.is_empty() {
                self.state = State::Flush;
                return Ok(Event::Async);
            }
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Collect) {
            State::Compact(blocks) => {
                let output = self.compact_strategy.process_data_blocks(blocks)?;
                self.output_data_blocks.extend(output);
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Collect) {
            State::Spill => {
                self.buffer.spill().await?;
            }
            State::Flush => {
                if let Some((partition_id, data_block)) = self.immediate_output_blocks.pop() {
                    let mut restored_data_blocks =
                        self.buffer.restore_by_id(partition_id, true).await?;
                    restored_data_blocks.push(data_block);
                    self.state = State::Compact(restored_data_blocks);
                }
            }
            State::Restore => {
                let restored_data_blocks = self.buffer.restore().await?;
                self.state = State::Compact(restored_data_blocks);
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}

impl TransformHilbertCollect {
    fn collect_data_block(&mut self) -> Result<()> {
        let data_block = self.input.pull_data().unwrap()?;
        if let Some(meta) = data_block
            .get_owned_meta()
            .and_then(WindowPartitionMeta::downcast_from)
        {
            for (partition_id, data_block) in meta.partitioned_data.into_iter() {
                let new_id = self.partition_id[partition_id];
                self.partition_sizes[new_id] += data_block.estimate_block_size();
                if self.partition_sizes[new_id] >= self.max_block_size {
                    self.immediate_output_blocks.push((new_id, data_block));
                    self.partition_sizes[new_id] = 0;
                    continue;
                }
                self.buffer.add_data_block(new_id, data_block);
            }
        }
        Ok(())
    }

    fn need_spill(&mut self) -> bool {
        self.buffer.need_spill()
    }
}
