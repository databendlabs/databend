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
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_settings::Settings;
use databend_common_storage::DataOperator;

use super::WindowPartitionBuffer;
use super::WindowPartitionMeta;
use crate::pipelines::processors::transforms::DataProcessorStrategy;
use crate::sessions::QueryContext;
use crate::spillers::Spiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerDiskConfig;
use crate::spillers::SpillerType;

#[derive(Debug, Clone, Copy)]
pub enum Step {
    Sync(SyncStep),
    Async(AsyncStep),
    Finish,
}

#[derive(Debug, Clone, Copy)]
pub enum SyncStep {
    Collect,
    Process,
}

#[derive(Debug, Clone, Copy)]
pub enum AsyncStep {
    Spill,
    Restore,
}

pub struct TransformWindowPartitionCollect<S: DataProcessorStrategy> {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    restored_data_blocks: Vec<DataBlock>,
    output_data_blocks: VecDeque<DataBlock>,

    // The partition id is used to map the partition id to the new partition id.
    partition_id: Vec<usize>,
    // The buffer is used to control the memory usage of the window operator.
    buffer: WindowPartitionBuffer,

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
        disk_spill: Option<SpillerDiskConfig>,
        strategy: S,
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
        let sort_block_size = settings.get_window_partition_sort_block_size()? as usize;
        let buffer = WindowPartitionBuffer::new(
            spiller,
            partitions.len(),
            sort_block_size,
            memory_settings,
        )?;

        Ok(Self {
            input,
            output,
            partition_id,
            buffer,
            strategy,
            is_collect_finished: false,
            output_data_blocks: VecDeque::new(),
            restored_data_blocks: Vec::new(),
            step: Step::Sync(SyncStep::Collect),
        })
    }

    fn next_step(&mut self, step: Step) -> Result<Event> {
        let event = match step {
            Step::Sync(_) => Event::Sync,
            Step::Async(_) => Event::Async,
            Step::Finish => {
                self.input.finish();
                self.output.finish();
                Event::Finished
            }
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
            return self.next_step(Step::Async(AsyncStep::Spill));
        }

        if self.input.has_data() {
            Self::collect_data_block(
                self.input.pull_data().unwrap()?,
                &self.partition_id,
                &mut self.buffer,
            );
        }

        // Check again. flush memory data to external storage if need
        if self.need_spill() {
            return self.next_step(Step::Async(AsyncStep::Spill));
        }

        if self.input.is_finished() {
            self.is_collect_finished = true;
            return self.next_step(Step::Async(AsyncStep::Restore));
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
            return self.next_step(Step::Async(AsyncStep::Spill));
        }

        if let Some(data_block) = self.output_data_blocks.pop_front() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        match self.buffer.is_empty() {
            true => self.next_step(Step::Finish),
            false => self.next_step(Step::Async(AsyncStep::Restore)),
        }
    }
}

#[async_trait::async_trait]
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
            Step::Sync(sync_step) => match sync_step {
                SyncStep::Collect => self.collect(),
                SyncStep::Process => self.output(),
            },
            Step::Async(async_step) => match async_step {
                AsyncStep::Spill => match self.is_collect_finished {
                    true => {
                        self.step = Step::Sync(SyncStep::Process);
                        self.output()
                    }
                    false => {
                        // collect data again.
                        self.step = Step::Sync(SyncStep::Collect);
                        self.collect()
                    }
                },
                AsyncStep::Restore => match self.restored_data_blocks.is_empty() {
                    true => self.next_step(Step::Finish),
                    false => self.next_step(Step::Sync(SyncStep::Process)),
                },
            },
            Step::Finish => Ok(Event::Finished),
        }
    }

    fn process(&mut self) -> Result<()> {
        match self.step {
            Step::Sync(SyncStep::Process) => {
                let restored_data_blocks = std::mem::take(&mut self.restored_data_blocks);
                let processed_blocks = self.strategy.process_data_blocks(restored_data_blocks)?;
                self.output_data_blocks.extend(processed_blocks);
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

impl<S: DataProcessorStrategy> TransformWindowPartitionCollect<S> {
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
