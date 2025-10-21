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

use databend_common_base::runtime::GlobalIORuntime;
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
use either::Either;

use super::window_partition_buffer_v2::WindowPartitionBufferV2;
use super::WindowPartitionBuffer;
use super::WindowPartitionMeta;
use crate::pipelines::processors::transforms::DataProcessorStrategy;
use crate::sessions::QueryContext;
use crate::spillers::BackpressureSpiller;
use crate::spillers::Spiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerDiskConfig;
use crate::spillers::SpillerType;
use crate::spillers::SpillsBufferPool;

enum WindowBuffer {
    V1(WindowPartitionBuffer),
    V2(WindowPartitionBufferV2),
}

impl WindowBuffer {
    fn new(
        spiller: Either<Spiller, BackpressureSpiller>,
        num_partitions: usize,
        sort_block_size: usize,
        memory_settings: MemorySettings,
    ) -> Result<Self> {
        match spiller {
            Either::Left(spiller) => {
                let inner = WindowPartitionBuffer::new(
                    spiller,
                    num_partitions,
                    sort_block_size,
                    memory_settings,
                )?;
                Ok(Self::V1(inner))
            }
            Either::Right(spiller) => {
                let inner = WindowPartitionBufferV2::new(
                    move |schema| spiller.new_writer_creator(Arc::new(schema)).unwrap(),
                    num_partitions,
                    sort_block_size,
                    memory_settings,
                )?;
                Ok(Self::V2(inner))
            }
        }
    }

    fn need_spill(&mut self) -> bool {
        match self {
            WindowBuffer::V1(inner) => inner.need_spill(),
            WindowBuffer::V2(inner) => inner.need_spill(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            WindowBuffer::V1(inner) => inner.is_empty(),
            WindowBuffer::V2(inner) => inner.is_empty(),
        }
    }

    fn add_data_block(&mut self, index: usize, data_block: DataBlock) {
        match self {
            WindowBuffer::V1(inner) => inner.add_data_block(index, data_block),
            WindowBuffer::V2(inner) => inner.add_data_block(index, data_block),
        }
    }

    async fn spill(&mut self) -> Result<()> {
        match self {
            WindowBuffer::V1(inner) => inner.spill().await,
            WindowBuffer::V2(inner) => inner.spill().await,
        }
    }

    async fn restore(&mut self) -> Result<Vec<DataBlock>> {
        match self {
            WindowBuffer::V1(inner) => inner.restore().await,
            WindowBuffer::V2(inner) => inner.restore().await,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Step {
    Sync(SyncStep),
    Async(AsyncStep),
    Finish,
}

#[derive(Debug, Clone, Copy)]
enum SyncStep {
    Collect,
    Process,
}

#[derive(Debug, Clone, Copy)]
enum AsyncStep {
    Spill,
    Restore,
}

pub struct TransformWindowPartitionCollect<S: DataProcessorStrategy> {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    restored_data_blocks: Vec<DataBlock>,
    output_data_blocks: VecDeque<DataBlock>,

    // The partition id is used to map the partition id to the new partition id.
    index_map: Vec<usize>,
    // The buffer is used to control the memory usage of the window operator.
    buffer: WindowBuffer,

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

        // Map each partition id to new partition index.
        let mut index_map = vec![0; num_partitions];
        for (index, partition) in partitions.iter().enumerate() {
            index_map[*partition] = index;
        }

        let location_prefix = ctx.query_id_spill_prefix();
        let spill_config = SpillerConfig {
            spiller_type: SpillerType::Window,
            location_prefix,
            disk_spill,
            use_parquet: settings.get_spilling_file_format()?.is_parquet(),
        };

        // Create spillers for window operator.
        let operator = DataOperator::instance().spill_operator();
        let spiller = if !settings.get_enable_backpressure_spiller()? {
            Either::Left(Spiller::create(ctx, operator, spill_config)?)
        } else {
            let runtime = GlobalIORuntime::instance();
            let buffer_pool = SpillsBufferPool::create(runtime, 128 * 1024 * 1024, 3);
            Either::Right(BackpressureSpiller::create(
                ctx,
                operator,
                spill_config,
                buffer_pool,
                8 * 1024 * 1024,
            )?)
        };

        // Create the window partition buffer.
        let sort_block_size = settings.get_window_partition_sort_block_size()? as usize;
        let buffer =
            WindowBuffer::new(spiller, partitions.len(), sort_block_size, memory_settings)?;

        Ok(Self {
            input,
            output,
            index_map,
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
                &self.index_map,
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
            Step::Sync(SyncStep::Collect) => self.collect(),
            Step::Async(AsyncStep::Spill) => {
                if self.is_collect_finished {
                    self.step = Step::Sync(SyncStep::Process);
                    self.output()
                } else {
                    // collect data again.
                    self.step = Step::Sync(SyncStep::Collect);
                    self.collect()
                }
            }
            Step::Sync(SyncStep::Process) => self.output(),
            Step::Async(AsyncStep::Restore) => {
                if self.restored_data_blocks.is_empty() {
                    self.next_step(Step::Finish)
                } else {
                    self.next_step(Step::Sync(SyncStep::Process))
                }
            }
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
    fn collect_data_block(data_block: DataBlock, index_map: &[usize], buffer: &mut WindowBuffer) {
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
