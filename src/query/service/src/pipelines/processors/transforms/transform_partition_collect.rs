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
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::Exchange;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_settings::Settings;
use databend_common_storage::DataOperator;

use super::WindowPartitionBuffer;
use super::WindowPartitionMeta;
use super::WindowSpillSettings;
use crate::sessions::QueryContext;
use crate::spillers::Spiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerDiskConfig;
use crate::spillers::SpillerType;

pub struct HilbertPartitionExchange {
    num_partitions: usize,
}

impl HilbertPartitionExchange {
    pub fn create(num_partitions: usize) -> Arc<HilbertPartitionExchange> {
        Arc::new(HilbertPartitionExchange { num_partitions })
    }
}

impl Exchange for HilbertPartitionExchange {
    const NAME: &'static str = "Window";
    fn partition(&self, data_block: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        // Extract the columns used for hash computation.
        let mut data_block = data_block.consume_convert_to_full();
        let last = data_block.get_last_column().as_nullable().unwrap();
        let range_ids = last.column.as_number().unwrap().as_u_int64().unwrap();

        // Scatter the data block to different partitions.
        let indices = range_ids
            .iter()
            .map(|&id| (id % self.num_partitions as u64) as u16)
            .collect::<Vec<_>>();
        let scatter_indices =
            DataBlock::divide_indices_by_scatter_size(&indices, self.num_partitions);

        let mut scatter_blocks = Vec::with_capacity(self.num_partitions);
        data_block.pop_columns(1);
        for (partition_id, indices) in scatter_indices.iter().take(self.num_partitions).enumerate()
        {
            if indices.is_empty() {
                continue;
            }
            let block = data_block.take_with_optimize_size(indices)?;
            scatter_blocks.push((partition_id, block));
        }

        // Partition the data blocks to different processors.
        let mut output_data_blocks = vec![vec![]; n];
        for (idx, data_block) in scatter_blocks.into_iter().enumerate() {
            output_data_blocks[idx % n].push(data_block);
        }

        // Union data blocks for each processor.
        Ok(output_data_blocks
            .into_iter()
            .map(WindowPartitionMeta::create)
            .map(DataBlock::empty_with_meta)
            .collect())
    }
}

enum State {
    Consume,
    Flush,
    Spill,
    Restore,
    Concat,
    Coalesce,
}

pub struct TransformHilbertPartitionCollect {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    immediate_output_blocks: Vec<(usize, DataBlock)>,
    restored_data_blocks: Vec<DataBlock>,
    pending_small_blocks: Vec<DataBlock>,
    output_data_blocks: Vec<DataBlock>,

    max_block_size: usize,
    // The partition id is used to map the partition id to the new partition id.
    partition_id: Vec<usize>,
    partition_sizes: Vec<usize>,
    // The buffer is used to control the memory usage of the window operator.
    buffer: WindowPartitionBuffer,
    state: State,
    process_size: usize,
    processor_id: usize,
}

impl TransformHilbertPartitionCollect {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: Arc<QueryContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        settings: &Settings,
        processor_id: usize,
        num_processors: usize,
        num_partitions: usize,
        spill_settings: WindowSpillSettings,
        disk_spill: Option<SpillerDiskConfig>,
        max_block_size: usize,
    ) -> Result<Self> {
        // Calculate the partition ids collected by the processor.
        let partitions: Vec<usize> = (0..num_partitions)
            .filter(|&partition| partition % num_processors == processor_id)
            .collect();
        log::warn!(
            "collect processor id: {}, partitions: {:?}",
            processor_id,
            partitions
        );

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
            WindowPartitionBuffer::new(spiller, partitions.len(), max_block_size, spill_settings)?;

        Ok(Self {
            input,
            output,
            partition_id,
            buffer,
            immediate_output_blocks: vec![],
            pending_small_blocks: vec![],
            output_data_blocks: vec![],
            restored_data_blocks: Vec::new(),
            max_block_size,
            partition_sizes: vec![0; num_partitions],
            state: State::Consume,
            process_size: 0,
            processor_id,
        })
    }

    fn collect_data_block(&mut self) -> Result<()> {
        let data_block = self.input.pull_data().unwrap()?;
        if let Some(meta) = data_block
            .get_owned_meta()
            .and_then(WindowPartitionMeta::downcast_from)
        {
            for (partition_id, data_block) in meta.partitioned_data.into_iter() {
                log::warn!(
                    "collect processor id: {}, partition_id: {}",
                    self.processor_id,
                    partition_id
                );
                self.process_size += data_block.num_rows();
                let partition_id = self.partition_id[partition_id];
                self.partition_sizes[partition_id] += data_block.num_rows();
                if self.partition_sizes[partition_id] >= self.max_block_size {
                    self.immediate_output_blocks
                        .push((partition_id, data_block));
                    self.partition_sizes[partition_id] = 0;
                    continue;
                }
                self.buffer.add_data_block(partition_id, data_block);
            }
        }
        Ok(())
    }

    fn need_spill(&mut self) -> bool {
        self.buffer.need_spill()
    }
}

#[async_trait::async_trait]
impl Processor for TransformHilbertPartitionCollect {
    fn name(&self) -> String {
        "TransformHilbertPartitionCollect".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::Concat | State::Coalesce) {
            return Ok(Event::Sync);
        }

        if matches!(self.state, State::Flush | State::Restore | State::Spill) {
            return Ok(Event::Async);
        }

        if self.output.is_finished() {
            log::warn!(
                "process id: {}, process rows: {}",
                self.processor_id,
                self.process_size
            );
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data_blocks.pop() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if !self.immediate_output_blocks.is_empty() {
            self.state = State::Flush;
            return Ok(Event::Async);
        }

        if self.need_spill() {
            self.state = State::Spill;
            return Ok(Event::Async);
        }

        if self.input.is_finished() {
            if !self.buffer.is_empty() {
                self.state = State::Restore;
                return Ok(Event::Async);
            }

            if !self.pending_small_blocks.is_empty() {
                self.state = State::Coalesce;
                return Ok(Event::Sync);
            }
            self.output.finish();
            log::warn!(
                "process id: {}, process rows: {}",
                self.processor_id,
                self.process_size
            );
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
        match std::mem::replace(&mut self.state, State::Consume) {
            State::Concat => {
                let restored_data_blocks = std::mem::take(&mut self.restored_data_blocks);
                let block = DataBlock::concat(&restored_data_blocks)?;
                if block.num_rows() < self.max_block_size / 10 {
                    self.pending_small_blocks.push(block);
                } else {
                    self.output_data_blocks.push(block);
                }
            }
            State::Coalesce => {
                let pending_small_blocks = std::mem::take(&mut self.pending_small_blocks);
                let block = DataBlock::concat(&pending_small_blocks)?;
                self.output_data_blocks.push(block);
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::Flush => {
                if let Some((partition_id, data_block)) = self.immediate_output_blocks.pop() {
                    self.restored_data_blocks = self.buffer.restore_by_id(partition_id).await?;
                    self.restored_data_blocks.push(data_block);
                    log::warn!(
                        "flush processor id: {}, partition id: {}",
                        self.processor_id,
                        partition_id
                    );
                    self.state = State::Concat;
                }
            }
            State::Spill => self.buffer.spill().await?,
            State::Restore => {
                let (id, blocks) = self.buffer.restore_with_id().await?;
                log::warn!(
                    "restore processor id: {}, partition id: {}",
                    self.processor_id,
                    id
                );
                if !blocks.is_empty() {
                    self.restored_data_blocks = blocks;
                    self.state = State::Concat;
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}
