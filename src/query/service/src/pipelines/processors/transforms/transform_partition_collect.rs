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
    const NAME: &'static str = "Hilbert";
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
        data_block.pop_columns(1);
        let scatter_blocks = DataBlock::scatter(&data_block, &indices, self.num_partitions)?;

        // Partition the data blocks to different processors.
        let mut output_data_blocks = vec![vec![]; n];
        for (partition_id, data_block) in scatter_blocks.into_iter().enumerate() {
            output_data_blocks[partition_id % n].push((partition_id, data_block));
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
    Spill,
    Restore,
    Concat,
}

pub struct TransformHilbertPartitionCollect {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    restored_data_blocks: Vec<DataBlock>,
    output_data_blocks: Vec<DataBlock>,

    max_block_size: usize,
    // The partition id is used to map the partition id to the new partition id.
    partition_id: Vec<usize>,
    // The buffer is used to control the memory usage of the window operator.
    buffer: WindowPartitionBuffer,
    state: State,
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
            output_data_blocks: vec![],
            restored_data_blocks: Vec::new(),
            max_block_size,
            state: State::Consume,
        })
    }

    fn collect_data_block(&mut self) -> Result<()> {
        let data_block = self.input.pull_data().unwrap()?;
        if let Some(meta) = data_block
            .get_owned_meta()
            .and_then(WindowPartitionMeta::downcast_from)
        {
            for (partition_id, data_block) in meta.partitioned_data.into_iter() {
                let partition_id = self.partition_id[partition_id];
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
        if matches!(self.state, State::Concat) {
            return Ok(Event::Sync);
        }

        if matches!(self.state, State::Restore | State::Spill) {
            return Ok(Event::Async);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data_blocks.pop() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
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

            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.input.has_data() {
            self.collect_data_block()?;

            if self.need_spill() {
                self.state = State::Spill;
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
                self.output_data_blocks.push(block);
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::Spill => self.buffer.spill().await?,
            State::Restore => {
                let blocks = self.buffer.restore().await?;
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
