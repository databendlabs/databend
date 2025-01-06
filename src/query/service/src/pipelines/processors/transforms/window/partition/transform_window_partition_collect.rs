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
use databend_common_pipeline_transforms::processors::sort_merge;
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
    input: Arc<InputPort>,
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
    #[expect(clippy::too_many_arguments)]
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
        sort_desc: Vec<SortColumnDescription>,
        schema: DataSchemaRef,
        have_order_col: bool,
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
        let buffer =
            WindowPartitionBuffer::new(spiller, partitions.len(), sort_block_size, spill_settings)?;

        let max_block_size = settings.get_max_block_size()? as usize;
        let enable_loser_tree = settings.get_enable_loser_tree_merge_sort()?;
        let sort_spilling_batch_bytes = settings.get_sort_spilling_batch_bytes()?;

        Ok(Self {
            input,
            output,
            partition_id,
            buffer,
            sort_desc,
            schema,
            max_block_size,
            have_order_col,
            enable_loser_tree,
            sort_spilling_batch_bytes,
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
impl Processor for TransformWindowPartitionCollect {
    fn name(&self) -> String {
        "TransformWindowPartitionCollect".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        // (collect <--> spill) -> (sort <--> restore) -> finish
        match self.step {
            Step::Sync(sync_step) => match sync_step {
                SyncStep::Collect => self.collect(),
                SyncStep::Sort => self.output(),
            },
            Step::Async(async_step) => match async_step {
                AsyncStep::Spill => match self.is_collect_finished {
                    true => {
                        self.step = Step::Sync(SyncStep::Sort);
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
                    false => self.next_step(Step::Sync(SyncStep::Sort)),
                },
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
