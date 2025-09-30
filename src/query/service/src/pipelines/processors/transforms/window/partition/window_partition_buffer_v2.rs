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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::MemorySettings;

use super::concat_data_blocks;

#[async_trait::async_trait]
pub trait SpillReader: Send {
    async fn restore(&mut self, ordinals: Vec<i16>) -> Result<Vec<DataBlock>>;
}

#[async_trait::async_trait]
pub trait SpillWriter: Send {
    type Reader: SpillReader;

    async fn spill(&mut self, blocks: Vec<DataBlock>) -> Result<i16>;

    async fn close(self) -> Result<Self::Reader>;
}

#[async_trait::async_trait]
pub trait SpillBuilder: Send + Sync {
    type Writer: SpillWriter;

    async fn create(&self) -> Result<Self::Writer>;
}

#[derive(Default)]
enum PartitionSpillState<W>
where
    W: SpillWriter,
    W::Reader: SpillReader,
{
    #[default]
    Empty,
    Writing(W),
    Reading(W::Reader),
}

struct PartitionSlot<W>
where
    W: SpillWriter,
    W::Reader: SpillReader,
{
    state: PartitionSpillState<W>,
    spilled_ordinals: Vec<i16>,
    buffered_blocks: Vec<DataBlock>,
    buffered_size: usize,
}

impl<W> Default for PartitionSlot<W>
where
    W: SpillWriter,
    W::Reader: SpillReader,
{
    fn default() -> Self {
        Self {
            state: PartitionSpillState::Empty,
            spilled_ordinals: Vec::new(),
            buffered_blocks: Vec::new(),
            buffered_size: 0,
        }
    }
}

impl<W> PartitionSlot<W>
where
    W: SpillWriter,
    W::Reader: SpillReader,
{
    fn add_block(&mut self, block: DataBlock) {
        self.buffered_size += block.memory_size();
        self.buffered_blocks.push(block);
    }

    fn memory_size(&self) -> usize {
        self.buffered_size
    }

    fn is_empty(&self) -> bool {
        self.buffered_blocks.is_empty()
    }

    fn fetch_blocks(&mut self, threshold: Option<usize>) -> Option<Vec<DataBlock>> {
        match threshold {
            None => {
                if self.buffered_blocks.is_empty() {
                    None
                } else {
                    Some(self.buffered_blocks.clone())
                }
            }
            Some(threshold) => {
                if self.buffered_size >= threshold {
                    self.buffered_size = 0;
                    Some(std::mem::take(&mut self.buffered_blocks))
                } else {
                    None
                }
            }
        }
    }

    async fn writer_mut<'a, B>(&'a mut self, builder: &B) -> Result<&'a mut W>
    where B: SpillBuilder<Writer = W> {
        match &mut self.state {
            state @ PartitionSpillState::Empty => {
                let writer = builder.create().await?;
                let _ = std::mem::replace(state, PartitionSpillState::Writing(writer));
                let PartitionSpillState::Writing(writer) = state else {
                    unreachable!()
                };
                Ok(writer)
            }
            PartitionSpillState::Writing(writer) => Ok(writer),
            PartitionSpillState::Reading(_) => unreachable!("partition already closed"),
        }
    }

    async fn close_writer(&mut self) -> Result<&mut W::Reader> {
        let PartitionSpillState::Writing(writer) = std::mem::take(&mut self.state) else {
            unreachable!()
        };
        self.state = PartitionSpillState::Reading(writer.close().await?);
        let PartitionSpillState::Reading(reader) = &mut self.state else {
            unreachable!()
        };
        Ok(reader)
    }
}

pub struct WindowPartitionBufferV2<B>
where
    B: SpillBuilder,
    <B::Writer as SpillWriter>::Reader: SpillReader,
{
    spill_builder: B,
    partitions: Vec<PartitionSlot<B::Writer>>,
    memory_settings: MemorySettings,
    min_spill_size: usize,
    num_partitions: usize,
    sort_block_size: usize,
    can_spill: bool,
    next_to_restore_partition_id: isize,
}

impl<B> WindowPartitionBufferV2<B>
where
    B: SpillBuilder,
    <B::Writer as SpillWriter>::Reader: SpillReader,
{
    pub fn new(
        spill_builder: B,
        num_partitions: usize,
        sort_block_size: usize,
        memory_settings: MemorySettings,
    ) -> Result<Self> {
        let mut partitions = Vec::with_capacity(num_partitions);
        for _ in 0..num_partitions {
            partitions.push(PartitionSlot::<B::Writer>::default());
        }
        Ok(Self {
            spill_builder,
            partitions,
            memory_settings,
            min_spill_size: 1024 * 1024,
            num_partitions,
            sort_block_size,
            can_spill: false,
            next_to_restore_partition_id: -1,
        })
    }

    pub fn need_spill(&mut self) -> bool {
        self.can_spill && self.memory_settings.check_spill()
    }

    pub fn out_of_memory_limit(&mut self) -> bool {
        self.memory_settings.check_spill()
    }

    pub fn is_empty(&self) -> bool {
        self.next_to_restore_partition_id + 1 >= self.num_partitions as isize
    }

    pub fn add_data_block(&mut self, partition_id: usize, data_block: DataBlock) {
        if data_block.is_empty() {
            return;
        }
        let partition = &mut self.partitions[partition_id];
        partition.add_block(data_block);
        if !self.can_spill && partition.memory_size() >= self.min_spill_size {
            self.can_spill = true;
        }
    }

    pub async fn spill(&mut self) -> Result<()> {
        let spill_unit_size = self.memory_settings.spill_unit_size;
        let next_to_restore_partition_id = (self.next_to_restore_partition_id + 1) as usize;

        let mut preferred_partition: Option<(usize, usize)> = None;
        for partition_id in (next_to_restore_partition_id..self.num_partitions).rev() {
            let partition = &mut self.partitions[partition_id];
            if partition.is_empty() {
                continue;
            }
            if let Some(blocks) = partition.fetch_blocks(Some(spill_unit_size)) {
                let ordinal = {
                    let writer = partition.writer_mut(&self.spill_builder).await?;
                    writer.spill(blocks).await?
                };
                partition.spilled_ordinals.push(ordinal);
                return Ok(());
            }

            let partition_size = partition.memory_size();
            if preferred_partition
                .as_ref()
                .map(|(_, size)| partition_size > *size)
                .unwrap_or(true)
            {
                preferred_partition = Some((partition_id, partition_size));
            }
        }

        if let Some((partition_id, size)) = preferred_partition
            && size >= self.min_spill_size
        {
            let partition = &mut self.partitions[partition_id];
            let blocks = partition.fetch_blocks(None).unwrap();
            let ordinal = {
                let writer = partition.writer_mut(&self.spill_builder).await?;
                writer.spill(blocks).await?
            };
            partition.spilled_ordinals.push(ordinal);
        } else {
            self.can_spill = false;
        }
        Ok(())
    }

    pub async fn restore(&mut self) -> Result<Vec<DataBlock>> {
        while self.next_to_restore_partition_id + 1 < self.num_partitions as isize {
            self.next_to_restore_partition_id += 1;
            let partition_id = self.next_to_restore_partition_id as usize;
            let partition = &mut self.partitions[partition_id];

            let ordinals = std::mem::take(&mut partition.spilled_ordinals);
            let mut result = if ordinals.is_empty() {
                Vec::new()
            } else {
                let reader = partition.close_writer().await?;
                reader.restore(ordinals).await?
            };

            if let Some(blocks) = partition.fetch_blocks(None) {
                result.extend(concat_data_blocks(blocks, self.sort_block_size)?);
            }

            if !result.is_empty() {
                return Ok(result);
            }
        }

        Ok(vec![])
    }
}
