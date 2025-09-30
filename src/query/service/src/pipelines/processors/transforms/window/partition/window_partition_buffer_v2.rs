use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::MemorySettings;

use super::concat_data_blocks;
use crate::spillers::PartitionBuffer;
use crate::spillers::PartitionBufferFetchOption;

#[async_trait::async_trait]
pub trait Spill: Send + Sync {
    async fn spill(&mut self, blocks: Vec<DataBlock>) -> Result<i16>;
    async fn restore(&mut self, ordinals: Vec<i16>) -> Result<Vec<DataBlock>>;
}

pub struct WindowPartitionBufferV2<S: Spill> {
    spill: S,
    memory_settings: MemorySettings,
    min_spill_size: usize,
    partition_buffer: PartitionBuffer,
    num_partitions: usize,
    sort_block_size: usize,
    can_spill: bool,
    next_to_restore_partition_id: isize,
    spilled_partition_ordinals: Vec<Vec<i16>>,
}

impl<S: Spill> WindowPartitionBufferV2<S> {
    pub fn new(
        spill: S,
        num_partitions: usize,
        sort_block_size: usize,
        memory_settings: MemorySettings,
    ) -> Result<Self> {
        let partition_buffer = PartitionBuffer::create(num_partitions);
        Ok(Self {
            spill,
            memory_settings,
            min_spill_size: 1024 * 1024,
            partition_buffer,
            num_partitions,
            sort_block_size,
            can_spill: false,
            next_to_restore_partition_id: -1,
            spilled_partition_ordinals: vec![Vec::new(); num_partitions],
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
        self.partition_buffer
            .add_data_block(partition_id, data_block);
        if !self.can_spill
            && self.partition_buffer.partition_memory_size(partition_id) >= self.min_spill_size
        {
            self.can_spill = true;
        }
    }

    pub async fn spill(&mut self) -> Result<()> {
        let spill_unit_size = self.memory_settings.spill_unit_size;
        let next_to_restore_partition_id = (self.next_to_restore_partition_id + 1) as usize;

        let mut preferred_partition: Option<(usize, usize)> = None;
        for partition_id in (next_to_restore_partition_id..self.num_partitions).rev() {
            if self.partition_buffer.is_partition_empty(partition_id) {
                continue;
            }
            if let Some(blocks) = self.partition_buffer.fetch_data_blocks(
                partition_id,
                &PartitionBufferFetchOption::PickPartitionWithThreshold(spill_unit_size),
            ) {
                let ordinal = self.spill.spill(blocks).await?;
                self.spilled_partition_ordinals[partition_id].push(ordinal);
                return Ok(());
            }

            let partition_size = self.partition_buffer.partition_memory_size(partition_id);
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
            let blocks = self
                .partition_buffer
                .fetch_data_blocks(partition_id, &PartitionBufferFetchOption::ReadPartition)
                .unwrap();
            let ordinal = self.spill.spill(blocks).await?;
            self.spilled_partition_ordinals[partition_id].push(ordinal);
        } else {
            self.can_spill = false;
        }
        Ok(())
    }

    pub async fn restore(&mut self) -> Result<Vec<DataBlock>> {
        while self.next_to_restore_partition_id + 1 < self.num_partitions as isize {
            self.next_to_restore_partition_id += 1;
            let partition_id = self.next_to_restore_partition_id as usize;

            let ordinals = std::mem::take(&mut self.spilled_partition_ordinals[partition_id]);
            let mut result = if ordinals.is_empty() {
                Vec::new()
            } else {
                self.spill.restore(ordinals).await?
            };

            if let Some(blocks) = self
                .partition_buffer
                .fetch_data_blocks(partition_id, &PartitionBufferFetchOption::ReadPartition)
            {
                result.extend(concat_data_blocks(blocks, self.sort_block_size)?);
            }

            if !result.is_empty() {
                return Ok(result);
            }
        }

        Ok(vec![])
    }
}
