use std::future::Future;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::MemorySettings;

use super::concat_data_blocks;
use crate::spillers::PartitionBuffer;
use crate::spillers::PartitionBufferFetchOption;

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
pub trait SpillBuilder<W: SpillWriter>: Send + Sync {
    async fn create(&self, partition_id: usize) -> Result<W>;
}

#[async_trait::async_trait]
impl<W, F, Fut> SpillBuilder<W> for F
where
    W: SpillWriter,
    F: Fn(usize) -> Fut + Send + Sync,
    Fut: Future<Output = Result<W>> + Send,
{
    async fn create(&self, partition_id: usize) -> Result<W> {
        (self)(partition_id).await
    }
}

#[derive(Default)]
enum PartitionSpillState<W, R> {
    #[default]
    Empty,
    Writing(W),
    Reading(R),
}

pub struct WindowPartitionBufferV2<W, B>
where
    W: SpillWriter,
    W::Reader: SpillReader,
    B: SpillBuilder<W>,
{
    spill_builder: B,
    partition_spills: Vec<PartitionSpillState<W, W::Reader>>,
    memory_settings: MemorySettings,
    min_spill_size: usize,
    partition_buffer: PartitionBuffer,
    num_partitions: usize,
    sort_block_size: usize,
    can_spill: bool,
    next_to_restore_partition_id: isize,
    spilled_partition_ordinals: Vec<Vec<i16>>,
}

impl<W, B> WindowPartitionBufferV2<W, B>
where
    W: SpillWriter,
    W::Reader: SpillReader,
    B: SpillBuilder<W>,
{
    pub fn new(
        spill_builder: B,
        num_partitions: usize,
        sort_block_size: usize,
        memory_settings: MemorySettings,
    ) -> Result<Self> {
        let partition_buffer = PartitionBuffer::create(num_partitions);
        let partition_spills = (0..num_partitions)
            .map(|_| PartitionSpillState::default)
            .collect();
        Ok(Self {
            spill_builder,
            partition_spills,
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
                self.ensure_partition_writer(partition_id).await?;
                let writer = self
                    .partition_writer_mut(partition_id)
                    .expect("partition writer must exist");
                let ordinal = writer.spill(blocks).await?;
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
            self.ensure_partition_writer(partition_id).await?;
            let writer = self
                .partition_writer_mut(partition_id)
                .expect("partition writer must exist");
            let ordinal = writer.spill(blocks).await?;
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
                self.close_partition_writer(partition_id).await?;
                let reader = self
                    .partition_reader_mut(partition_id)
                    .expect("partition reader must exist after closing writer");
                reader.restore(ordinals).await?
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

    async fn ensure_partition_writer(&mut self, partition_id: usize) -> Result<()> {
        if matches!(
            self.partition_spills.get(partition_id),
            Some(PartitionSpillState::Empty)
        ) {
            let writer = self.spill_builder.create(partition_id).await?;
            self.partition_spills[partition_id] = PartitionSpillState::Writing(writer);
            return Ok(());
        }

        if matches!(
            self.partition_spills.get(partition_id),
            Some(PartitionSpillState::Reading(_))
        ) {
            debug_assert!(
                false,
                "partition {} spill already closed before new writes",
                partition_id
            );
        }
        Ok(())
    }

    async fn close_partition_writer(&mut self, partition_id: usize) -> Result<()> {
        let state = std::mem::replace(
            &mut self.partition_spills[partition_id],
            PartitionSpillState::Empty,
        );
        match state {
            PartitionSpillState::Empty => {
                debug_assert!(
                    false,
                    "closing partition {} without spill writer",
                    partition_id
                );
                self.partition_spills[partition_id] = PartitionSpillState::Empty;
            }
            PartitionSpillState::Writing(writer) => {
                let reader = writer.close().await?;
                self.partition_spills[partition_id] = PartitionSpillState::Reading(reader);
            }
            PartitionSpillState::Reading(reader) => {
                self.partition_spills[partition_id] = PartitionSpillState::Reading(reader);
            }
        }
        Ok(())
    }

    fn partition_writer_mut(&mut self, partition_id: usize) -> Option<&mut W> {
        match self.partition_spills.get_mut(partition_id) {
            Some(PartitionSpillState::Writing(writer)) => Some(writer),
            _ => None,
        }
    }

    fn partition_reader_mut(&mut self, partition_id: usize) -> Option<&mut W::Reader> {
        match self.partition_spills.get_mut(partition_id) {
            Some(PartitionSpillState::Reading(reader)) => Some(reader),
            _ => None,
        }
    }
}
