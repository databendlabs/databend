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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_pipeline_transforms::MemorySettings;

use super::concat_data_blocks;
use crate::spillers::AnyFileWriter;
use crate::spillers::RowGroupEncoder;
use crate::spillers::SpillReader;
use crate::spillers::SpillWriter;
use crate::spillers::WriterCreator;

#[async_trait::async_trait]
pub trait Reader: Send {
    async fn restore(&mut self, row_groups: Vec<usize>) -> Result<Vec<DataBlock>>;
}

#[async_trait::async_trait]
pub trait Writer: Send {
    type R: Reader;

    fn need_new_file(&mut self, incoming_size: usize) -> Result<bool>;

    fn add_row_group_encoded(&mut self, row_group: RowGroupEncoder) -> Result<usize>;

    fn close(self) -> Result<Self::R>;
}

#[async_trait::async_trait]
pub trait WriterFactory: Send {
    type W: Writer;

    async fn open(&mut self, local_file_size: Option<usize>) -> Result<Self::W>;

    fn create_encoder(&self) -> RowGroupEncoder;
}

#[async_trait::async_trait]
impl Reader for SpillReader {
    async fn restore(&mut self, row_groups: Vec<usize>) -> Result<Vec<DataBlock>> {
        self.restore(row_groups).await
    }
}

#[async_trait::async_trait]
impl Writer for SpillWriter {
    type R = SpillReader;

    fn need_new_file(&mut self, incoming_size: usize) -> Result<bool> {
        Ok(match self.file_writer() {
            AnyFileWriter::Local(file_writer) => !file_writer.check_grow(incoming_size, true)?,
            _ => false,
        })
    }

    fn add_row_group_encoded(&mut self, row_group: RowGroupEncoder) -> Result<usize> {
        let meta = SpillWriter::add_encoded_row_group(self, row_group)?;
        Ok(meta.ordinal().unwrap() as usize)
    }

    fn close(self) -> Result<SpillReader> {
        SpillWriter::close(self)
    }
}

#[async_trait::async_trait]
impl WriterFactory for WriterCreator {
    type W = SpillWriter;

    async fn open(&mut self, local_file_size: Option<usize>) -> Result<SpillWriter> {
        WriterCreator::open(self, local_file_size).await
    }

    fn create_encoder(&self) -> RowGroupEncoder {
        WriterCreator::new_encoder(self)
    }
}

#[derive(Default)]
enum PartitionSpillState<W>
where W: Writer
{
    #[default]
    Empty,
    Writing {
        writer: W,
        row_groups: Vec<usize>,
    },
    Reading,
}

type FactoryReader<F> = <<F as WriterFactory>::W as Writer>::R;

struct PartitionSlot<F>
where F: WriterFactory
{
    state: PartitionSpillState<F::W>,
    readers: Vec<(FactoryReader<F>, Vec<usize>)>,
    buffered_blocks: Vec<DataBlock>,
    buffered_size: usize,
}

impl<F> Default for PartitionSlot<F>
where F: WriterFactory
{
    fn default() -> Self {
        Self {
            state: Default::default(),
            readers: Default::default(),
            buffered_blocks: Default::default(),
            buffered_size: Default::default(),
        }
    }
}

impl<F> PartitionSlot<F>
where F: WriterFactory
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
        if self.buffered_size >= threshold.unwrap_or_default() {
            self.buffered_size = 0;
            Some(std::mem::take(&mut self.buffered_blocks))
        } else {
            None
        }
    }

    async fn spill_blocks(&mut self, factory: &mut F, blocks: Vec<DataBlock>) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        let mut encoder = factory.create_encoder();
        for block in blocks {
            encoder.add(block)?;
        }

        match &mut self.state {
            PartitionSpillState::Empty => {
                const FILE_SIZE: usize = 10 * 1024 * 1024;
                let mut writer = factory.open(Some(FILE_SIZE)).await?;
                let ordinal = writer.add_row_group_encoded(encoder)?;
                self.state = PartitionSpillState::Writing {
                    writer,
                    row_groups: vec![ordinal],
                };
                Ok(())
            }
            PartitionSpillState::Writing { writer, row_groups } => {
                if !writer.need_new_file(encoder.memory_size())? {
                    let ordinal = writer.add_row_group_encoded(encoder)?;
                    row_groups.push(ordinal);

                    if ordinal >= SpillWriter::MAX_ORDINAL {
                        let PartitionSpillState::Writing { writer, row_groups } =
                            std::mem::replace(&mut self.state, PartitionSpillState::Empty)
                        else {
                            unreachable!()
                        };
                        let reader = writer.close()?;
                        self.readers.push((reader, row_groups));
                    }

                    return Ok(());
                }

                let PartitionSpillState::Writing { writer, row_groups } =
                    std::mem::replace(&mut self.state, PartitionSpillState::Empty)
                else {
                    unreachable!()
                };
                let reader = writer.close()?;
                self.readers.push((reader, row_groups));

                let mut writer = factory.open(None).await?;
                let ordinal = writer.add_row_group_encoded(encoder)?;
                self.state = PartitionSpillState::Writing {
                    writer,
                    row_groups: vec![ordinal],
                };
                Ok(())
            }
            PartitionSpillState::Reading => unreachable!("partition already closed"),
        }
    }

    fn take_readers(&mut self) -> Result<Vec<(FactoryReader<F>, Vec<usize>)>> {
        if let PartitionSpillState::Writing { writer, row_groups } =
            std::mem::replace(&mut self.state, PartitionSpillState::Reading)
        {
            self.readers.push((writer.close()?, row_groups));
        }
        Ok(std::mem::take(&mut self.readers))
    }
}

pub(super) type WindowPartitionBufferV2 = PartitionBuffer<WriterCreator>;

pub(super) struct PartitionBuffer<F>
where F: WriterFactory
{
    factory: Option<F>,
    factory_builder: Arc<dyn Fn(DataSchema) -> F + Send + Sync + 'static>,
    partitions: Vec<PartitionSlot<F>>,
    memory_settings: MemorySettings,
    min_row_group_size: usize,
    num_partitions: usize,
    sort_block_size: usize,
    can_spill: bool,
    next_to_restore_partition_id: isize,
}

impl<F> PartitionBuffer<F>
where F: WriterFactory
{
    pub fn new(
        factory_builder: impl Fn(DataSchema) -> F + Send + Sync + 'static,
        num_partitions: usize,
        sort_block_size: usize,
        memory_settings: MemorySettings,
    ) -> Result<Self> {
        let partitions = (0..num_partitions)
            .map(|_| PartitionSlot::default())
            .collect();
        Ok(Self {
            factory: None,
            factory_builder: Arc::new(factory_builder),
            partitions,
            memory_settings,
            min_row_group_size: 10 * 1024 * 1024,
            num_partitions,
            sort_block_size,
            can_spill: false,
            next_to_restore_partition_id: -1,
        })
    }

    pub fn need_spill(&mut self) -> bool {
        self.can_spill && self.memory_settings.check_spill()
    }

    pub fn is_empty(&self) -> bool {
        self.next_to_restore_partition_id + 1 >= self.num_partitions as isize
    }

    pub fn add_data_block(&mut self, partition_id: usize, data_block: DataBlock) {
        if data_block.is_empty() {
            return;
        }

        if self.factory.is_none() {
            let facroty = (self.factory_builder)(data_block.infer_schema());
            self.factory = Some(facroty)
        }

        let partition = &mut self.partitions[partition_id];
        partition.add_block(data_block);
        if !self.can_spill && partition.memory_size() >= self.min_row_group_size {
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
                partition
                    .spill_blocks(self.factory.as_mut().unwrap(), blocks)
                    .await?;
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
            && size >= self.min_row_group_size
        {
            let partition = &mut self.partitions[partition_id];
            let blocks = partition.fetch_blocks(None).unwrap();
            partition
                .spill_blocks(self.factory.as_mut().unwrap(), blocks)
                .await?;
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

            let mut result = Vec::new();
            for (mut reader, row_groups) in partition.take_readers()? {
                debug_assert!(!row_groups.is_empty());
                let mut blocks = reader.restore(row_groups).await?;
                result.append(&mut blocks);
            }

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
