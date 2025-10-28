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

pub trait Reader: Send {
    fn restore(&mut self, row_groups: Vec<usize>, batch_size: usize) -> Result<Vec<DataBlock>>;
}

pub trait Writer: Send + 'static {
    type R: Reader;

    fn need_new_file(&mut self, incoming_size: usize) -> Result<bool>;

    fn add_row_group_encoded(&mut self, row_group: RowGroupEncoder) -> Result<usize>;

    fn close(self) -> Result<Self::R>;
}

pub trait WriterFactory: Send {
    type W: Writer;

    fn open(&mut self, local_file_size: Option<usize>) -> Result<Self::W>;

    fn create_encoder(&self) -> RowGroupEncoder;
}

impl Reader for SpillReader {
    fn restore(&mut self, row_groups: Vec<usize>, batch_size: usize) -> Result<Vec<DataBlock>> {
        self.restore2(row_groups, batch_size)
    }
}

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

impl WriterFactory for WriterCreator {
    type W = SpillWriter;

    fn open(&mut self, local_file_size: Option<usize>) -> Result<SpillWriter> {
        WriterCreator::open(self, local_file_size)
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
    id: usize,
    state: PartitionSpillState<F::W>,
    readers: Vec<(FactoryReader<F>, Vec<usize>)>,
    buffered_blocks: Vec<DataBlock>,
    buffered_size: usize,
}

impl<F> PartitionSlot<F>
where F: WriterFactory
{
    fn new(id: usize) -> Self {
        Self {
            id,
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

    fn take_blocks(&mut self, threshold: Option<usize>) -> Option<Vec<DataBlock>> {
        if self.buffered_size >= threshold.unwrap_or_default() {
            self.buffered_size = 0;
            Some(std::mem::take(&mut self.buffered_blocks))
        } else {
            None
        }
    }

    #[fastrace::trace(name = "PartitionSlot::spill_blocks")]
    fn spill_blocks(
        &mut self,
        factory: &mut F,
        blocks: Vec<DataBlock>,
        spill_unit_size: usize,
    ) -> Result<()> {
        let mut encoder = factory.create_encoder();
        for block in blocks {
            encoder.add(block)?;
        }
        let row_group_size = encoder.memory_size();
        log::debug!(id = self.id, row_group_size ; "spill new row_group");

        match &mut self.state {
            PartitionSpillState::Empty => {
                let local_file_size = self
                    .readers
                    .is_empty()
                    .then(|| spill_unit_size.max(row_group_size));
                let mut writer = factory.open(local_file_size)?;
                let ordinal = writer.add_row_group_encoded(encoder)?;
                self.state = PartitionSpillState::Writing {
                    writer,
                    row_groups: vec![ordinal],
                };
                Ok(())
            }
            PartitionSpillState::Writing { writer, .. } => {
                if !writer.need_new_file(encoder.memory_size())? {
                    let PartitionSpillState::Writing {
                        mut writer,
                        mut row_groups,
                    } = std::mem::replace(&mut self.state, PartitionSpillState::Empty)
                    else {
                        unreachable!()
                    };

                    let ordinal = writer.add_row_group_encoded(encoder)?;
                    row_groups.push(ordinal);

                    if ordinal >= SpillWriter::MAX_ORDINAL {
                        let reader = writer.close()?;
                        self.readers.push((reader, row_groups));
                    } else {
                        self.state = PartitionSpillState::Writing { writer, row_groups };
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

                let mut writer = factory.open(None)?;
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
            let reader = writer.close()?;
            self.readers.push((reader, row_groups));
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
    sort_block_size: usize,
    can_spill: bool,
    next_to_restore: usize,
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
            .map(|id| PartitionSlot::new(id))
            .collect();
        Ok(Self {
            factory: None,
            factory_builder: Arc::new(factory_builder),
            partitions,
            memory_settings,
            min_row_group_size: 10 * 1024 * 1024,
            sort_block_size,
            can_spill: false,
            next_to_restore: 0,
        })
    }

    pub fn need_spill(&mut self) -> bool {
        self.can_spill && self.memory_settings.check_spill()
    }

    pub fn is_empty(&self) -> bool {
        self.next_to_restore >= self.partitions.len()
    }

    pub fn add_data_block(&mut self, index: usize, data_block: DataBlock) {
        if data_block.is_empty() {
            return;
        }

        if self.factory.is_none() {
            let facroty = (self.factory_builder)(data_block.infer_schema());
            self.factory = Some(facroty)
        }

        let partition = &mut self.partitions[index];
        partition.add_block(data_block);
        if !self.can_spill && partition.buffered_size >= self.min_row_group_size {
            self.can_spill = true;
        }
    }

    #[fastrace::trace(name = "PartitionBuffer::spill")]
    pub fn spill(&mut self) -> Result<()> {
        let spill_unit_size = self.memory_settings.spill_unit_size;

        let mut preferred_partition = None;
        for partition in self.partitions[self.next_to_restore..].iter_mut().rev() {
            if partition.buffered_blocks.is_empty() {
                continue;
            }
            if let Some(blocks) = partition.take_blocks(Some(spill_unit_size)) {
                partition.spill_blocks(self.factory.as_mut().unwrap(), blocks, spill_unit_size)?;
                return Ok(());
            }

            let partition_size = partition.buffered_size;
            if preferred_partition
                .as_ref()
                .map(|(_, size)| partition_size > *size)
                .unwrap_or(true)
            {
                preferred_partition = Some((partition, partition_size));
            }
        }

        if let Some((partition, size)) = preferred_partition
            && size >= self.min_row_group_size
        {
            let blocks = partition.take_blocks(None).unwrap();
            partition.spill_blocks(self.factory.as_mut().unwrap(), blocks, spill_unit_size)?;
        } else {
            self.can_spill = false;
        }
        Ok(())
    }

    #[fastrace::trace(name = "PartitionBuffer::restore")]
    pub fn restore(&mut self) -> Result<Vec<DataBlock>> {
        for partition in &mut self.partitions[self.next_to_restore..] {
            self.next_to_restore += 1;

            let mut result = Vec::new();
            for (mut reader, row_groups) in partition.take_readers()? {
                debug_assert!(!row_groups.is_empty());
                let blocks = reader.restore(row_groups, self.sort_block_size)?;
                result.extend(blocks);
            }

            if let Some(blocks) = partition.take_blocks(None) {
                result.extend(concat_data_blocks(blocks, self.sort_block_size)?);
            }

            if !result.is_empty() {
                return Ok(result);
            }
        }
        Ok(vec![])
    }
}
