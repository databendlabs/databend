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

use databend_base::uniq_id::GlobalUniq;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_pipeline_transforms::traits::Location;
use databend_common_storage::DataOperator;
use databend_common_storages_parquet::ReadSettings;
use parquet::file::metadata::RowGroupMetaData;

use crate::sessions::QueryContext;
use crate::spillers::Layout;
use crate::spillers::SpillAdapter;
use crate::spillers::SpillTarget;
use crate::spillers::SpillsBufferPool;
use crate::spillers::SpillsDataWriter;

/// Maximum number of row groups per file before rotating to a new file.
const MAX_ROW_GROUPS_PER_FILE: usize = 2 << 15;

fn concat_data_blocks(data_blocks: Vec<DataBlock>, target_size: usize) -> Result<Vec<DataBlock>> {
    let mut num_rows = 0;
    let mut result = Vec::new();
    let mut current_blocks = Vec::new();

    for data_block in data_blocks.into_iter() {
        num_rows += data_block.num_rows();
        current_blocks.push(data_block);
        if num_rows >= target_size {
            result.push(DataBlock::concat(&current_blocks)?);
            num_rows = 0;
            current_blocks.clear();
        }
    }

    if !current_blocks.is_empty() {
        result.push(DataBlock::concat(&current_blocks)?);
    }

    Ok(result)
}

struct PartitionFileWriter {
    path: String,
    writer: SpillsDataWriter,
    schema: Option<DataSchemaRef>,
}

impl PartitionFileWriter {
    fn create(prefix: &str, writer_pool_bytes: usize) -> Result<Self> {
        let data_operator = DataOperator::instance();
        let target = SpillTarget::from_storage_params(data_operator.spill_params());
        let operator = data_operator.spill_operator();
        let buffer_pool = SpillsBufferPool::instance();
        let path = format!("{}/{}", prefix, GlobalUniq::unique());
        let writer = buffer_pool.writer(operator, path.clone(), writer_pool_bytes, target)?;
        Ok(Self {
            path,
            writer,
            schema: None,
        })
    }

    fn write_blocks(&mut self, blocks: Vec<DataBlock>) -> Result<usize> {
        for block in blocks {
            if self.schema.is_none() {
                self.schema = Some(Arc::new(block.infer_schema()));
            }
            self.writer.write(block)?;
        }
        self.writer.flush_row_groups()
    }

    fn close(self, ctx: &Arc<QueryContext>) -> Result<PartitionFileReader> {
        let Self {
            path,
            writer,
            schema,
        } = self;
        let (bytes_written, row_groups) = writer.close()?;
        log::debug!(
            path = path,
            bytes_written;
            "partition file closed"
        );

        if bytes_written > 0 {
            SpillAdapter::add_spill_file(
                ctx,
                Location::Remote(path.clone()),
                Layout::Parquet,
                bytes_written,
            );
        }

        Ok(PartitionFileReader {
            path,
            row_groups,
            schema: schema.unwrap_or_else(|| Arc::new(Default::default())),
        })
    }
}

struct PartitionFileReader {
    path: String,
    row_groups: Vec<RowGroupMetaData>,
    schema: DataSchemaRef,
}

impl PartitionFileReader {
    fn restore(&self) -> Result<Vec<DataBlock>> {
        if self.row_groups.is_empty() {
            return Ok(Vec::new());
        }
        let data_operator = DataOperator::instance();
        let target = SpillTarget::from_storage_params(data_operator.spill_params());
        let operator = data_operator.spill_operator();
        let buffer_pool = SpillsBufferPool::instance();
        let settings = ReadSettings::default();
        let mut reader = buffer_pool.reader(
            operator,
            self.path.clone(),
            self.schema.clone(),
            self.row_groups.clone(),
            target,
            settings,
        )?;
        let mut blocks = Vec::new();
        while let Some(block) = reader.read()? {
            blocks.push(block);
        }
        Ok(blocks)
    }
}

// --- Partition slot state machine ---

#[derive(Default)]
enum PartitionSpillState {
    #[default]
    Empty,
    Writing {
        writer: PartitionFileWriter,
    },
    Reading,
}

struct PartitionSlot {
    id: usize,
    state: PartitionSpillState,
    readers: Vec<PartitionFileReader>,
    buffered_blocks: Vec<DataBlock>,
    buffered_size: usize,
}

impl PartitionSlot {
    fn new(id: usize) -> Self {
        Self {
            id,
            state: Default::default(),
            readers: Default::default(),
            buffered_blocks: Default::default(),
            buffered_size: Default::default(),
        }
    }

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
        ctx: &Arc<QueryContext>,
        prefix: &str,
        writer_pool_bytes: usize,
        blocks: Vec<DataBlock>,
    ) -> Result<()> {
        let row_group_size: usize = blocks.iter().map(|b| b.memory_size()).sum();
        log::debug!(id = self.id, row_group_size; "spill new row_group");

        match &mut self.state {
            PartitionSpillState::Empty => {
                let mut writer = PartitionFileWriter::create(prefix, writer_pool_bytes)?;
                let row_group_count = writer.write_blocks(blocks)?;
                if row_group_count >= MAX_ROW_GROUPS_PER_FILE {
                    let reader = writer.close(ctx)?;
                    self.readers.push(reader);
                } else {
                    self.state = PartitionSpillState::Writing { writer };
                }
                Ok(())
            }
            PartitionSpillState::Writing { .. } => {
                let PartitionSpillState::Writing { mut writer } =
                    std::mem::replace(&mut self.state, PartitionSpillState::Empty)
                else {
                    unreachable!()
                };

                let row_group_count = writer.write_blocks(blocks)?;

                if row_group_count >= MAX_ROW_GROUPS_PER_FILE {
                    let reader = writer.close(ctx)?;
                    self.readers.push(reader);
                } else {
                    self.state = PartitionSpillState::Writing { writer };
                }
                Ok(())
            }
            PartitionSpillState::Reading => unreachable!("partition already closed"),
        }
    }

    fn take_readers(&mut self, ctx: &Arc<QueryContext>) -> Result<Vec<PartitionFileReader>> {
        if let PartitionSpillState::Writing { writer } =
            std::mem::replace(&mut self.state, PartitionSpillState::Reading)
        {
            let reader = writer.close(ctx)?;
            self.readers.push(reader);
        }
        Ok(std::mem::take(&mut self.readers))
    }
}

pub(super) struct WindowPartitionBuffer {
    ctx: Arc<QueryContext>,
    prefix: String,
    writer_pool_bytes: usize,
    partitions: Vec<PartitionSlot>,
    memory_settings: MemorySettings,
    min_row_group_size: usize,
    sort_block_size: usize,
    can_spill: bool,
    next_to_restore: usize,
}

impl WindowPartitionBuffer {
    pub fn new(
        ctx: Arc<QueryContext>,
        prefix: String,
        writer_pool_bytes: usize,
        num_partitions: usize,
        sort_block_size: usize,
        memory_settings: MemorySettings,
    ) -> Result<Self> {
        let partitions = (0..num_partitions).map(PartitionSlot::new).collect();
        Ok(Self {
            ctx,
            prefix,
            writer_pool_bytes,
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
        let partition = &mut self.partitions[index];
        partition.add_block(data_block);
        if !self.can_spill && partition.buffered_size >= self.min_row_group_size {
            self.can_spill = true;
        }
    }

    #[fastrace::trace(name = "WindowPartitionBuffer::spill")]
    pub fn spill(&mut self) -> Result<()> {
        let spill_unit_size = self.memory_settings.spill_unit_size;

        let mut preferred_partition = None;
        for partition in self.partitions[self.next_to_restore..].iter_mut().rev() {
            if partition.buffered_blocks.is_empty() {
                continue;
            }
            if let Some(blocks) = partition.take_blocks(Some(spill_unit_size)) {
                partition.spill_blocks(&self.ctx, &self.prefix, self.writer_pool_bytes, blocks)?;
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
            partition.spill_blocks(&self.ctx, &self.prefix, self.writer_pool_bytes, blocks)?;
        } else {
            self.can_spill = false;
        }
        Ok(())
    }

    #[fastrace::trace(name = "WindowPartitionBuffer::restore")]
    pub fn restore(&mut self) -> Result<Vec<DataBlock>> {
        for partition in &mut self.partitions[self.next_to_restore..] {
            self.next_to_restore += 1;

            let mut result = Vec::new();
            for reader in partition.take_readers(&self.ctx)? {
                let blocks = reader.restore()?;
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
