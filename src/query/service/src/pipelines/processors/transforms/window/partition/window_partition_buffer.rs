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

use std::ops::Range;
use std::sync::Arc;

use databend_base::uniq_id::GlobalUniq;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline::core::check_interrupt;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_pipeline_transforms::sorts::SortedBlockIterator;
use databend_common_pipeline_transforms::traits::Location;
use databend_common_storage::DataOperator;
use databend_common_storages_parquet::ReadSettings;
use parquet::file::metadata::RowGroupMetaData;

use crate::pipelines::processors::transforms::DataProcessorStrategy;
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

/// Bytes the blocks take once written: constant columns are expanded to full
/// columns, though memory_size() counts them once.
fn written_size(blocks: &[DataBlock]) -> usize {
    blocks
        .iter()
        .map(|block| block.estimate_block_size(block.num_columns()))
        .sum()
}

struct PartitionFileWriter {
    path: String,
    writer: SpillsDataWriter,
    schema: Option<DataSchemaRef>,
    row_groups: usize,
    // Each run is a sorted sequence of consecutive row groups.
    runs: Vec<Range<usize>>,
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
            row_groups: 0,
            runs: Vec::new(),
        })
    }

    /// Write one sorted run, one row group per page, so it can be read back
    /// page by page.
    fn write_run(&mut self, run: SortedBlockIterator) -> Result<usize> {
        let start = self.row_groups;
        for page in run {
            check_interrupt()?;
            let page = page?;
            if page.is_empty() {
                continue;
            }
            if self.schema.is_none() {
                self.schema = Some(Arc::new(page.infer_schema()));
            }
            self.writer.write(page)?;
            self.row_groups = self.writer.flush_row_groups()?;
        }
        if self.row_groups > start {
            self.runs.push(start..self.row_groups);
        }
        Ok(self.row_groups)
    }

    fn close(self, ctx: &Arc<QueryContext>) -> Result<PartitionFileReader> {
        let Self {
            path,
            writer,
            schema,
            runs,
            ..
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
            runs,
            schema: schema.unwrap_or_else(|| Arc::new(Default::default())),
        })
    }
}

struct PartitionFileReader {
    path: String,
    row_groups: Vec<RowGroupMetaData>,
    runs: Vec<Range<usize>>,
    schema: DataSchemaRef,
}

impl PartitionFileReader {
    /// Open every run of this file. Each reader fetches its row groups in
    /// order and holds at most one ahead of the page being merged.
    fn open_runs(self) -> Result<Vec<SortedBlockIterator>> {
        let data_operator = DataOperator::instance();
        let target = SpillTarget::from_storage_params(data_operator.spill_params());
        let buffer_pool = SpillsBufferPool::instance();
        self.runs
            .iter()
            .filter(|run| !run.is_empty())
            .map(|run| {
                let mut reader = buffer_pool.reader(
                    data_operator.spill_operator(),
                    self.path.clone(),
                    self.schema.clone(),
                    self.row_groups[run.clone()].to_vec(),
                    target,
                    ReadSettings::default(),
                )?;
                Ok(Box::new(std::iter::from_fn(move || {
                    check_interrupt().and_then(|_| reader.read()).transpose()
                })) as SortedBlockIterator)
            })
            .collect()
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
    spilled_size: usize,
}

impl PartitionSlot {
    fn new(id: usize) -> Self {
        Self {
            id,
            state: Default::default(),
            readers: Default::default(),
            buffered_blocks: Default::default(),
            buffered_size: Default::default(),
            spilled_size: Default::default(),
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
        run: SortedBlockIterator,
    ) -> Result<()> {
        log::debug!(id = self.id; "spill new run");

        match &mut self.state {
            PartitionSpillState::Empty => {
                let mut writer = PartitionFileWriter::create(prefix, writer_pool_bytes)?;
                let row_group_count = writer.write_run(run)?;
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

                let row_group_count = writer.write_run(run)?;

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
    pub fn spill(&mut self, strategy: &impl DataProcessorStrategy) -> Result<()> {
        let spill_unit_size = self.memory_settings.spill_unit_size;

        let mut preferred_partition = None;
        for partition in self.partitions[self.next_to_restore..].iter_mut().rev() {
            if partition.buffered_blocks.is_empty() {
                continue;
            }
            let spilled_size = partition.spilled_size;
            if let Some(blocks) = partition.take_blocks(Some(spill_unit_size)) {
                partition.spilled_size += written_size(&blocks);
                let blocks = concat_data_blocks(blocks, self.sort_block_size)?;
                let run = strategy.sort_run(blocks, spilled_size)?;
                partition.spill_blocks(&self.ctx, &self.prefix, self.writer_pool_bytes, run)?;
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
            let spilled_size = partition.spilled_size;
            let blocks = partition.take_blocks(None).unwrap();
            partition.spilled_size += written_size(&blocks);
            let blocks = concat_data_blocks(blocks, self.sort_block_size)?;
            let run = strategy.sort_run(blocks, spilled_size)?;
            partition.spill_blocks(&self.ctx, &self.prefix, self.writer_pool_bytes, run)?;
        } else {
            self.can_spill = false;
        }
        Ok(())
    }

    /// Merge the next non-empty partition: its spilled runs are read page by
    /// page, so the partition is never loaded into memory as a whole.
    #[fastrace::trace(name = "WindowPartitionBuffer::restore")]
    pub fn restore(
        &mut self,
        strategy: &impl DataProcessorStrategy,
    ) -> Result<Option<SortedBlockIterator>> {
        for partition in &mut self.partitions[self.next_to_restore..] {
            check_interrupt()?;
            self.next_to_restore += 1;

            let mut runs = Vec::new();
            for reader in partition.take_readers(&self.ctx)? {
                runs.extend(reader.open_runs()?);
            }
            if let Some(blocks) = partition.take_blocks(None)
                && !blocks.is_empty()
            {
                let blocks = concat_data_blocks(blocks, self.sort_block_size)?;
                runs.push(strategy.sort_run(blocks, partition.spilled_size)?);
            }

            if !runs.is_empty() {
                return Ok(Some(strategy.merge_runs(runs)?));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::BlockEntry;
    use databend_common_expression::FromData;
    use databend_common_expression::SortColumnDescription;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::UInt64Type;
    use databend_common_expression::utils::block_debug::pretty_format_blocks;

    use super::*;
    use crate::pipelines::processors::transforms::SortStrategy;
    use crate::sessions::TableContextSettings;
    use crate::test_kits::TestFixture;

    #[tokio::test(flavor = "multi_thread")]
    async fn spilled_runs_merge_back_page_by_page() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();
        settings.set_setting("max_block_size".to_string(), "100".to_string())?;

        let rows = 20_000u64;
        let input = DataBlock::new_from_columns(vec![
            UInt64Type::from_data((0..rows).map(|i| i * 7919 % rows).collect::<Vec<_>>()),
            StringType::from_data(
                (0..rows)
                    .map(|i| format!("payload-{i}"))
                    .collect::<Vec<_>>(),
            ),
        ]);
        let sort_desc = vec![SortColumnDescription {
            offset: 0,
            asc: true,
            nulls_first: false,
        }];
        let strategy =
            SortStrategy::try_create(&settings, sort_desc.clone(), Arc::new(input.infer_schema()))?;
        // Every check reports pressure; spill once 64 KiB is buffered.
        let memory = MemorySettings::builder()
            .with_workload_group(false)
            .with_max_memory_usage(0)
            .with_spill_unit_size(64 * 1024)
            .build();
        let mut buffer = WindowPartitionBuffer::new(
            ctx.clone(),
            ctx.query_id_spill_prefix(),
            4 * 1024 * 1024,
            1,
            1024,
            memory,
        )?;
        buffer.min_row_group_size = 1;

        let blocks = (0..rows as usize)
            .step_by(500)
            .map(|start| input.slice(start..start + 500))
            .collect::<Vec<_>>();
        let (resident, spilled) = blocks.split_at(2);
        for block in spilled {
            buffer.add_data_block(0, block.clone());
            while buffer.need_spill() {
                buffer.spill(&strategy)?;
            }
        }
        // The tail stays in memory and is merged with the spilled runs.
        for block in resident {
            buffer.add_data_block(0, block.clone());
        }

        let PartitionSpillState::Writing { writer } = &buffer.partitions[0].state else {
            panic!("partition should have spilled runs");
        };
        assert!(writer.runs.len() >= 3, "{:?}", writer.runs);
        assert!(writer.runs.iter().any(|run| run.len() > 1));

        let mut merged = buffer.restore(&strategy)?.unwrap();
        let mut output = Vec::new();
        while let Some(block) = merged.next().transpose()? {
            assert!(block.num_rows() <= 100);
            output.push(block);
        }
        assert!(buffer.restore(&strategy)?.is_none());
        assert_eq!(
            pretty_format_blocks(&[DataBlock::concat(&output)?])?,
            pretty_format_blocks(&[DataBlock::sort(&input, &sort_desc, None)?])?
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn runs_are_split_once_partition_is_large() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();

        // About 200 bytes per row; every 4 MiB spill becomes one run. Blocks
        // are built separately, as slices would report the whole buffer.
        let rows = 40_000u64;
        let make_block = |range: std::ops::Range<u64>| {
            DataBlock::new_from_columns(vec![
                UInt64Type::from_data(range.clone().map(|i| i * 7919 % rows).collect::<Vec<_>>()),
                StringType::from_data(range.map(|i| format!("{i:0>200}")).collect::<Vec<_>>()),
            ])
        };
        let blocks = (0..rows)
            .step_by(500)
            .map(|start| make_block(start..start + 500))
            .collect::<Vec<_>>();
        let input = DataBlock::concat(&blocks)?;
        let sort_desc = vec![SortColumnDescription {
            offset: 0,
            asc: true,
            nulls_first: false,
        }];
        let strategy =
            SortStrategy::try_create(&settings, sort_desc.clone(), Arc::new(input.infer_schema()))?;
        let spill_unit = 4 * 1024 * 1024;
        let memory = MemorySettings::builder()
            .with_workload_group(false)
            .with_max_memory_usage(0)
            .with_spill_unit_size(spill_unit)
            .build();
        let mut buffer = WindowPartitionBuffer::new(
            ctx.clone(),
            ctx.query_id_spill_prefix(),
            4 * 1024 * 1024,
            1,
            1024,
            memory,
        )?;
        buffer.min_row_group_size = spill_unit;

        let mut added = 0;
        for block in blocks {
            added += written_size(std::slice::from_ref(&block));
            buffer.add_data_block(0, block);
            while buffer.need_spill() {
                buffer.spill(&strategy)?;
            }
        }
        let partition = &buffer.partitions[0];
        let buffered = written_size(&partition.buffered_blocks);
        assert_eq!(partition.spilled_size + buffered, added);

        // The partition is small: each 4 MiB run stays in one page.
        let PartitionSpillState::Writing { writer } = &partition.state else {
            panic!("partition should have spilled runs");
        };
        assert!(writer.runs.len() >= 2, "{:?}", writer.runs);
        assert!(
            writer.runs.iter().all(|run| run.len() == 1),
            "{:?}",
            writer.runs
        );

        // Once the partition is large, the same run is split into pages of
        // the 1 MiB lower bound.
        let run = make_block(0..20_000);
        assert_eq!(strategy.sort_run(vec![run.clone()], 0)?.count(), 1);
        assert!(strategy.sort_run(vec![run], 64 * 1024 * 1024)?.count() >= 3);

        let mut merged = buffer.restore(&strategy)?.unwrap();
        let mut output = Vec::new();
        while let Some(block) = merged.next().transpose()? {
            output.push(block);
        }
        assert_eq!(
            pretty_format_blocks(&[DataBlock::concat(&output)?])?,
            pretty_format_blocks(&[DataBlock::sort(&input, &sort_desc, None)?])?
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pages_count_constant_columns_at_full_size() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();

        // A 200-byte constant is stored once in memory but written to every
        // row, so 60K rows make about 12 MiB of pages.
        let rows = 60_000u64;
        let block = DataBlock::new(
            vec![
                UInt64Type::from_data((0..rows).map(|i| i * 7919 % rows).collect::<Vec<_>>())
                    .into(),
                BlockEntry::new_const_column_arg::<StringType>("x".repeat(200), rows as usize),
            ],
            rows as usize,
        );
        let sort_desc = vec![SortColumnDescription {
            offset: 0,
            asc: true,
            nulls_first: false,
        }];
        let strategy =
            SortStrategy::try_create(&settings, sort_desc, Arc::new(block.infer_schema()))?;

        // The partition counts what it writes, not the in-memory size.
        let memory = MemorySettings::builder()
            .with_workload_group(false)
            .with_max_memory_usage(0)
            .with_spill_unit_size(usize::MAX)
            .build();
        let mut buffer = WindowPartitionBuffer::new(
            ctx.clone(),
            ctx.query_id_spill_prefix(),
            4 * 1024 * 1024,
            1,
            1024,
            memory,
        )?;
        buffer.min_row_group_size = 1;
        buffer.add_data_block(0, block.clone());
        buffer.spill(&strategy)?;
        assert!(buffer.partitions[0].spilled_size >= 200 * rows as usize);

        let pages = strategy
            .sort_run(vec![block], 0)?
            .collect::<Result<Vec<_>>>()?;
        assert!(pages.len() >= 2, "{}", pages.len());
        for page in &pages {
            let page_bytes = page.estimate_block_size(page.num_columns());
            assert!(page_bytes <= 8 * 1024 * 1024, "{page_bytes}");
        }
        assert_eq!(
            pages.iter().map(DataBlock::num_rows).sum::<usize>(),
            rows as usize
        );
        Ok(())
    }
}
