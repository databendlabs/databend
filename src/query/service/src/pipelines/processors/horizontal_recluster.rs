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

//! Task-local external merge of independently ordered FUSE blocks. Temporary
//! runs are sequences of bounded spill chunks, never whole in-memory runs.

use std::cmp::Reverse;
use std::collections::VecDeque;
use std::mem;
use std::ops::Range;
use std::sync::Arc;

use databend_common_base::base::ProgressValues;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::ReclusterTask;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::AbortChecker;
use databend_common_catalog::table_context::TableContextPartitionStats;
use databend_common_catalog::table_context::TableContextProgress;
use databend_common_catalog::table_context::TableContextQueryState;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::UInt64Type;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::sources::SyncSource;
use databend_common_pipeline::sources::SyncSourcer;
use databend_common_pipeline_transforms::Transform;
use databend_common_pipeline_transforms::blocks::CompoundBlockOperator;
use databend_common_pipeline_transforms::sorts::core::LoserTreeMerger;
use databend_common_pipeline_transforms::sorts::core::RowConverter;
use databend_common_pipeline_transforms::sorts::core::Rows;
use databend_common_pipeline_transforms::sorts::core::RowsTypeVisitor;
use databend_common_pipeline_transforms::sorts::core::SortKeyDescription;
use databend_common_pipeline_transforms::sorts::core::SortedStream;
use databend_common_pipeline_transforms::sorts::core::select_row_type;
use databend_common_pipeline_transforms::sorts::try_add_multi_sort_merge_with_flush_before_refill;
use databend_common_pipeline_transforms::traits::SortSpiller;
use databend_common_pipeline_transforms::traits::SpillReader;
use databend_common_storages_fuse::FuseBlockPartInfo;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::BlockReader;
use databend_common_storages_fuse::io::FuseLowLevelBlockReadOptions;
use databend_common_storages_fuse::io::FuseLowLevelBlockReader;
use databend_common_storages_fuse::io::FuseLowLevelFullRowReader;
use databend_common_storages_fuse::operations::ReclusterIndexInput;
use databend_common_storages_fuse::operations::ReclusterIndexRowRange;
use databend_common_storages_fuse::statistics::ClusterStatsGenerator;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::Versioned;

use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;
use crate::spillers::SortSpillerImpl;

#[derive(Clone)]
struct ReclusterMergeInputBlock {
    meta: Arc<BlockMeta>,
    ordinal: u32,
}

#[derive(Clone)]
struct ReclusterMergeConfig {
    ctx: Arc<QueryContext>,
    abort_checker: AbortChecker,
    table: FuseTable,
    schema: TableSchemaRef,
    defaults: Vec<Scalar>,
    eval: CompoundBlockOperator,
    keys: SortKeyDescription,
    read_settings: ReadSettings,
    batch_rows: usize,
    output_batch_rows: usize,
    minimum_batch: bool,
    lineage: bool,
    emit_order: bool,
}

/// Source ordinals always refer to original task inputs, including after spill.
#[derive(Debug, PartialEq, Eq)]
pub struct ReclusterRowOriginRange {
    pub source_ordinal: u32,
    pub rows: Range<u32>,
}

pub struct TransformReclusterOutputOrder {
    pub next_row: u64,
}

impl Transform for TransformReclusterOutputOrder {
    const NAME: &'static str = "ReclusterOutputOrder";

    fn transform(&mut self, mut block: DataBlock) -> Result<DataBlock> {
        let end = self
            .next_row
            .checked_add(block.num_rows() as u64)
            .ok_or_else(|| ErrorCode::Internal("recluster output row overflow"))?;
        let order = UInt64Type::from_data((self.next_row..end).collect::<Vec<_>>());
        block.add_column(order);
        self.next_row = end;
        Ok(block)
    }
}

pub struct TransformPrepareReclusterIndex {
    pub merged_names: Vec<String>,
}

impl Transform for TransformPrepareReclusterIndex {
    const NAME: &'static str = "PrepareReclusterIndexLineage";

    fn transform(&mut self, mut block: DataBlock) -> Result<DataBlock> {
        let order = block.get_last_column().clone();
        let Column::Number(NumberColumn::UInt64(order)) = order else {
            return Err(ErrorCode::Internal("missing recluster output order"));
        };
        let output_row = order
            .first()
            .copied()
            .ok_or_else(|| ErrorCode::Internal("empty recluster output order"))?;
        for (index, &row) in order.iter().enumerate() {
            if row != output_row + index as u64 {
                return Err(ErrorCode::Internal("non-contiguous recluster output order"));
            }
        }
        block.pop_columns(1);
        let (block, origins) = extract_recluster_lineage(block)?;
        let rows = origins
            .into_iter()
            .map(|origin| ReclusterIndexRowRange {
                source: origin.source_ordinal,
                rows: origin.rows,
            })
            .collect();
        block.add_meta(Some(Box::new(ReclusterIndexInput {
            rows,
            output_row,
            merged_names: self.merged_names.clone(),
        })))
    }
}

/// Extract lineage after final compact establishes output block boundaries.
pub fn extract_recluster_lineage(
    mut block: DataBlock,
) -> Result<(DataBlock, Vec<ReclusterRowOriginRange>)> {
    if block.num_columns() < 2 {
        return Err(ErrorCode::Internal("missing recluster lineage columns"));
    }
    let n = block.num_columns();
    let input_block = block.get_by_offset(n - 2).to_column();
    let offset = block.get_by_offset(n - 1).to_column();
    let Column::Number(NumberColumn::UInt32(input_block)) = input_block else {
        return Err(ErrorCode::Internal("invalid lineage source type"));
    };
    let Column::Number(NumberColumn::UInt32(offset)) = offset else {
        return Err(ErrorCode::Internal("invalid lineage offset type"));
    };
    if input_block.len() != block.num_rows() || offset.len() != block.num_rows() {
        return Err(ErrorCode::Internal("recluster lineage row count mismatch"));
    }
    let mut ranges: Vec<ReclusterRowOriginRange> = Vec::new();
    for (&input_block, &row) in input_block.iter().zip(offset.iter()) {
        let end = row
            .checked_add(1)
            .ok_or_else(|| ErrorCode::BadArguments("lineage row exceeds u32"))?;
        if let Some(last) = ranges.last_mut()
            && last.source_ordinal == input_block
            && last.rows.end == row
        {
            last.rows.end = end;
        } else {
            ranges.push(ReclusterRowOriginRange {
                source_ordinal: input_block,
                rows: row..end,
            });
        }
    }
    block.pop_columns(2);
    Ok((block, ranges))
}

enum ReclusterMergeInput {
    Original {
        input_block: ReclusterMergeInputBlock,
        reader: Option<FuseLowLevelFullRowReader>,
        position: usize,
    },
    Spill(VecDeque<String>),
}

struct ReclusterMergeStream<R: Rows> {
    merge_input: ReclusterMergeInput,
    head: Option<(DataBlock, Column)>,
    converter: R::Converter,
    last_key: Option<Scalar>,
    merge_config: ReclusterMergeConfig,
    spiller: SortSpillerImpl,
}

impl<R: Rows> ReclusterMergeStream<R> {
    fn new(
        merge_input: ReclusterMergeInput,
        merge_config: &ReclusterMergeConfig,
        spiller: &SortSpillerImpl,
    ) -> Result<Self> {
        Ok(Self {
            converter: R::Converter::new(merge_config.keys.clone())?,
            merge_input,
            head: None,
            last_key: None,
            merge_config: merge_config.clone(),
            spiller: spiller.clone(),
        })
    }

    fn validate_batch(&mut self, block: DataBlock, keys: Column) -> Result<(DataBlock, Column)> {
        let rows = R::from_column(&keys)?;
        if block.is_empty() || rows.len() != block.num_rows() {
            return Err(ErrorCode::ParquetFileInvalid(
                "invalid recluster merge batch row count",
            ));
        }
        if let Some(previous) = &self.last_key
            && R::scalar_as_item(previous) > rows.first()
        {
            return Err(ErrorCode::ParquetFileInvalid(
                "recluster input is not ordered across batches",
            ));
        }
        for row in 1..rows.len() {
            if rows.row(row - 1) > rows.row(row) {
                return Err(ErrorCode::ParquetFileInvalid(
                    "recluster input batch is not ordered",
                ));
            }
        }
        self.last_key = Some(R::owned_item(rows.last()));
        Ok((block, keys))
    }
}

impl<R: Rows> SortedStream for ReclusterMergeStream<R>
where R::Converter: Send
{
    fn next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        self.merge_config.abort_checker.try_check_aborting()?;
        if let Some(head) = self.head.take() {
            return Ok((Some(head), false));
        }
        let mut block = match &mut self.merge_input {
            ReclusterMergeInput::Spill(paths) => {
                let Some(path) = paths.pop_front() else {
                    return Ok((None, false));
                };
                let mut block = self.spiller.reader(&path)?.read()?;
                let key = block.get_last_column().clone();
                block.pop_columns(1);
                let batch = self.validate_batch(block, key)?;
                return Ok((Some(batch), false));
            }
            ReclusterMergeInput::Original {
                input_block,
                reader,
                position,
            } => {
                if *position == input_block.meta.row_count as usize {
                    if let Some(reader) = reader.take() {
                        reader.finish()?;
                    }
                    return Ok((None, false));
                }
                if reader.is_none() {
                    let options = FuseLowLevelBlockReadOptions::new(
                        self.merge_config.table.get_operator(),
                        self.merge_config.schema.clone(),
                        input_block.meta.clone(),
                    )
                    .with_default_values(self.merge_config.defaults.clone())
                    .with_stream_table_version(self.merge_config.table.get_table_info().ident.seq)
                    .with_window_rows(self.merge_config.batch_rows)
                    .with_max_prefetch(1)
                    .with_merge_io(self.merge_config.read_settings);
                    let block_reader = FuseLowLevelBlockReader::create(options)?;
                    let mut reopened = block_reader.read_full_rows()?;
                    // Skip the decoded prefix when reopening a recovered stream.
                    let mut skipped = 0;
                    while skipped < *position {
                        self.merge_config.abort_checker.try_check_aborting()?;
                        let rows = (*position - skipped).min(self.merge_config.batch_rows);
                        reopened.read(rows, false)?.ok_or_else(|| {
                            ErrorCode::ParquetFileInvalid("premature EOF reopening recluster input")
                        })?;
                        skipped += rows;
                    }
                    *reader = Some(reopened);
                }
                let minimum = self.merge_config.minimum_batch;
                let Some(block) = reader
                    .as_mut()
                    .expect("initialized reader")
                    .read(self.merge_config.batch_rows, minimum)?
                else {
                    reader.take().expect("initialized reader").finish()?;
                    return Ok((None, false));
                };
                let progress = self.merge_config.ctx.get_scan_progress();
                progress.incr(&ProgressValues {
                    rows: block.num_rows(),
                    bytes: block.memory_size(),
                });
                let rows = block.num_rows();
                let mut block = self.merge_config.eval.transform(block)?.maybe_gc();
                if self.merge_config.lineage {
                    let start = u32::try_from(*position)
                        .map_err(|_| ErrorCode::BadArguments("input_block row exceeds u32"))?;
                    let end = u32::try_from(*position + rows)
                        .map_err(|_| ErrorCode::BadArguments("input_block row exceeds u32"))?;
                    block.add_column(UInt32Type::from_data(vec![input_block.ordinal; rows]));
                    block.add_column(UInt32Type::from_data((start..end).collect::<Vec<_>>()));
                }
                *position += rows;
                if *position == input_block.meta.row_count as usize {
                    // Release reader buffers before the final batch is consumed.
                    reader.take().expect("initialized reader").finish()?;
                }
                block
            }
        };
        let rows = self.converter.convert(&block)?;
        let keys = rows.to_column();
        block.take_meta();
        let batch = self.validate_batch(block, keys)?;
        Ok((Some(batch), false))
    }
}

trait ReclusterMergeExecution: Send {
    /// One synchronous work unit. None means a spill unit completed, not EOF.
    fn step(&mut self) -> Result<Option<DataBlock>>;
    fn finished(&self) -> bool;
}

struct ReclusterMergeSpillJob<R: Rows> {
    merger: LoserTreeMerger<R, ReclusterMergeStream<R>>,
    paths: VecDeque<String>,
}

struct ReclusterExternalMerge<R: Rows> {
    merge_config: ReclusterMergeConfig,
    spiller: SortSpillerImpl,
    pending: VecDeque<ReclusterMergeStream<R>>,
    completed: VecDeque<ReclusterMergeStream<R>>,
    job: Option<ReclusterMergeSpillJob<R>>,
    final_merge: Option<LoserTreeMerger<R, ReclusterMergeStream<R>>>,
    fan_in: usize,
    force_initial: bool,
    done: bool,
    output_rows: usize,
    expected_rows: usize,
}

impl<R: Rows + 'static> ReclusterExternalMerge<R>
where R::Converter: Send
{
    fn merger(
        &self,
        merge_streams: Vec<ReclusterMergeStream<R>>,
    ) -> LoserTreeMerger<R, ReclusterMergeStream<R>> {
        LoserTreeMerger::new(merge_streams, self.merge_config.output_batch_rows, None)
            .with_flush_before_refill()
    }

    fn spill_chunk(&self, mut block: DataBlock) -> Result<String> {
        // Spill chunks carry an explicit key column, including simple-row encodings.
        let converter = R::Converter::new(self.merge_config.keys.clone())?;
        let keys = converter.convert(&block)?.to_column();
        block.add_column(keys);
        self.spiller.spill(block)
    }

    fn recover_inputs(
        &mut self,
        merger: LoserTreeMerger<R, ReclusterMergeStream<R>>,
    ) -> Result<()> {
        for recovered in merger.into_remaining_streams()? {
            let mut stream = recovered.stream;
            stream.head = recovered.head;
            if let ReclusterMergeInput::Original { reader, .. } = &mut stream.merge_input {
                *reader = None;
            }
            self.pending.push_back(stream);
        }
        self.force_initial = true;
        let previous_fan_in = self.fan_in;
        self.fan_in = (self.fan_in / 2).max(2);
        log::info!(
            "recluster multiway merge reduces fan-in: {} -> {}, output_rows={}",
            previous_fan_in,
            self.fan_in,
            self.output_rows
        );
        Ok(())
    }

    fn pressure(&self) -> Result<bool> {
        // Ignore the forced-spill zero threshold after the initial round.
        let settings = self.merge_config.ctx.get_settings();
        let forced_spill = settings.get_force_sort_data_spill()?;
        Ok(!forced_spill && self.spiller.memory_settings().check_spill())
    }
}

impl<R: Rows + 'static> ReclusterMergeExecution for ReclusterExternalMerge<R>
where R::Converter: Send
{
    fn finished(&self) -> bool {
        self.done
    }

    fn step(&mut self) -> Result<Option<DataBlock>> {
        self.merge_config.abort_checker.try_check_aborting()?;
        if let Some(mut merger) = self.final_merge.take() {
            let block = merger.next_block()?.map(DataBlock::maybe_gc);
            if let Some(ref block) = block {
                self.output_rows += block.num_rows();
            }
            if merger.is_finished() {
                if self.output_rows != self.expected_rows {
                    return Err(ErrorCode::Internal(
                        "recluster merge output row count mismatch",
                    ));
                }
                self.done = true;
            } else if self.pressure()? {
                if !self.merge_config.ctx.get_enable_sort_spill() {
                    return Err(ErrorCode::MemoryExceedsLimit(
                        "recluster merge is under memory pressure with spill disabled",
                    ));
                }
                // Preserve unconsumed suffixes after emitting the selected prefix.
                if self.fan_in <= 2 {
                    return Err(ErrorCode::MemoryExceedsLimit(
                        "minimum two-way recluster merge cannot fit under current memory pressure",
                    ));
                }
                self.recover_inputs(merger)?;
            } else {
                self.final_merge = Some(merger);
            }
            let block = match block {
                Some(mut block) => {
                    if self.merge_config.emit_order
                        && !self.merge_config.keys.uses_source_sort_col()
                    {
                        let converter = R::Converter::new(self.merge_config.keys.clone())?;
                        block.add_column(converter.convert(&block)?.to_column());
                    }
                    Some(block)
                }
                None => None,
            };
            return Ok(block);
        }
        if let Some(mut job) = self.job.take() {
            if let Some(block) = job.merger.next_block()? {
                job.paths.push_back(self.spill_chunk(block.maybe_gc())?);
            }
            if !job.merger.is_finished() && self.pressure()? {
                // Keep the spilled prefix separate from the recovered suffixes.
                if self.fan_in <= 2 {
                    return Err(ErrorCode::MemoryExceedsLimit(
                        "minimum external recluster merge cannot fit under current memory pressure",
                    ));
                }
                self.recover_inputs(job.merger)?;
                if !job.paths.is_empty() {
                    self.completed.push_back(ReclusterMergeStream::new(
                        ReclusterMergeInput::Spill(job.paths),
                        &self.merge_config,
                        &self.spiller,
                    )?);
                }
                return Ok(None);
            }
            if job.merger.is_finished() {
                if !job.paths.is_empty() {
                    self.completed.push_back(ReclusterMergeStream::new(
                        ReclusterMergeInput::Spill(job.paths),
                        &self.merge_config,
                        &self.spiller,
                    )?);
                }
            } else {
                self.job = Some(job);
            }
            return Ok(None);
        }
        if self.pending.is_empty() {
            self.pending = mem::take(&mut self.completed);
            self.force_initial = false;
        }
        if !self.force_initial && self.completed.is_empty() && self.pending.len() <= self.fan_in {
            let mut merge_streams = self.pending.drain(..).collect::<Vec<_>>();
            if merge_streams.is_empty() {
                if self.output_rows != self.expected_rows {
                    return Err(ErrorCode::Internal(
                        "empty recluster merge lost merge_input rows",
                    ));
                }
                self.done = true;
                return Ok(None);
            }
            if merge_streams.len() == 1 {
                merge_streams.push(ReclusterMergeStream::new(
                    ReclusterMergeInput::Spill(VecDeque::new()),
                    &self.merge_config,
                    &self.spiller,
                )?);
            }
            self.final_merge = Some(self.merger(merge_streams));
            return Ok(None);
        }
        if !self.merge_config.ctx.get_enable_sort_spill() {
            return Err(ErrorCode::MemoryExceedsLimit(
                "recluster merge needs external runs but sort spill is disabled",
            ));
        }
        let count = match self.force_initial {
            true => 1,
            false => self.fan_in.min(self.pending.len()),
        };
        let mut merge_streams = self.pending.drain(..count).collect::<Vec<_>>();
        if merge_streams.len() == 1 {
            merge_streams.push(ReclusterMergeStream::new(
                ReclusterMergeInput::Spill(VecDeque::new()),
                &self.merge_config,
                &self.spiller,
            )?);
        }
        self.job = Some(ReclusterMergeSpillJob {
            merger: self.merger(merge_streams),
            paths: VecDeque::new(),
        });
        Ok(None)
    }
}

struct ReclusterMergeFactory {
    merge_config: ReclusterMergeConfig,
    input_blocks: Vec<ReclusterMergeInputBlock>,
    spiller: SortSpillerImpl,
    fan_in: usize,
    force_initial: bool,
    expected_rows: usize,
}

impl RowsTypeVisitor for ReclusterMergeFactory {
    type Result = Result<Box<dyn ReclusterMergeExecution>>;
    fn sort_key_desc(&self) -> SortKeyDescription {
        self.merge_config.keys.clone()
    }
    fn visit_type<R>(&mut self) -> Self::Result
    where
        R: Rows + 'static,
        R::Converter: Send + 'static,
    {
        let merge_streams = self
            .input_blocks
            .drain(..)
            .map(|input_block| {
                ReclusterMergeStream::<R>::new(
                    ReclusterMergeInput::Original {
                        input_block,
                        reader: None,
                        position: 0,
                    },
                    &self.merge_config,
                    &self.spiller,
                )
            })
            .collect::<Result<VecDeque<_>>>()?;
        Ok(Box::new(ReclusterExternalMerge::<R> {
            merge_config: self.merge_config.clone(),
            spiller: self.spiller.clone(),
            pending: merge_streams,
            completed: VecDeque::new(),
            job: None,
            final_merge: None,
            fan_in: self.fan_in,
            force_initial: self.force_initial,
            done: false,
            output_rows: 0,
            expected_rows: self.expected_rows,
        }))
    }
}

pub struct HorizontalReclusterSource {
    merge_execution: Box<dyn ReclusterMergeExecution>,
}

impl HorizontalReclusterSource {
    /// Build parallel group mergers followed by one nonblocking input-port merger.
    pub fn build_pipeline(
        ctx: Arc<QueryContext>,
        pipeline: &mut Pipeline,
        table: FuseTable,
        task: &ReclusterTask,
        stats: &ClusterStatsGenerator,
        lineage: bool,
    ) -> Result<()> {
        let merge_factory = ReclusterMergeFactory::prepare(ctx, table, task, stats, lineage)?;
        let settings = merge_factory.merge_config.ctx.get_settings();
        let fixed = settings.get_enable_fixed_rows_sort()?;
        let max_groups = settings.get_max_threads()? as usize;
        let groups = max_groups.min(merge_factory.input_blocks.len()).max(1);
        let merge_keys = merge_factory.merge_config.keys.clone();
        let output_rows = merge_factory.merge_config.output_batch_rows;
        let group_factories = merge_factory.into_groups(groups)?;
        let mut pipe = Vec::with_capacity(groups);
        for mut factory in group_factories {
            let output = OutputPort::create();
            let merge_execution = select_row_type(&mut factory, fixed)?;
            let source = Self { merge_execution };
            pipe.push(PipeItem::create(
                SyncSourcer::create(
                    factory.merge_config.ctx.get_scan_progress(),
                    output.clone(),
                    source,
                )?,
                vec![],
                vec![output],
            ));
        }
        pipeline.add_pipe(Pipe::create(0, groups, pipe));
        if groups > 1 {
            try_add_multi_sort_merge_with_flush_before_refill(
                pipeline,
                merge_keys,
                output_rows,
                None,
                true,
                true,
                fixed,
            )?;
        }
        Ok(())
    }
}

impl ReclusterMergeFactory {
    fn prepare(
        ctx: Arc<QueryContext>,
        table: FuseTable,
        task: &ReclusterTask,
        stats: &ClusterStatsGenerator,
        lineage: bool,
    ) -> Result<Self> {
        let settings = ctx.get_settings();
        let schema = table.schema_with_stream();
        if lineage
            && schema
                .fields()
                .iter()
                .any(|field| matches!(field.data_type(), TableDataType::AggregateState { .. }))
        {
            return Err(ErrorCode::Unimplemented(
                "recluster lineage does not support aggregate-state reaggregation",
            ));
        }
        let block_reader = BlockReader::create(
            ctx.clone(),
            table.get_operator(),
            schema.clone(),
            Projection::Columns((0..schema.num_fields()).collect()),
            false,
        )?;
        let defaults = block_reader.default_values().to_vec();
        let read_settings = ReadSettings {
            max_gap_size: settings.get_storage_io_min_bytes_for_seek()?,
            max_range_size: settings.get_storage_io_max_page_bytes_for_read()?,
            parquet_fast_read_bytes: settings.get_parquet_fast_read_bytes()?,
        };
        let requested_rows = (settings.get_max_block_size()? as usize).clamp(1, 8192);
        let mut input_blocks = Vec::with_capacity(task.parts.len());
        for (ordinal, part) in task.parts.partitions.iter().enumerate() {
            let part = FuseBlockPartInfo::from_part(part)?;
            let meta = BlockMeta::new(
                part.nums_rows as _,
                0,
                0,
                part.columns_stat.clone().unwrap_or_default(),
                part.columns_meta.clone(),
                None,
                (part.location.clone(), DataBlock::VERSION),
                part.bloom_filter_index_location.clone(),
                part.bloom_filter_index_size,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                part.compression,
                part.create_on,
            );
            input_blocks.push(ReclusterMergeInputBlock {
                meta: Arc::new(meta),
                ordinal: u32::try_from(ordinal)
                    .map_err(|_| ErrorCode::BadArguments("too many recluster input_blocks"))?,
            });
        }
        let batch_rows = requested_rows;
        let output_batch_rows = requested_rows;
        // Without spill, all input streams must merge directly.
        let fan_in = match ctx.get_enable_sort_spill() {
            true => input_blocks.len().clamp(2, 64),
            false => input_blocks.len().max(2),
        };
        let force_initial = ctx.get_enable_sort_spill() && settings.get_force_sort_data_spill()?;
        let minimum_batch = false;
        log::info!(
            "recluster multiway merge: inputs={}, rows={}, fan_in={}, batch_rows={}, force_spill={}",
            input_blocks.len(),
            task.total_rows,
            fan_in,
            batch_rows,
            force_initial
        );
        let fixed = settings.get_enable_fixed_rows_sort()?;
        let mut merge_fields = stats.out_fields.clone();
        if lineage {
            merge_fields.push(DataField::new(
                "__recluster_source_ordinal",
                DataType::Number(NumberDataType::UInt32),
            ));
            merge_fields.push(DataField::new(
                "__recluster_source_row",
                DataType::Number(NumberDataType::UInt32),
            ));
        }
        let merge_config = ReclusterMergeConfig {
            ctx: ctx.clone(),
            abort_checker: ctx.clone().get_abort_checker(),
            table,
            defaults,
            schema: schema.clone(),
            eval: CompoundBlockOperator::new(
                stats.eval_operators.clone(),
                stats.func_ctx.clone(),
                schema.num_fields(),
            ),
            keys: SortKeyDescription::new(
                stats.sort_descs().into(),
                DataSchemaRefExt::create(merge_fields),
                fixed,
            )?,
            read_settings,
            batch_rows,
            output_batch_rows,
            minimum_batch,
            lineage,
            emit_order: false,
        };
        Ok(Self {
            merge_config,
            input_blocks,
            spiller: SortSpillerImpl::new(ctx)?,
            fan_in,
            force_initial,
            expected_rows: task.total_rows,
        })
    }
}

impl ReclusterMergeFactory {
    fn into_groups(self, groups: usize) -> Result<Vec<Self>> {
        if groups == 0 || groups > self.input_blocks.len() {
            return Err(ErrorCode::BadArguments(
                "invalid recluster merge group count",
            ));
        }
        if groups == 1 {
            return Ok(vec![self]);
        }
        let mut partitions = vec![Vec::new(); groups];
        let mut group_rows = vec![0usize; groups];
        let mut inputs = self.input_blocks;
        inputs.sort_by_key(|input| Reverse(input.meta.row_count));
        for input in inputs {
            let index = group_rows
                .iter()
                .enumerate()
                .min_by_key(|(_, rows)| **rows)
                .unwrap()
                .0;
            group_rows[index] += input.meta.row_count as usize;
            partitions[index].push(input);
        }
        let mut factories = Vec::with_capacity(groups);
        for (input_blocks, rows) in partitions.into_iter().zip(group_rows) {
            let mut config = self.merge_config.clone();
            config.emit_order = true;
            let fan_in = self.fan_in.min(input_blocks.len().max(2));
            factories.push(Self {
                merge_config: config,
                input_blocks,
                spiller: self.spiller.clone(),
                fan_in,
                force_initial: self.force_initial,
                expected_rows: rows,
            });
        }
        log::info!("recluster two-level merge: groups={groups}");
        Ok(factories)
    }
}

impl SyncSource for HorizontalReclusterSource {
    const NAME: &'static str = "HorizontalMultiwayReclusterSource";
    // Input reads already account for scan progress; spill/replayed output must not count again.
    const RECORD_SCAN_PROGRESS: bool = false;

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.merge_execution.finished() {
            return Ok(None);
        }
        // A spill/recovery step can make progress without producing rows. Yield an empty block,
        // which SyncSourcer skips, rather than interpreting that step as EOF or busy-looping here.
        Ok(Some(
            self.merge_execution
                .step()?
                .unwrap_or_else(DataBlock::empty),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::sync::Mutex;
    use std::time::Instant;

    use databend_common_base::runtime::MemStat;
    use databend_common_base::runtime::ThreadTracker;
    use databend_common_catalog::plan::ReclusterTaskKind;
    use databend_common_catalog::table_context::TableContextQueryIdentity;
    use databend_common_catalog::table_context::TableContextTableAccess;
    use databend_common_expression::BlockMetaInfoDowncast;
    use databend_common_expression::BlockThresholds;
    use databend_common_expression::DataField;
    use databend_common_expression::ScalarRef;
    use databend_common_expression::SortColumnDescription;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::block_debug::pretty_format_blocks;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberDataType;
    use databend_common_pipeline_transforms::AccumulatingTransform;
    use databend_common_pipeline_transforms::BlockCompactMeta;
    use databend_common_pipeline_transforms::BlockMetaTransform;
    use databend_common_pipeline_transforms::OrderedBlockCompactBuilder;
    use databend_common_pipeline_transforms::TransformCompactBlock;
    use databend_common_pipeline_transforms::sorts::core::SimpleRowsAsc;
    use databend_common_pipeline_transforms::sorts::core::VariableRows;
    use databend_common_storage::ColumnNodes;
    use databend_common_storage::DataOperator;
    use databend_common_storages_fuse::io::SegmentsIO;
    use databend_common_storages_fuse::io::WriteSettings;
    use databend_common_storages_fuse::io::serialize_block;
    use databend_common_storages_fuse::operations::MutationLogEntry;
    use databend_storages_common_index::MergeSourceDirectory;
    use databend_storages_common_table_meta::meta::Compression;
    use databend_storages_common_table_meta::meta::ExtendedBlockMeta;
    use databend_storages_common_table_meta::meta::SegmentInfo;

    use super::*;
    use crate::test_kits::TestFixture;

    async fn check_external_merge<R: Rows + 'static>(
        force_initial: bool,
        lineage: bool,
        fan_in: usize,
        spill_enabled: bool,
        groups: usize,
    ) -> anyhow::Result<()>
    where
        R::Converter: Send + 'static,
    {
        let fixture = TestFixture::setup().await?;
        fixture.create_default_database().await?;
        fixture.create_default_table().await?;
        let ctx = fixture.new_query_ctx().await?;
        ctx.set_enable_sort_spill(spill_enabled);
        let table_ref = fixture.latest_default_table().await?;
        let table = FuseTable::try_from_table(table_ref.as_ref())?.clone();
        let schema = Arc::new(TableSchema::new(vec![
            TableField::new("k", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("payload", TableDataType::Number(NumberDataType::Int32)),
        ]));
        let mut input_blocks = Vec::new();
        let mut original = Vec::new();
        for ordinal in 0..9 {
            // Duplicate keys deliberately do not imply any cross-source stability.
            let block = DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![0, 0, 1, 1, 2]),
                Int32Type::from_data((0..5).map(|row| ordinal * 100 + row).collect::<Vec<_>>()),
            ]);
            let (col_metas, bytes) =
                serialize_block(&WriteSettings::default(), &schema, block.clone())?;
            let path = format!("external-merge-{}-{ordinal}.parquet", ctx.get_id());
            let size = bytes.len();
            table.get_operator().write(&path, bytes).await?;
            let meta = BlockMeta::new(
                5,
                block.memory_size() as _,
                size as _,
                Default::default(),
                col_metas,
                None,
                (path, DataBlock::VERSION),
                None,
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Compression::Zstd,
                None,
            );
            input_blocks.push(ReclusterMergeInputBlock {
                meta: Arc::new(meta),
                ordinal: ordinal as _,
            });
            original.push(block);
        }
        let mut fields = vec![
            DataField::new("k", DataType::Number(NumberDataType::Int32)),
            DataField::new("payload", DataType::Number(NumberDataType::Int32)),
        ];
        if lineage {
            fields.push(DataField::new(
                "origin",
                DataType::Number(NumberDataType::UInt32),
            ));
            fields.push(DataField::new(
                "row",
                DataType::Number(NumberDataType::UInt32),
            ));
        }
        let merge_config = ReclusterMergeConfig {
            ctx: ctx.clone(),
            abort_checker: ctx.clone().get_abort_checker(),
            table,
            schema: schema.clone(),
            defaults: vec![Scalar::Number(0i32.into()); 2],
            eval: CompoundBlockOperator::new(Vec::new(), Default::default(), 2),
            keys: SortKeyDescription::new(
                vec![SortColumnDescription {
                    offset: 0,
                    asc: true,
                    nulls_first: false,
                }]
                .into(),
                DataSchemaRefExt::create(fields),
                false,
            )?,
            read_settings: ReadSettings {
                max_gap_size: 48,
                max_range_size: 512 * 1024,
                parquet_fast_read_bytes: 0,
            },
            batch_rows: 2,
            output_batch_rows: 2,
            minimum_batch: fan_in != 16,
            lineage,
            emit_order: false,
        };
        let spiller = SortSpillerImpl::new(ctx.clone())?;
        let mut probe = ReclusterMergeStream::<R>::new(
            ReclusterMergeInput::Spill(VecDeque::new()),
            &merge_config,
            &spiller,
        )?;
        let unsorted = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![2, 1]),
            Int32Type::from_data(vec![0, 1]),
        ]);
        let keys = probe.converter.convert(&unsorted)?.to_column();
        assert!(probe.validate_batch(unsorted, keys).is_err());
        // Check ordering across batches, including restored spill chunks.
        let high = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![2]),
            Int32Type::from_data(vec![0]),
        ]);
        let keys = probe.converter.convert(&high)?.to_column();
        probe.validate_batch(high, keys)?;
        let low = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1]),
            Int32Type::from_data(vec![0]),
        ]);
        let keys = probe.converter.convert(&low)?.to_column();
        assert!(probe.validate_batch(low, keys).is_err());

        // Read failure is propagated, never treated as EOF or silently rebuilt.
        let missing = ReclusterMergeInputBlock {
            meta: Arc::new(BlockMeta {
                location: ("missing-recluster-input.parquet".into(), DataBlock::VERSION),
                ..input_blocks[0].meta.as_ref().clone()
            }),
            ordinal: 0,
        };
        let mut missing_stream = ReclusterMergeStream::<R>::new(
            ReclusterMergeInput::Original {
                input_block: missing,
                reader: None,
                position: 0,
            },
            &merge_config,
            &spiller,
        )?;
        assert!(missing_stream.next().is_err());

        // Missing spill chunks must also propagate as an execution error.
        let mut spilled = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1]),
            Int32Type::from_data(vec![0]),
        ]);
        let keys = probe.converter.convert(&spilled)?.to_column();
        spilled.add_column(keys);
        let path = spiller.spill(spilled)?;
        let operator = DataOperator::instance().spill_operator();
        operator.delete(&path).await?;
        let mut missing_spill = ReclusterMergeStream::<R>::new(
            ReclusterMergeInput::Spill(VecDeque::from([path])),
            &merge_config,
            &spiller,
        )?;
        assert!(missing_spill.next().is_err());
        ctx.unload_spill_meta();

        let mut merge_factory = ReclusterMergeFactory {
            merge_config,
            input_blocks,
            spiller,
            fan_in,
            force_initial,
            expected_rows: 45,
        };
        let mut merged = Vec::new();
        if groups == 1 {
            let mut merge_execution = merge_factory.visit_type::<R>()?;
            let mut work_units = 0;
            while !merge_execution.finished() {
                if let Some(block) = merge_execution.step()? {
                    merged.push(block);
                }
                work_units += 1;
                assert!(work_units < 1000, "external merge must terminate");
            }
            ctx.kill(ErrorCode::aborting());
            assert!(
                merge_execution.step().is_err(),
                "merge work must honour cancellation"
            );
        } else {
            use crate::pipelines::PipelineBuildResult;
            use crate::pipelines::executor::ExecutorSettings;
            use crate::pipelines::executor::PipelinePullingExecutor;
            let keys = merge_factory.merge_config.keys.clone();
            let group_factories = merge_factory.into_groups(groups)?;
            assert_eq!(
                group_factories
                    .iter()
                    .map(|group| group.expected_rows)
                    .sum::<usize>(),
                45
            );
            let mut ordinals = Vec::new();
            for group in &group_factories {
                for input in &group.input_blocks {
                    ordinals.push(input.ordinal);
                }
            }
            ordinals.sort();
            assert_eq!(ordinals, (0..9).collect::<Vec<u32>>());
            let mut pipeline = Pipeline::create();
            let mut items = Vec::new();
            for mut factory in group_factories {
                let output = OutputPort::create();
                let source = HorizontalReclusterSource {
                    merge_execution: factory.visit_type::<R>()?,
                };
                items.push(PipeItem::create(
                    SyncSourcer::create(ctx.get_scan_progress(), output.clone(), source)?,
                    vec![],
                    vec![output],
                ));
            }
            pipeline.add_pipe(Pipe::create(0, groups, items));
            try_add_multi_sort_merge_with_flush_before_refill(
                &mut pipeline,
                keys,
                3,
                None,
                true,
                true,
                false,
            )?;
            pipeline.set_max_threads(1);
            let mut result = PipelineBuildResult::create();
            result.main_pipeline = pipeline;
            let mut executor_settings = ExecutorSettings::try_create(ctx.clone())?;
            executor_settings.max_threads = 1;
            let mut executor = PipelinePullingExecutor::from_pipelines(result, executor_settings)?;
            executor.start();
            while let Some(block) = executor.pull_data().await? {
                merged.push(block);
            }
        }
        assert_eq!(
            !ctx.get_spilled_files().is_empty(),
            force_initial || fan_in < 9usize.div_ceil(groups)
        );
        assert_eq!(ctx.get_scan_progress().get_values().rows, 45);
        let combined = DataBlock::concat(&merged)?;
        let keys = combined.get_by_offset(0).to_column();
        for row in 1..45 {
            assert!(keys.index(row - 1).unwrap() <= keys.index(row).unwrap());
        }
        let payload = combined.get_by_offset(1).to_column();
        let mut actual = (0..45)
            .map(|row| payload.index(row).unwrap().to_owned())
            .collect::<Vec<_>>();
        actual.sort();
        let mut expected = original
            .iter()
            .flat_map(|block| {
                let payload = block.get_by_offset(1).to_column();
                (0..5).map(move |row| payload.index(row).unwrap().to_owned())
            })
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(actual, expected);
        if lineage {
            // Exercise the real compact concat/split boundary, not just source batches.
            let mut compact = OrderedBlockCompactBuilder::new(
                BlockThresholds::new(7, 5, 1024 * 1024, 1024 * 1024),
                2,
            );
            let mut compact_outputs = Vec::new();
            for batch in merged {
                compact_outputs.extend(compact.transform(batch)?);
            }
            compact_outputs.extend(compact.on_finish(true)?);
            let mut output_rows = 0;
            for mut wrapped in compact_outputs {
                let meta = BlockCompactMeta::downcast_from(wrapped.take_meta().unwrap()).unwrap();
                for block in TransformCompactBlock.transform(meta)? {
                    let (block, origins) = extract_recluster_lineage(block)?;
                    assert_eq!(block.num_columns(), 2);
                    let mut row = 0;
                    for origin in origins {
                        for source_row in origin.rows {
                            for field in 0..2 {
                                assert_eq!(
                                    block.get_by_offset(field).to_column().index(row),
                                    original[origin.source_ordinal as usize]
                                        .get_by_offset(field)
                                        .to_column()
                                        .index(source_row as usize)
                                );
                            }
                            row += 1;
                        }
                    }
                    assert_eq!(row, block.num_rows());
                    output_rows += row;
                }
            }
            assert_eq!(output_rows, 45);
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_horizontal_merge_sql_setting_and_defaults() -> anyhow::Result<()> {
        use futures::TryStreamExt;

        use crate::test_kits::execute_command;
        use crate::test_kits::execute_query;
        let fixture = TestFixture::setup().await?;
        fixture.create_default_database().await?;
        for (sql, enabled, force) in [
            (
                "create table default.multiway_sql(k int, payload string) cluster by(k) row_per_block=100 block_per_segment=2",
                true,
                false,
            ),
            (
                "insert into default.multiway_sql values (1, 'a'), (3, 'c'), (NULL, 'null-a')",
                true,
                false,
            ),
            (
                "insert into default.multiway_sql values (2, 'b'), (3, 'duplicate'), (NULL, 'null-b')",
                true,
                false,
            ),
            (
                "alter table default.multiway_sql recluster final",
                true,
                true,
            ),
            (
                "alter table default.multiway_sql add column extra string default 'default-value'",
                true,
                false,
            ),
            (
                "alter table default.multiway_sql cluster by(-k)",
                true,
                false,
            ),
            (
                "insert into default.multiway_sql(k, payload) values (0, 'zero'), (4, 'four')",
                true,
                false,
            ),
            (
                "alter table default.multiway_sql recluster final",
                true,
                false,
            ),
            (
                "insert into default.multiway_sql(k, payload) values (5, 'five')",
                false,
                false,
            ),
            (
                "alter table default.multiway_sql recluster final",
                false,
                false,
            ),
        ] {
            let ctx = fixture.new_query_ctx().await?;
            let settings = ctx.get_settings();
            settings.set_setting(
                "enable_recluster_multiway_merge".into(),
                u8::from(enabled).to_string(),
            )?;
            settings.set_setting("force_sort_data_spill".into(), u8::from(force).to_string())?;
            settings.set_setting("max_block_size".into(), "2".into())?;
            ctx.set_enable_sort_spill(true);
            execute_command(ctx.clone(), sql).await?;
        }
        // FINAL clears spill metadata; direct merge tests assert forced spill.
        let blocks = execute_query(fixture.new_query_ctx().await?,
            "select count(), count(k), sum(k), count_if(extra = 'default-value') from default.multiway_sql")
            .await?.try_collect::<Vec<_>>().await?;
        let block = DataBlock::concat(&blocks)?;
        for (field, value) in [9u64, 7, 18, 9].into_iter().enumerate() {
            let actual = block.get_by_offset(field).to_column();
            assert_eq!(actual.index(0).unwrap().to_string(), value.to_string());
        }
        let table = fixture
            .new_query_ctx()
            .await?
            .get_table("default", "default", "multiway_sql")
            .await?;
        let snapshot = FuseTable::try_from_table(table.as_ref())?
            .read_table_snapshot()
            .await?
            .unwrap();
        assert_eq!(snapshot.summary.row_count, 9);

        // Stream origin fields must survive incremental reading and reordering.
        for sql in [
            "create table default.multiway_stream(k int, payload string) cluster by(k) row_per_block=100 change_tracking=true",
            "insert into default.multiway_stream values (3, 'c'), (1, 'a')",
            "insert into default.multiway_stream values (4, 'd'), (2, 'b')",
            "alter table default.multiway_stream recluster final",
        ] {
            let ctx = fixture.new_query_ctx().await?;
            let settings = ctx.get_settings();
            settings.set_setting("enable_recluster_multiway_merge".into(), "1".into())?;
            settings.set_setting("max_block_size".into(), "1".into())?;
            execute_command(ctx, sql).await?;
        }
        let blocks = execute_query(fixture.new_query_ctx().await?,
            "select count(), count(distinct (_origin_block_id, _origin_block_row_num)), count(_origin_version) from default.multiway_stream")
            .await?.try_collect::<Vec<_>>().await?;
        let block = DataBlock::concat(&blocks)?;
        for field in 0..3 {
            assert_eq!(
                block
                    .get_by_offset(field)
                    .to_column()
                    .index(0)
                    .unwrap()
                    .to_owned(),
                Scalar::Number(4u64.into())
            );
        }
        // Composite keys use the existing encoded-row ordering, including NULL,
        // floating-point values and Decimal. Nested payloads must remain intact.
        for sql in [
            "create table default.multiway_nested(k double, d decimal(18,2), payload array(int)) cluster by(k,d) row_per_block=100",
            "insert into default.multiway_nested values (NULL,1.25,[1,2]), (-0.0,2.50,[3]), (1.5,3.75,[])",
            "insert into default.multiway_nested values (0.0,2.50,[4,5]), (NULL,1.25,[6]), (-1.5,4.00,[7])",
            "alter table default.multiway_nested recluster final",
        ] {
            let ctx = fixture.new_query_ctx().await?;
            let settings = ctx.get_settings();
            settings.set_setting("enable_recluster_multiway_merge".into(), "1".into())?;
            settings.set_setting("max_block_size".into(), "1".into())?;
            execute_command(ctx, sql).await?;
        }
        for (sql, expected) in [(
            "select count(), sum(array_sum(payload)) from default.multiway_nested",
            [6u64, 28],
        )] {
            let blocks = execute_query(fixture.new_query_ctx().await?, sql)
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            let block = DataBlock::concat(&blocks)?;
            for (field, value) in expected.into_iter().enumerate() {
                assert_eq!(
                    block
                        .get_by_offset(field)
                        .to_column()
                        .index(0)
                        .unwrap()
                        .to_string(),
                    value.to_string()
                );
            }
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_horizontal_merge_read_failure_keeps_snapshot() -> anyhow::Result<()> {
        use futures::TryStreamExt;

        use crate::test_kits::execute_command;
        use crate::test_kits::execute_query;

        let fixture = TestFixture::setup().await?;
        for sql in [
            "create table default.multiway_failure(k int, payload string) cluster by(k) row_per_block=100",
            "insert into default.multiway_failure values (1,'a'),(3,'c')",
            "insert into default.multiway_failure values (2,'b'),(4,'d')",
        ] {
            execute_command(fixture.new_query_ctx().await?, sql).await?;
        }
        let ctx = fixture.new_query_ctx().await?;
        let table = ctx
            .get_table("default", "default", "multiway_failure")
            .await?;
        let fuse = FuseTable::try_from_table(table.as_ref())?;
        let before = fuse.read_table_snapshot().await?.unwrap();
        let blocks = execute_query(
            fixture.new_query_ctx().await?,
            "select block_location from fuse_block('default','multiway_failure') limit 1",
        )
        .await?
        .try_collect::<Vec<_>>()
        .await?;
        let location = blocks[0].get_by_offset(0).to_column();
        let ScalarRef::String(location) = location.index(0).unwrap() else {
            panic!("expected source block path");
        };
        let operator = fuse.get_operator();
        let contents = operator.read(location).await?;
        operator
            .write(location, "corrupted recluster test input")
            .await?;
        let settings = ctx.get_settings();
        settings.set_setting("enable_recluster_multiway_merge".into(), "1".into())?;
        let result = execute_command(
            ctx.clone(),
            "alter table default.multiway_failure recluster final",
        )
        .await;
        // Restore the fixture file before asserting the expected failure.
        operator.write(location, contents).await?;
        assert!(
            result.is_err(),
            "corrupted input cannot successfully commit"
        );
        let ctx = fixture.new_query_ctx().await?;
        let table = ctx
            .get_table("default", "default", "multiway_failure")
            .await?;
        let after = FuseTable::try_from_table(table.as_ref())?
            .read_table_snapshot()
            .await?
            .unwrap();
        assert_eq!(before.snapshot_id, after.snapshot_id);
        let blocks = execute_query(ctx, "select count(), sum(k) from default.multiway_failure")
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let block = DataBlock::concat(&blocks)?;
        for (field, expected) in [4, 10].into_iter().enumerate() {
            let column = block.get_by_offset(field).to_column();
            assert_eq!(column.index(0).unwrap().to_string(), expected.to_string());
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recluster_inverted_index_merge_search_and_failure() -> anyhow::Result<()> {
        use futures::TryStreamExt;

        use crate::test_kits::execute_command;
        use crate::test_kits::execute_query;

        let fixture = TestFixture::setup().await?;
        for sql in [
            "create table default.index_merge(k int, content string, inverted index text_idx(content)) cluster by(k) row_per_block=100 block_per_segment=2",
            "insert into default.index_merge values (3,'third alpha'),(1,'first alpha')",
            "insert into default.index_merge values (4,'fourth beta'),(2,'second beta')",
        ] {
            execute_command(fixture.new_query_ctx().await?, sql).await?;
        }
        // Check compatibility independently of planner task selection.
        let admission_ctx = fixture.new_query_ctx().await?;
        let table = admission_ctx
            .get_table("default", "default", "index_merge")
            .await?;
        let fuse = FuseTable::try_from_table(table.as_ref())?;
        let snapshot = fuse.read_table_snapshot().await?.unwrap();
        let segments =
            SegmentsIO::create(admission_ctx.clone(), fuse.get_operator(), fuse.schema())
                .read_segments::<SegmentInfo>(&snapshot.segments, true)
                .await?;
        let mut blocks = Vec::new();
        for segment in segments {
            for block in segment?.blocks {
                blocks.push((None, block));
            }
        }
        let arrow_schema = arrow_schema::Schema::from(fuse.schema().as_ref());
        let nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&fuse.schema()));
        let (stats, parts) =
            FuseTable::to_partitions(Some(&fuse.schema()), &blocks, &nodes, None, None);
        let mut task = ReclusterTask {
            parts,
            stats,
            total_rows: 4,
            total_bytes: 1024,
            total_compressed: 512,
            level: 0,
            input_level_stats: vec![],
            kind: ReclusterTaskKind::MergeBlocks,
            vertical_kind: None,
            memory_budget: 0,
            virtual_column_layout: None,
            inverted_index_sources: blocks
                .iter()
                .map(|(_, block)| block.inverted_index_metas.clone().unwrap_or_default())
                .collect(),
        };
        use databend_common_storages_fuse::operations::ReclusterIndexMergeInputs;
        assert!(ReclusterIndexMergeInputs::try_create(fuse, &task)?.is_some());
        // Interleaved source rows, with output serialization completing in reverse order.
        use databend_common_storages_fuse::operations::MutationLogs;
        use databend_common_storages_fuse::operations::ReclusterIndexOutput;
        use databend_common_storages_fuse::operations::ReclusterIndexRowRange;
        use databend_common_storages_fuse::operations::TransformReclusterIndexMerge;
        let inputs = ReclusterIndexMergeInputs::try_create(fuse, &task)?.unwrap();
        let mut collector =
            TransformReclusterIndexMerge::new(admission_ctx.clone(), fuse.clone(), inputs);
        for source_row in (0..2).rev() {
            let mut meta = blocks[0].1.as_ref().clone();
            meta.row_count = 2;
            meta.inverted_index_metas = Some(vec![]);
            meta.inverted_index_size = None;
            let output = ExtendedBlockMeta {
                block_meta: meta,
                draft_virtual_block_meta: None,
                column_hlls: None,
                column_top_n: None,
            };
            let rows = (0..2)
                .map(|source| ReclusterIndexRowRange {
                    source,
                    rows: source_row..source_row + 1,
                })
                .collect();
            assert!(
                collector
                    .transform(DataBlock::empty_with_meta(Box::new(ReclusterIndexOutput {
                        meta: output,
                        rows,
                        output_row: u64::from(source_row) * 2,
                    })))?
                    .is_empty()
            );
        }
        let outputs = collector.on_finish(true)?;
        assert_eq!(outputs.len(), 2);
        for mut output in outputs {
            let logs = MutationLogs::downcast_from(output.take_meta().unwrap()).unwrap();
            let MutationLogEntry::AppendBlock { block_meta, .. } = &logs.entries[0] else {
                panic!("expected append block");
            };
            let meta = block_meta
                .block_meta
                .inverted_index_meta("text_idx")
                .unwrap();
            let directory = MergeSourceDirectory::open(
                fuse.get_operator(),
                meta.location.0.clone(),
                meta.size,
            )?;
            let index = directory.open_index()?;
            let searcher = index.reader()?.searcher();
            assert_eq!(searcher.num_docs(), 2);
            assert!(block_meta.block_meta.inverted_index_size.unwrap() >= meta.size);
        }
        let inputs = ReclusterIndexMergeInputs::try_create(fuse, &task)?.unwrap();
        let mut cancelled =
            TransformReclusterIndexMerge::new(admission_ctx.clone(), fuse.clone(), inputs.clone());
        assert!(cancelled.on_finish(false)?.is_empty());
        let cancelled_ctx = fixture.new_query_ctx().await?;
        let mut cancelled =
            TransformReclusterIndexMerge::new(cancelled_ctx.clone(), fuse.clone(), inputs);
        cancelled_ctx.kill(ErrorCode::aborting());
        assert!(cancelled.on_finish(true).is_err());
        let source_metas = task.inverted_index_sources.clone();
        task.inverted_index_sources[0].clear();
        assert!(ReclusterIndexMergeInputs::try_create(fuse, &task)?.is_none());
        task.inverted_index_sources = source_metas;
        task.inverted_index_sources[0][0].index_version = "old-definition".into();
        assert!(ReclusterIndexMergeInputs::try_create(fuse, &task)?.is_none());

        let query = "select k,content from default.index_merge where match(content,'alpha OR beta') order by k,content";
        let before = execute_query(fixture.new_query_ctx().await?, query)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let expected = pretty_format_blocks(&before)?.to_string();
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();
        settings.set_setting("enable_recluster_multiway_merge".into(), "1".into())?;
        settings.set_setting("enable_recluster_inverted_index_merge".into(), "1".into())?;
        execute_command(ctx, "alter table default.index_merge recluster final").await?;
        let after = execute_query(fixture.new_query_ctx().await?, query)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(pretty_format_blocks(&after)?.to_string(), expected);

        // Fresh same-level overlapping inputs guarantee selection of the bad bundle.
        for sql in [
            "create table default.index_failure(k int, content string, inverted index text_idx(content)) cluster by(k) row_per_block=100 block_per_segment=2",
            "insert into default.index_failure values (3,'third alpha'),(1,'first alpha')",
            "insert into default.index_failure values (4,'fourth beta'),(2,'second beta')",
        ] {
            execute_command(fixture.new_query_ctx().await?, sql).await?;
        }
        let ctx = fixture.new_query_ctx().await?;
        let table = ctx.get_table("default", "default", "index_failure").await?;
        let fuse = FuseTable::try_from_table(table.as_ref())?;
        let snapshot = fuse.read_table_snapshot().await?.unwrap();
        let segments = SegmentsIO::create(ctx.clone(), fuse.get_operator(), fuse.schema())
            .read_segments::<SegmentInfo>(&snapshot.segments, true)
            .await?;
        let mut source_index = None;
        for segment in segments {
            let segment = segment?;
            for block in &segment.blocks {
                if let Some(meta) = block.inverted_index_meta("text_idx") {
                    source_index = Some(meta.location.0.clone());
                    break;
                }
            }
        }
        let location = source_index.expect("source inverted index must exist");
        let operator = fuse.get_operator();
        let contents = operator.read(&location).await?;
        operator
            .write(&location, "corrupt source inverted index")
            .await?;
        let settings = ctx.get_settings();
        settings.set_setting("enable_recluster_multiway_merge".into(), "1".into())?;
        settings.set_setting("enable_recluster_inverted_index_merge".into(), "1".into())?;
        let result =
            execute_command(ctx, "alter table default.index_failure recluster final").await;
        operator.write(&location, contents).await?;
        assert!(
            result.is_err(),
            "selected source index errors cannot silently rebuild"
        );
        let ctx = fixture.new_query_ctx().await?;
        let table = ctx.get_table("default", "default", "index_failure").await?;
        let after = FuseTable::try_from_table(table.as_ref())?
            .read_table_snapshot()
            .await?
            .unwrap();
        assert_eq!(snapshot.snapshot_id, after.snapshot_id);

        // Verify doc-to-row mapping across spill and computed/duplicate keys.
        for sql in [
            "create table default.index_multi(k int, content string, inverted index text_idx(content)) cluster by(-k) row_per_block=100 block_per_segment=2",
            "insert into default.index_multi select number, concat('word',to_string(number),' alpha') from numbers(200)",
            "insert into default.index_multi select number, concat('word',to_string(number),' beta') from numbers(200)",
        ] {
            execute_command(fixture.new_query_ctx().await?, sql).await?;
        }
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();
        settings.set_setting("enable_recluster_multiway_merge".into(), "1".into())?;
        settings.set_setting("enable_recluster_inverted_index_merge".into(), "1".into())?;
        settings.set_setting("force_sort_data_spill".into(), "1".into())?;
        ctx.set_enable_sort_spill(true);
        execute_command(ctx, "alter table default.index_multi recluster final").await?;
        for (sql, expected) in [
            (
                "select count() from default.index_multi where match(content,'word123') and k=123",
                2,
            ),
            (
                "select count() from default.index_multi where match(content,'alpha')",
                200,
            ),
            (
                "select count() from default.index_multi where match(content,'beta')",
                200,
            ),
            (
                "select count() from default.index_multi where match(content,'missingword')",
                0,
            ),
            (
                "select count() from default.index_multi where match(content,'\"word123 alpha\"')",
                1,
            ),
        ] {
            let blocks = execute_query(fixture.new_query_ctx().await?, sql)
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            let block = DataBlock::concat(&blocks)?;
            assert_eq!(
                block
                    .get_by_offset(0)
                    .to_column()
                    .index(0)
                    .unwrap()
                    .to_string(),
                expected.to_string()
            );
        }
        // Reuse the existing index and rebuild the newly added definition.
        for sql in [
            "create table default.index_partial(k int, content string, other string, inverted index existing_idx(content)) cluster by(k) row_per_block=100 block_per_segment=2",
            "insert into default.index_partial values (3,'alpha','delta'),(1,'alpha','gamma')",
            "insert into default.index_partial values (4,'beta','delta'),(2,'beta','gamma')",
            "create inverted index late_idx on default.index_partial(other)",
        ] {
            execute_command(fixture.new_query_ctx().await?, sql).await?;
        }
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();
        settings.set_setting("enable_recluster_multiway_merge".into(), "1".into())?;
        settings.set_setting("enable_recluster_inverted_index_merge".into(), "1".into())?;
        execute_command(ctx, "alter table default.index_partial recluster final").await?;
        for sql in [
            "select count() from default.index_partial where match(content,'alpha')",
            "select count() from default.index_partial where match(other,'gamma')",
        ] {
            let blocks = execute_query(fixture.new_query_ctx().await?, sql)
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            let block = DataBlock::concat(&blocks)?;
            assert_eq!(
                block
                    .get_by_offset(0)
                    .to_column()
                    .index(0)
                    .unwrap()
                    .to_string(),
                "2"
            );
        }
        Ok(())
    }

    /// Local performance probe, not a CI timing assertion. Run with --ignored
    /// --nocapture to compare identical ordered inputs with the feature off/on.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "recluster performance probe"]
    async fn benchmark_horizontal_merge_wide_rows() -> anyhow::Result<()> {
        use futures::TryStreamExt;

        use crate::test_kits::execute_command;
        use crate::test_kits::execute_query;
        let fixture = TestFixture::setup().await?;
        let mut reference_result = None;
        let rows_per_source = env::var("RECLUSTER_BENCH_ROWS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(4096);
        let payload_repeat = env::var("RECLUSTER_BENCH_PAYLOAD_REPEAT")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(256);
        for enabled in [false, true] {
            let table = if enabled { "merge_new" } else { "merge_old" };
            let create = format!(
                "create table default.{table}(k bigint, payload string) cluster by(k) row_per_block=1000000 block_per_segment=2"
            );
            execute_command(fixture.new_query_ctx().await?, &create).await?;
            for source in 0..16 {
                let insert = format!(
                    "insert into default.{table} select number * 16 + {source}, repeat(to_string(number), {payload_repeat}) from numbers({rows_per_source})"
                );
                execute_command(fixture.new_query_ctx().await?, &insert).await?;
            }
            let ctx = fixture.new_query_ctx().await?;
            let settings = ctx.get_settings();
            settings.set_setting(
                "enable_recluster_multiway_merge".into(),
                u8::from(enabled).to_string(),
            )?;
            settings.set_setting("max_threads".into(), "4".into())?;
            let memory = MemStat::create("recluster benchmark".into());
            ctx.set_query_memory_tracking(Some(memory.clone()));
            let mut tracking = ThreadTracker::new_tracking_payload();
            tracking.mem_stat = Some(memory.clone());
            let sql = format!("alter table default.{table} recluster final");
            let start = Instant::now();
            let execution = execute_command(ctx.clone(), &sql);
            ThreadTracker::tracking_future_with_payload(execution, Some(Arc::new(tracking)))
                .await?;
            let elapsed = start.elapsed();
            let peak = memory.get_peak_memory_usage();
            let blocks = execute_query(
                fixture.new_query_ctx().await?,
                &format!("select count(), sum(k), sum(length(payload)) from default.{table}"),
            )
            .await?
            .try_collect::<Vec<_>>()
            .await?;
            let result = pretty_format_blocks(&blocks)?.to_string();
            if let Some(reference) = &reference_result {
                assert_eq!(&result, reference);
            } else {
                reference_result = Some(result.clone());
            }
            println!(
                "multiway={enabled}, elapsed={elapsed:?}, query_peak={peak:?}, result={result}"
            );
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_second_stage_merges_heads_without_local_memory_threshold() -> anyhow::Result<()> {
        use databend_common_pipeline::sources::BlocksSource;

        use crate::pipelines::PipelineBuildResult;
        use crate::pipelines::executor::ExecutorSettings;
        use crate::pipelines::executor::PipelinePullingExecutor;

        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let mut pipeline = Pipeline::create();
        let block = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1, 2, 3])]);
        pipeline.add_source(
            |output| {
                BlocksSource::create(
                    ctx.get_scan_progress(),
                    output,
                    Arc::new(Mutex::new(VecDeque::from([block.clone()]))),
                )
            },
            2,
        )?;
        let keys = SortKeyDescription::new(
            vec![SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
            }]
            .into(),
            DataSchemaRefExt::create(vec![DataField::new(
                "key",
                DataType::Number(NumberDataType::Int32),
            )]),
            false,
        )?;
        try_add_multi_sort_merge_with_flush_before_refill(
            &mut pipeline,
            keys,
            3,
            None,
            true,
            true,
            false,
        )?;
        pipeline.set_max_threads(1);
        let mut result = PipelineBuildResult::create();
        result.main_pipeline = pipeline;
        let mut executor_settings = ExecutorSettings::try_create(ctx)?;
        executor_settings.max_threads = 1;
        let mut executor = PipelinePullingExecutor::from_pipelines(result, executor_settings)?;
        executor.start();
        let mut output = Vec::new();
        while let Some(block) = executor.pull_data().await? {
            output.push(block);
        }
        let actual = DataBlock::concat(&output)?;
        assert_eq!(
            actual.get_by_offset(0).to_column(),
            Int32Type::from_data(vec![1, 1, 2, 2, 3, 3])
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_two_level_merge_variable_keys_spill_and_original_lineage() -> anyhow::Result<()> {
        check_external_merge::<VariableRows>(true, true, 2, true, 3).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_two_level_merge_simple_keys_without_spill() -> anyhow::Result<()> {
        check_external_merge::<SimpleRowsAsc<Int32Type>>(false, true, 32, false, 3).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_external_merge_without_local_memory_threshold() -> anyhow::Result<()> {
        check_external_merge::<SimpleRowsAsc<Int32Type>>(false, true, 16, true, 1).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_external_merge_without_spill() -> anyhow::Result<()> {
        check_external_merge::<SimpleRowsAsc<Int32Type>>(false, true, 16, false, 1).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_external_merge_rejects_required_spill_when_disabled() {
        let err = check_external_merge::<SimpleRowsAsc<Int32Type>>(false, false, 2, false, 1)
            .await
            .unwrap_err();
        assert_eq!(
            err.downcast_ref::<ErrorCode>().unwrap().code(),
            ErrorCode::MEMORY_EXCEEDS_LIMIT
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_external_merge_multiple_rounds_and_lineage() -> anyhow::Result<()> {
        check_external_merge::<SimpleRowsAsc<Int32Type>>(false, true, 2, true, 1).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_external_merge_forced_spill_variable_rows() -> anyhow::Result<()> {
        check_external_merge::<VariableRows>(true, false, 2, true, 1).await
    }
}
