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

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::BlockMetaOptions;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PrewhereInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_column::bitmap::utils::SlicesIterator;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::Scalar;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_metrics::storage::*;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use roaring::RoaringTreemap;

use super::parquet_data_source::BlockSource;
use super::parquet_data_source::ParquetDataSource;
use super::read_block_context::ReadBlockContext;
use super::read_state::BlockBatches;
use super::read_state::BlockRows;
use super::read_state::BlockScanStats;
use super::read_state::ReadState;
use super::read_state::ReadStateBatch;
use super::read_state::ReadStateOutput;
use super::util::add_data_block_meta;
use super::util::selected_row_offsets;
use crate::fuse_part::FuseBlockPartInfo;
use crate::io::BlockReadResult;
use crate::io::BlockReader;
use crate::io::GranuleDataReader;
use crate::io::RowSelection;
use crate::io::VirtualBlockReadResult;
use crate::io::VirtualColumnReader;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;

/// A block read granule group by granule group, see [`BlockSource::Granule`].
struct ActiveGranuleRead {
    part: PartInfoPtr,
    groups: VecDeque<Vec<std::ops::Range<usize>>>,
    reader: GranuleDataReader,
    virtual_reader: Option<GranuleDataReader>,
}

/// The decoded rows of one granule range.
struct DecodedRange {
    block: DataBlock,
    offsets: Option<RoaringTreemap>,
    start_row: usize,
    /// Scan workload of this range, measured before prewhere and runtime
    /// filters dropped rows.
    scan_stats: BlockScanStats,
}

#[derive(Default)]
struct ExecuteScanDiagnostics {
    normal_parts: usize,
    granule_parts: usize,
    granule_groups: usize,
    granule_ranges: usize,
    projected_columns: usize,
    selected_bytes: u64,
    coalesced_bytes: u64,
    coalesced_requests: usize,
    input_rows: usize,
    output_blocks: usize,
    output_rows: usize,
    output_bytes: usize,
    granule_reader_create: Duration,
    granule_read: Duration,
    decode_filter: Duration,
    finalize_concat: Duration,
    normal_process: Duration,
}

impl ExecuteScanDiagnostics {
    fn has_activity(&self) -> bool {
        self.normal_parts != 0 || self.granule_parts != 0
    }
}

pub struct DeserializeDataTransform {
    ctx: Arc<dyn TableContext>,
    scan_id: usize,
    scan_progress: Arc<Progress>,
    block_reader: Arc<BlockReader>,
    read_block_context: Arc<ReadBlockContext>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: VecDeque<DataBlock>,
    src_schema: DataSchema,
    output_schema: DataSchema,
    parts: Vec<PartInfoPtr>,
    chunks: Vec<BlockSource>,

    virtual_reader: Arc<Option<VirtualColumnReader>>,

    base_block_ids: Option<Scalar>,
    block_meta_options: BlockMetaOptions,

    prewhere_info: Option<PrewhereInfo>,
    read_state: Option<ReadState>,
    /// The block whose batches are being output, see [`ReadState::start_batches`].
    pending: Option<PendingBlock>,
    /// The block whose granule groups are being output.
    active_granule_read: Option<ActiveGranuleRead>,
    diagnostics: ExecuteScanDiagnostics,
}

/// A block decoded in batches: the next batch is decoded only once downstream took the
/// output of the previous one.
struct PendingBlock {
    part: PartInfoPtr,
    batches: BlockBatches,
    /// The scan of the block reported so far. The scan is reported as the batches are
    /// decoded: the processor may be dropped without finishing the block, e.g. when a
    /// nested executor stops pulling.
    reported: BlockScanStats,
    /// Time spent decoding the batches so far.
    elapsed: Duration,
}

unsafe impl Send for DeserializeDataTransform {}

impl DeserializeDataTransform {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        ctx: Arc<dyn TableContext>,
        block_reader: Arc<BlockReader>,
        read_block_context: Arc<ReadBlockContext>,
        plan: &DataSourcePlan,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();

        let src_schema = with_virtual_fields(
            (block_reader.schema().as_ref()).into(),
            virtual_reader.as_ref().as_ref(),
        );

        let mut output_schema = plan.schema().as_ref().clone();
        output_schema.remove_internal_fields();
        let output_schema: DataSchema = (&output_schema).into();

        let prewhere_info = plan
            .push_downs
            .as_ref()
            .and_then(|p| p.prewhere.as_ref())
            .cloned();

        Ok(ProcessorPtr::create(Box::new(DeserializeDataTransform {
            ctx: ctx.clone(),
            scan_id: plan.scan_id,
            scan_progress,
            block_reader,
            read_block_context,
            input,
            output,
            output_data: VecDeque::new(),
            src_schema,
            output_schema,
            parts: vec![],
            chunks: vec![],
            virtual_reader,
            base_block_ids: plan.base_block_ids.clone(),
            block_meta_options: plan.block_meta_options.clone(),
            prewhere_info,
            read_state: None,
            pending: None,
            active_granule_read: None,
            diagnostics: ExecuteScanDiagnostics::default(),
        })))
    }
}

impl DeserializeDataTransform {
    fn ensure_read_state(&mut self) -> Result<()> {
        if self.read_state.is_none() {
            let read_state = ReadState::create(
                self.ctx.clone(),
                self.scan_id,
                self.prewhere_info.as_ref(),
                self.block_reader.clone(),
            )?;
            // The read state leaves out the columns only its early stage reads.
            self.src_schema = with_virtual_fields(
                read_state.output_schema.clone(),
                self.virtual_reader.as_ref().as_ref(),
            );
            self.read_state = Some(read_state);
        }
        Ok(())
    }

    fn no_output_columns(&self) -> bool {
        self.output_schema.fields().is_empty() && self.virtual_reader.is_none()
    }

    /// A block whose rows come from the index matches alone. Use the same row-id and
    /// score mapping as a filtered scan: the index can return rows in score order,
    /// not block-offset order.
    fn process_index_rows(&mut self, part: &PartInfoPtr, offsets: RoaringTreemap) -> Result<()> {
        let part = FuseBlockPartInfo::from_part(part)?;
        let rows = offsets.len() as usize;
        self.record_scan_progress(BlockScanStats { rows, bytes: 0 });
        self.output_data.push_back(add_data_block_meta(
            DataBlock::empty_with_rows(rows),
            part,
            Some(offsets),
            self.base_block_ids.clone(),
            &self.block_meta_options,
            0,
        )?);
        Ok(())
    }

    fn deserialize_parquet(
        &mut self,
        part_info: PartInfoPtr,
        read_res: ParquetDataSource,
    ) -> Result<()> {
        let start = Instant::now();
        self.diagnostics.normal_parts += 1;
        let columns_chunks = read_res.data.columns_chunks()?;
        let part = FuseBlockPartInfo::from_part(&part_info)?;
        self.diagnostics.input_rows += part.nums_rows;

        self.ensure_read_state()?;

        let no_output_columns = self.no_output_columns();
        if self.can_split_block(part)
            && let Some(batches) = self.read_state.as_mut().unwrap().start_batches(
                &columns_chunks,
                part,
                no_output_columns,
                self.block_meta_options.query_internal_columns,
            )?
        {
            // The batches own the column chunks they decode from.
            drop(columns_chunks);
            drop(read_res);
            self.pending = Some(PendingBlock {
                part: part_info,
                batches,
                reported: BlockScanStats::default(),
                elapsed: start.elapsed(),
            });
            return self.process_pending();
        }

        let ReadStateOutput {
            block: mut data_block,
            bitmap_selection,
            mut scan_stats,
        } = self.read_state.as_mut().unwrap().deserialize_and_filter(
            columns_chunks,
            part,
            no_output_columns,
            read_res.condition.as_ref(),
            read_res.io_skipped,
        )?;

        // Virtual columns are read in addition to the columns `ReadState`
        // accounted for, so add them to the scan statistics.
        if let Some(virtual_reader) = self.virtual_reader.as_ref() {
            let bytes_before = data_block.memory_size();
            let virtual_block_location = read_res
                .virtual_data
                .as_ref()
                .map(|data| data.virtual_block_location.clone());
            // `src_schema` describes the block `ReadState` returned, which leaves out the
            // columns only its early stage reads, followed by the virtual columns.
            data_block = virtual_reader
                .deserialize_virtual_columns(
                    data_block,
                    &self.src_schema,
                    read_res.virtual_data,
                    bitmap_selection
                        .as_ref()
                        .map(|bitmap| RowSelection::from(bitmap).selection),
                )
                .inspect_err(|error| {
                    log::warn!(
                        "failed to deserialize virtual columns: block_location={}, virtual_block_location={:?}, error={}",
                        part.location,
                        virtual_block_location,
                        error
                    );
                })?;
            scan_stats.add_decoded_bytes(
                data_block.memory_size().saturating_sub(bytes_before),
                data_block.num_rows(),
            );
        }

        let elapsed = start.elapsed();
        self.diagnostics.normal_process += elapsed;
        self.record_scan(elapsed, scan_stats);

        let data_block = data_block.resort(&self.src_schema, &self.output_schema)?;

        // Fill `BlockMetaIndex` as `DataBlock.meta` if query internal columns,
        // `TransformAddInternalColumns` will generate internal columns using `BlockMetaIndex` in next pipeline.
        let offsets = if self.block_meta_options.query_internal_columns {
            bitmap_selection.as_ref().map(selected_row_offsets)
        } else {
            None
        };

        self.output_data.push_back(add_data_block_meta(
            data_block,
            part,
            offsets,
            self.base_block_ids.clone(),
            &self.block_meta_options,
            0,
        )?);
        Ok(())
    }

    /// Whether the rows of `part` may be output as several blocks: virtual columns are
    /// decoded for the whole block, `_origin_block_row_num` numbers the rows of each
    /// output block from zero, MERGE INTO target builds track whole blocks, and
    /// `add_data_block_meta` remaps whole-block inverted/vector index scores per output block.
    fn can_split_block(&self, part: &FuseBlockPartInfo) -> bool {
        self.virtual_reader.is_none()
            && !self.block_meta_options.update_stream_columns
            && !self.block_meta_options.reserve_block_index
            && !(self.block_meta_options.query_internal_columns
                && part.block_meta_index().is_some_and(|meta| {
                    meta.inverted_index_matches.is_some() || meta.vector_index_matches.is_some()
                }))
    }

    /// Decodes the next batch of the pending block with output rows, queues its output
    /// blocks and reports the scan of the batches decoded.
    fn process_pending(&mut self) -> Result<()> {
        let Some(pending) = self.pending.as_mut() else {
            return Ok(());
        };
        let start = Instant::now();
        let part_info = pending.part.clone();
        let part = FuseBlockPartInfo::from_part(&part_info)?;
        let read_state = self.read_state.as_mut().unwrap();
        let mut done = false;
        let mut batches = vec![];
        while batches.is_empty() {
            match pending.batches.next_batches(read_state, part)? {
                Some(next) => batches = next,
                None => {
                    done = true;
                    break;
                }
            }
        }
        pending.elapsed += start.elapsed();
        let scan_stats = pending.batches.scan_stats();
        let increase = BlockScanStats {
            rows: scan_stats.rows - pending.reported.rows,
            bytes: scan_stats.bytes - pending.reported.bytes,
        };
        pending.reported = scan_stats;
        self.record_scan_progress(increase);
        for ReadStateBatch { block, offsets } in batches {
            let data_block = block.resort(&self.src_schema, &self.output_schema)?;
            self.output_data.push_back(add_data_block_meta(
                data_block,
                part,
                offsets,
                self.base_block_ids.clone(),
                &self.block_meta_options,
                0,
            )?);
        }
        if done {
            self.finish_pending();
        }
        Ok(())
    }

    /// Drops the pending block, if any, and records the time spent decoding it.
    fn finish_pending(&mut self) {
        if let Some(pending) = self.pending.take() {
            self.diagnostics.normal_process += pending.elapsed;
            metrics_inc_remote_io_deserialize_milliseconds(pending.elapsed.as_millis() as u64);
        }
    }

    fn record_scan(&self, elapsed: Duration, scan_stats: BlockScanStats) {
        // Perf.
        {
            metrics_inc_remote_io_deserialize_milliseconds(elapsed.as_millis() as u64);
        }
        self.record_scan_progress(scan_stats);
    }

    fn record_scan_progress(&self, scan_stats: BlockScanStats) {
        // Report the rows and bytes the scan actually processed, not what
        // survived prewhere / runtime filters: scan progress drives
        // `system.processes`, `system_history.query_history` and the
        // `ScanBytes` profile, which measure scan workload rather than
        // filter output.
        let progress_values = ProgressValues {
            rows: scan_stats.rows,
            bytes: scan_stats.bytes,
        };
        self.scan_progress.incr(&progress_values);
        Profile::record_usize_profile(ProfileStatisticsName::ScanBytes, scan_stats.bytes);
    }
}

impl DeserializeDataTransform {
    /// The block offsets of the `num_rows` rows from `start_row` that `bitmap` selects, or
    /// of all of them.
    fn offsets_for_range(
        start_row: usize,
        num_rows: usize,
        bitmap: Option<&Bitmap>,
    ) -> RoaringTreemap {
        match bitmap {
            // Select by runs of set bits, as `selected_row_offsets` does: a TopN
            // query keeps a few rows out of a block of hundreds of thousands.
            Some(bitmap) => RoaringTreemap::from_sorted_iter(SlicesIterator::new(bitmap).flat_map(
                |(start, len)| (start_row + start) as u64..(start_row + start + len) as u64,
            ))
            .unwrap(),
            None => {
                let mut offsets = RoaringTreemap::new();
                offsets.insert_range(start_row as u64..(start_row + num_rows) as u64);
                offsets
            }
        }
    }

    fn decode_granule_range(
        &mut self,
        part: &FuseBlockPartInfo,
        data: BlockReadResult,
        virtual_data: Option<VirtualBlockReadResult>,
    ) -> Result<DecodedRange> {
        let start = Instant::now();
        let result = self.decode_granule_range_inner(part, data, virtual_data);
        self.diagnostics.decode_filter += start.elapsed();
        result
    }

    fn decode_granule_range_inner(
        &mut self,
        part: &FuseBlockPartInfo,
        data: BlockReadResult,
        virtual_data: Option<VirtualBlockReadResult>,
    ) -> Result<DecodedRange> {
        self.ensure_read_state()?;
        let row_range = data
            .row_range()
            .cloned()
            .ok_or_else(|| ErrorCode::Internal("granule read result has no row range"))?;
        let rows = BlockRows {
            start: row_range.start,
            len: row_range.len(),
        };
        self.diagnostics.input_rows += rows.len;
        let no_output_columns = self.no_output_columns();
        let columns_chunks = data.columns_chunks()?;
        let ReadStateOutput {
            mut block,
            bitmap_selection,
            mut scan_stats,
        } = self
            .read_state
            .as_mut()
            .unwrap()
            .deserialize_and_filter_rows(
                columns_chunks,
                part,
                rows,
                no_output_columns,
                None,
                false,
            )?;

        // Virtual columns are read in addition to the columns `ReadState`
        // accounted for, so add them to the scan statistics.
        if let Some(virtual_reader) = self.virtual_reader.as_ref() {
            let bytes_before = block.memory_size();
            let virtual_block_location = virtual_data
                .as_ref()
                .map(|data| data.virtual_block_location.clone());
            block = virtual_reader
                .deserialize_virtual_columns(
                    block,
                    &self.src_schema,
                    virtual_data,
                    bitmap_selection
                        .as_ref()
                        .map(|bitmap| RowSelection::from(bitmap).selection),
                )
                .inspect_err(|error| {
                    log::warn!(
                        "failed to deserialize virtual columns: block_location={}, virtual_block_location={:?}, error={}",
                        part.location,
                        virtual_block_location,
                        error
                    );
                })?;
            scan_stats.add_decoded_bytes(
                block.memory_size().saturating_sub(bytes_before),
                block.num_rows(),
            );
        }

        block = block.resort(&self.src_schema, &self.output_schema)?;
        let offsets = self
            .block_meta_options
            .query_internal_columns
            .then(|| Self::offsets_for_range(rows.start, rows.len, bitmap_selection.as_ref()));

        Ok(DecodedRange {
            block,
            offsets,
            start_row: rows.start,
            scan_stats,
        })
    }

    fn start_granule_read(
        &mut self,
        part: PartInfoPtr,
        groups: Vec<Vec<std::ops::Range<usize>>>,
    ) -> Result<()> {
        let start = Instant::now();
        self.diagnostics.granule_parts += 1;
        self.diagnostics.granule_groups += groups.len();
        self.diagnostics.granule_ranges += groups.iter().map(Vec::len).sum::<usize>();
        let reader = self
            .read_block_context
            .create_granule_data_reader(&part, &groups)?;
        let (columns, selected, coalesced, requests) = reader.read_plan_stats();
        self.diagnostics.projected_columns += columns;
        self.diagnostics.selected_bytes += selected;
        self.diagnostics.coalesced_bytes += coalesced;
        self.diagnostics.coalesced_requests += requests;
        let virtual_reader = self
            .read_block_context
            .create_virtual_granule_reader(&part, &groups)?;
        if let Some(reader) = &virtual_reader {
            let (columns, selected, coalesced, requests) = reader.read_plan_stats();
            self.diagnostics.projected_columns += columns;
            self.diagnostics.selected_bytes += selected;
            self.diagnostics.coalesced_bytes += coalesced;
            self.diagnostics.coalesced_requests += requests;
        }
        self.diagnostics.granule_reader_create += start.elapsed();
        self.active_granule_read = Some(ActiveGranuleRead {
            part,
            groups: groups.into(),
            reader,
            virtual_reader,
        });
        Ok(())
    }

    /// Reads and decodes the next granule group of the active granule read, and queues its
    /// output.
    fn process_granule_group(&mut self) -> Result<()> {
        let mut active = self
            .active_granule_read
            .take()
            .ok_or_else(|| ErrorCode::Internal("missing active granule read"))?;
        let Some(group) = active.groups.pop_front() else {
            return Ok(());
        };
        let start = Instant::now();
        let fuse_part = FuseBlockPartInfo::from_part(&active.part)?;
        let mut decoded_ranges = Vec::with_capacity(group.len());
        for expected_range in group {
            let read_start = Instant::now();
            let range_read = active.reader.read_next()?.ok_or_else(|| {
                ErrorCode::Internal("granule data reader ended before group was complete")
            })?;
            if range_read.range != expected_range {
                return Err(ErrorCode::Internal("granule read ranges are out of sync"));
            }
            let virtual_data = if let Some(reader) = &mut active.virtual_reader {
                let read = reader
                    .read_next()?
                    .ok_or_else(|| ErrorCode::Internal("virtual granule reader ended early"))?;
                if read.range != expected_range
                    || read.data.row_range() != range_read.data.row_range()
                {
                    return Err(ErrorCode::Internal(
                        "virtual granule ranges are out of sync",
                    ));
                }
                let meta = fuse_part
                    .block_meta_index()
                    .and_then(|index| index.virtual_block_meta.as_ref())
                    .ok_or_else(|| ErrorCode::Internal("virtual granule metadata is missing"))?;
                let schema = VirtualColumnReader::read_schema(meta).ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "virtual sidecar {} has a read slot without a parquet type",
                        meta.virtual_block_location
                    ))
                })?;
                Some(VirtualBlockReadResult::create(
                    meta.virtual_block_location.clone(),
                    read.data.row_range().unwrap().len(),
                    fuse_part.compression,
                    read.data,
                    schema,
                    meta.fields.clone(),
                    meta.read_slots.clone(),
                    None,
                ))
            } else {
                None
            };
            self.diagnostics.granule_read += read_start.elapsed();
            decoded_ranges.push(self.decode_granule_range(
                fuse_part,
                range_read.data,
                virtual_data,
            )?);
        }

        let finalize_start = Instant::now();
        if self.block_meta_options.update_stream_columns {
            // Stream columns number the rows of each output block from its first block row.
            for decoded in decoded_ranges {
                self.record_scan_progress(decoded.scan_stats);
                let block = add_data_block_meta(
                    decoded.block,
                    fuse_part,
                    decoded.offsets,
                    self.base_block_ids.clone(),
                    &self.block_meta_options,
                    decoded.start_row,
                )?;
                self.output_data.push_back(block);
            }
        } else {
            let mut offsets = self
                .block_meta_options
                .query_internal_columns
                .then(RoaringTreemap::new);
            let mut scan_stats = BlockScanStats::default();
            let blocks = decoded_ranges
                .into_iter()
                .map(|decoded| {
                    if let (Some(offsets), Some(range_offsets)) = (&mut offsets, decoded.offsets) {
                        *offsets |= range_offsets;
                    }
                    scan_stats.rows += decoded.scan_stats.rows;
                    scan_stats.bytes += decoded.scan_stats.bytes;
                    decoded.block
                })
                .collect::<Vec<_>>();
            let block = DataBlock::concat(&blocks)?;
            let block = add_data_block_meta(
                block,
                fuse_part,
                offsets,
                self.base_block_ids.clone(),
                &self.block_meta_options,
                0,
            )?;
            self.record_scan_progress(scan_stats);
            self.output_data.push_back(block);
        }
        self.diagnostics.finalize_concat += finalize_start.elapsed();
        metrics_inc_remote_io_deserialize_milliseconds(start.elapsed().as_millis() as u64);

        if !active.groups.is_empty() {
            self.active_granule_read = Some(active);
        }
        Ok(())
    }
}

impl Drop for DeserializeDataTransform {
    fn drop(&mut self) {
        let diagnostics = &self.diagnostics;
        if !diagnostics.has_activity() {
            return;
        }
        log::debug!(
            "[FUSE-PRUNER-DIAG] stage=execute_scan scan_id={} normal_parts={} granule_parts={} granule_groups={} granule_ranges={} projected_columns={} selected_bytes={} coalesced_bytes={} coalesced_requests={} input_rows={} output_blocks={} output_rows={} output_bytes={} granule_reader_create_us={} granule_read_us={} decode_filter_us={} finalize_concat_us={} normal_process_us={}",
            self.scan_id,
            diagnostics.normal_parts,
            diagnostics.granule_parts,
            diagnostics.granule_groups,
            diagnostics.granule_ranges,
            diagnostics.projected_columns,
            diagnostics.selected_bytes,
            diagnostics.coalesced_bytes,
            diagnostics.coalesced_requests,
            diagnostics.input_rows,
            diagnostics.output_blocks,
            diagnostics.output_rows,
            diagnostics.output_bytes,
            diagnostics.granule_reader_create.as_micros(),
            diagnostics.granule_read.as_micros(),
            diagnostics.decode_filter.as_micros(),
            diagnostics.finalize_concat.as_micros(),
            diagnostics.normal_process.as_micros(),
        );
    }
}

#[async_trait::async_trait]
impl Processor for DeserializeDataTransform {
    fn name(&self) -> String {
        String::from("DeserializeDataTransform")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.finish_pending();
            self.active_granule_read = None;
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.pop_front() {
            self.diagnostics.output_blocks += 1;
            self.diagnostics.output_rows += data_block.num_rows();
            self.diagnostics.output_bytes += data_block.memory_size();
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.pending.is_some() || self.active_granule_read.is_some() || !self.chunks.is_empty() {
            if !self.input.has_data() {
                self.input.set_need_data();
            }

            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;
            if let Some(source_meta) = data_block.take_meta()
                && let Some(source_meta) =
                    DataSourceWithMeta::<BlockSource>::downcast_from(source_meta)
            {
                self.parts = source_meta.meta;
                self.chunks = source_meta.data;
                return Ok(Event::Sync);
            }
            return Err(ErrorCode::Internal(
                "DeserializeDataTransform got wrong meta data",
            ));
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if self.pending.is_some() {
            return self.process_pending();
        }
        if self.active_granule_read.is_some() {
            return self.process_granule_group();
        }

        let part = self.parts.pop();
        let chunks = self.chunks.pop();
        let Some((part, source)) = part.zip(chunks) else {
            return Ok(());
        };
        match source {
            BlockSource::IndexRows(offsets) => self.process_index_rows(&part, offsets),
            BlockSource::Parquet(read_res) => self.deserialize_parquet(part, read_res),
            BlockSource::Granule(groups) => {
                self.start_granule_read(part, groups)?;
                self.process_granule_group()
            }
        }
    }
}

/// `schema` followed by the virtual columns, in the order they are appended to blocks.
fn with_virtual_fields(
    schema: DataSchema,
    virtual_reader: Option<&VirtualColumnReader>,
) -> DataSchema {
    let Some(virtual_reader) = virtual_reader else {
        return schema;
    };
    let mut fields = schema.fields().clone();
    for virtual_column_field in &virtual_reader.virtual_column_info.virtual_column_fields {
        fields.push(DataField::new(
            &virtual_column_field.name,
            DataType::from(&*virtual_column_field.data_type),
        ));
    }
    DataSchema::new(fields)
}
