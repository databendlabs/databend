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
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_metrics::storage::*;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;

use super::parquet_data_source::ParquetDataSource;
use super::read_state::BlockBatches;
use super::read_state::BlockScanStats;
use super::read_state::ReadState;
use super::read_state::ReadStateBatch;
use super::read_state::ReadStateOutput;
use super::util::add_data_block_meta;
use super::util::selected_row_offsets;
use crate::fuse_part::FuseBlockPartInfo;
use crate::io::BlockReader;
use crate::io::RowSelection;
use crate::io::VirtualColumnReader;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;

pub struct DeserializeDataTransform {
    ctx: Arc<dyn TableContext>,
    scan_id: usize,
    scan_progress: Arc<Progress>,
    block_reader: Arc<BlockReader>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: VecDeque<DataBlock>,
    src_schema: DataSchema,
    output_schema: DataSchema,
    parts: Vec<PartInfoPtr>,
    chunks: Vec<ParquetDataSource>,

    virtual_reader: Arc<Option<VirtualColumnReader>>,

    base_block_ids: Option<Scalar>,
    block_meta_options: BlockMetaOptions,

    prewhere_info: Option<PrewhereInfo>,
    read_state: Option<ReadState>,
    /// The block whose batches are being output, see [`ReadState::start_batches`].
    pending: Option<PendingBlock>,
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
    pub fn create(
        ctx: Arc<dyn TableContext>,
        block_reader: Arc<BlockReader>,
        plan: &DataSourcePlan,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();

        let mut src_schema: DataSchema = (block_reader.schema().as_ref()).into();
        if let Some(virtual_reader) = virtual_reader.as_ref() {
            let mut fields = src_schema.fields().clone();
            for virtual_column_field in &virtual_reader.virtual_column_info.virtual_column_fields {
                let field = DataField::new(
                    &virtual_column_field.name,
                    DataType::from(&*virtual_column_field.data_type),
                );
                fields.push(field);
            }
            src_schema = DataSchema::new(fields);
        }

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
        })))
    }
}

impl DeserializeDataTransform {
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
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.pop_front() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.pending.is_some() || !self.chunks.is_empty() {
            if !self.input.has_data() {
                self.input.set_need_data();
            }

            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;
            if let Some(source_meta) = data_block.take_meta() {
                if let Some(source_meta) =
                    DataSourceWithMeta::<ParquetDataSource>::downcast_from(source_meta)
                {
                    self.parts = source_meta.meta;
                    self.chunks = source_meta.data;
                    return Ok(Event::Sync);
                }
            }

            unreachable!();
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

        let part = self.parts.pop();
        let chunks = self.chunks.pop();
        if let Some((part, read_res)) = part.zip(chunks) {
            let start = Instant::now();
            let columns_chunks = read_res.data.columns_chunks()?;
            let fuse_part = FuseBlockPartInfo::from_part(&part)?;

            if self.read_state.is_none() {
                self.read_state = Some(ReadState::create(
                    self.ctx.clone(),
                    self.scan_id,
                    self.prewhere_info.as_ref(),
                    self.block_reader.clone(),
                )?);
            }

            let no_output_columns =
                self.output_schema.fields().is_empty() && self.virtual_reader.is_none();

            if self.can_split_block(fuse_part)
                && let Some(batches) = self.read_state.as_mut().unwrap().start_batches(
                    &columns_chunks,
                    fuse_part,
                    no_output_columns,
                    self.block_meta_options.query_internal_columns,
                )?
            {
                // The batches own the column chunks they decode from.
                drop(columns_chunks);
                drop(read_res);
                self.pending = Some(PendingBlock {
                    part: part.clone(),
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
                fuse_part,
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
                data_block = virtual_reader
                    .deserialize_virtual_columns(
                        data_block,
                        read_res.virtual_data,
                        bitmap_selection
                            .as_ref()
                            .map(|bitmap| RowSelection::from(bitmap).selection),
                    )
                    .inspect_err(|error| {
                        log::warn!(
                            "failed to deserialize virtual columns: block_location={}, virtual_block_location={:?}, error={}",
                            fuse_part.location,
                            virtual_block_location,
                            error
                        );
                    })?;
                scan_stats.add_decoded_bytes(
                    data_block.memory_size().saturating_sub(bytes_before),
                    data_block.num_rows(),
                );
            }

            self.record_scan(start.elapsed(), scan_stats);

            let mut data_block = data_block.resort(&self.src_schema, &self.output_schema)?;

            // Fill `BlockMetaIndex` as `DataBlock.meta` if query internal columns,
            // `TransformAddInternalColumns` will generate internal columns using `BlockMetaIndex` in next pipeline.
            let offsets = if self.block_meta_options.query_internal_columns {
                bitmap_selection.as_ref().map(selected_row_offsets)
            } else {
                None
            };

            data_block = add_data_block_meta(
                data_block,
                fuse_part,
                offsets,
                self.base_block_ids.clone(),
                &self.block_meta_options,
            )?;

            self.output_data.push_back(data_block);
        }

        Ok(())
    }
}
