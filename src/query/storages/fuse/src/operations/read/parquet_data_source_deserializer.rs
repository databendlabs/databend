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

use super::parquet_data_source::ParquetDataSource;
use super::read_block_context::ReadBlockContext;
use super::read_state::ReadState;
use super::util::add_data_block_meta;
use crate::fuse_part::FuseBlockPartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReadResult;
use crate::io::BlockReader;
use crate::io::GranuleDataReader;
use crate::io::VirtualBlockReadResult;
use crate::io::VirtualColumnReader;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;

struct ActiveGranuleRead {
    part: PartInfoPtr,
    groups: VecDeque<Vec<std::ops::Range<usize>>>,
    reader: GranuleDataReader,
}

struct DecodedRange {
    block: DataBlock,
    offsets: Option<RoaringTreemap>,
    start_row: usize,
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
    chunks: Vec<ParquetDataSource>,
    active_granule_read: Option<ActiveGranuleRead>,

    index_reader: Arc<Option<AggIndexReader>>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,

    base_block_ids: Option<Scalar>,
    block_meta_options: BlockMetaOptions,

    prewhere_info: Option<PrewhereInfo>,
    read_state: Option<ReadState>,
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
        index_reader: Arc<Option<AggIndexReader>>,
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
            read_block_context,
            input,
            output,
            output_data: VecDeque::new(),
            src_schema,
            output_schema,
            parts: vec![],
            chunks: vec![],
            active_granule_read: None,
            index_reader,
            virtual_reader,
            base_block_ids: plan.base_block_ids.clone(),
            block_meta_options: plan.block_meta_options.clone(),
            prewhere_info,
            read_state: None,
        })))
    }

    fn ensure_read_state(&mut self) -> Result<()> {
        if self.read_state.is_none() {
            self.read_state = Some(ReadState::create(
                self.ctx.clone(),
                self.scan_id,
                self.prewhere_info.as_ref(),
                self.block_reader.clone(),
            )?);
        }
        Ok(())
    }

    fn record_block_progress(&self, block: &DataBlock) {
        self.scan_progress.incr(&ProgressValues {
            rows: block.num_rows(),
            bytes: block.memory_size(),
        });
        Profile::record_usize_profile(ProfileStatisticsName::ScanBytes, block.memory_size());
    }

    fn offsets_for_range(
        start_row: usize,
        num_rows: usize,
        bitmap: Option<&Bitmap>,
    ) -> RoaringTreemap {
        let mut offsets = RoaringTreemap::new();
        match bitmap {
            Some(bitmap) => {
                for index in 0..bitmap.len() {
                    if unsafe { bitmap.get_bit_unchecked(index) } {
                        offsets.insert((start_row + index) as u64);
                    }
                }
            }
            None => {
                offsets.insert_range(start_row as u64..(start_row + num_rows) as u64);
            }
        }
        offsets
    }

    fn decode_normal_range(
        &mut self,
        part: &FuseBlockPartInfo,
        data: BlockReadResult,
        virtual_data: Option<VirtualBlockReadResult>,
    ) -> Result<DecodedRange> {
        self.ensure_read_state()?;
        let row_range = data.row_range();
        let num_rows = row_range.map(|range| range.len()).unwrap_or(part.nums_rows);
        let start_row = row_range.map(|range| range.start).unwrap_or(0);
        let columns_chunks = data.columns_chunks()?;
        let (mut block, row_selection, bitmap_selection) = self
            .read_state
            .as_ref()
            .unwrap()
            .deserialize_and_filter_with_num_rows(columns_chunks, part, num_rows)?;

        if let Some(virtual_reader) = self.virtual_reader.as_ref() {
            block = virtual_reader.deserialize_virtual_columns(
                block,
                virtual_data,
                row_selection
                    .as_ref()
                    .map(|selection| selection.selection.clone()),
            )?;
        }

        block = block.resort(&self.src_schema, &self.output_schema)?;
        let offsets = match self.block_meta_options.query_internal_columns {
            false => None,
            true => match bitmap_selection.as_ref() {
                Some(bitmap) => Some(Self::offsets_for_range(start_row, num_rows, Some(bitmap))),
                None if row_range.is_some() => {
                    Some(Self::offsets_for_range(start_row, num_rows, None))
                }
                None => None,
            },
        };

        Ok(DecodedRange {
            block,
            offsets,
            start_row,
        })
    }

    fn finalize_normal_range(
        &self,
        part: &FuseBlockPartInfo,
        decoded: DecodedRange,
    ) -> Result<DataBlock> {
        let offsets = if self.block_meta_options.query_internal_columns {
            decoded.offsets
        } else {
            None
        };
        add_data_block_meta(
            decoded.block,
            part,
            offsets,
            self.base_block_ids.clone(),
            &self.block_meta_options,
            decoded.start_row,
        )
    }

    fn process_normal(
        &mut self,
        part: &PartInfoPtr,
        results: Vec<BlockReadResult>,
        virtual_data: Option<VirtualBlockReadResult>,
    ) -> Result<()> {
        let start = Instant::now();
        let fuse_part = FuseBlockPartInfo::from_part(part)?;
        let mut virtual_data = virtual_data;
        for data in results {
            let decoded = self.decode_normal_range(fuse_part, data, virtual_data.take())?;
            let block = self.finalize_normal_range(fuse_part, decoded)?;
            self.record_block_progress(&block);
            self.output_data.push_back(block);
        }
        metrics_inc_remote_io_deserialize_milliseconds(start.elapsed().as_millis() as u64);
        Ok(())
    }

    fn start_granule_read(
        &mut self,
        part: PartInfoPtr,
        groups: Vec<Vec<std::ops::Range<usize>>>,
    ) -> Result<()> {
        let reader = self
            .read_block_context
            .create_granule_data_reader(&part, &groups)?;
        self.active_granule_read = Some(ActiveGranuleRead {
            part,
            groups: groups.into(),
            reader,
        });
        Ok(())
    }

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
            let range_read = active.reader.read_next()?.ok_or_else(|| {
                ErrorCode::Internal("granule data reader ended before group was complete")
            })?;
            if range_read.range != expected_range {
                return Err(ErrorCode::Internal("granule read ranges are out of sync"));
            }
            decoded_ranges.push(self.decode_normal_range(fuse_part, range_read.data, None)?);
        }

        if self.block_meta_options.update_stream_columns {
            for decoded in decoded_ranges {
                let block = self.finalize_normal_range(fuse_part, decoded)?;
                self.record_block_progress(&block);
                self.output_data.push_back(block);
            }
        } else {
            let mut offsets = self
                .block_meta_options
                .query_internal_columns
                .then(RoaringTreemap::new);
            let blocks = decoded_ranges
                .into_iter()
                .map(|decoded| {
                    if let (Some(offsets), Some(range_offsets)) = (&mut offsets, decoded.offsets) {
                        *offsets |= range_offsets;
                    }
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
            self.record_block_progress(&block);
            self.output_data.push_back(block);
        }
        metrics_inc_remote_io_deserialize_milliseconds(start.elapsed().as_millis() as u64);

        if !active.groups.is_empty() {
            self.active_granule_read = Some(active);
        }
        Ok(())
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
            self.input.finish();
            self.active_granule_read = None;
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

        if self.active_granule_read.is_some() || !self.chunks.is_empty() {
            if !self.input.has_data() {
                self.input.set_need_data();
            }
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;
            if let Some(source_meta) = data_block.take_meta()
                && let Some(source_meta) =
                    DataSourceWithMeta::<ParquetDataSource>::downcast_from(source_meta)
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
        if self.active_granule_read.is_some() {
            return self.process_granule_group();
        }

        let part = self.parts.pop();
        let source = self.chunks.pop();
        if let Some((part, source)) = part.zip(source) {
            match source {
                ParquetDataSource::AggIndex((actual_part, data)) => {
                    let agg_index_reader = self.index_reader.as_ref().as_ref().unwrap();
                    let block = agg_index_reader.deserialize_parquet_data(actual_part, data)?;
                    self.record_block_progress(&block);
                    self.output_data.push_back(block);
                }
                ParquetDataSource::Normal((results, virtual_data)) => {
                    self.process_normal(&part, results, virtual_data)?;
                }
                ParquetDataSource::Granule(groups) => {
                    self.start_granule_read(part, groups)?;
                    self.process_granule_group()?;
                }
            }
        }
        Ok(())
    }
}
