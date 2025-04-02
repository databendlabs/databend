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
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::PipeItem;
<<<<<<< HEAD
use databend_storages_common_table_meta::meta::column_oriented_segment::*;
use databend_storages_common_table_meta::meta::BlockMeta;
=======
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CachedObject;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;
>>>>>>> 31bba40384 (refactor(query): auto generate virtual columns for variant column)
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::VirtualBlockMeta;
use log::info;
use opendal::Operator;

use crate::io::TableMetaLocationGenerator;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
<<<<<<< HEAD
use crate::statistics::RowOrientedSegmentBuilder;
use crate::FuseSegmentFormat;
=======
use crate::statistics::StatisticsAccumulator;
use crate::statistics::VirtualColumnAccumulator;
>>>>>>> 31bba40384 (refactor(query): auto generate virtual columns for variant column)
use crate::FuseTable;
enum State<B: SegmentBuilder> {
    None,
    GenerateSegment,
    SerializedSegment {
        data: Vec<u8>,
        location: String,
        segment: B::Segment,
    },
    PreCommitSegment {
        location: String,
        segment: B::Segment,
    },
    Finished,
}

pub struct TransformSerializeSegment<B: SegmentBuilder> {
    data_accessor: Operator,
    meta_locations: TableMetaLocationGenerator,
<<<<<<< HEAD
    segment_builder: B,
    state: State<B>,
=======
    accumulator: StatisticsAccumulator,
    virtual_column_accumulator: VirtualColumnAccumulator,
    state: State,
>>>>>>> 31bba40384 (refactor(query): auto generate virtual columns for variant column)
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,

    thresholds: BlockThresholds,
    default_cluster_key_id: Option<u32>,
    table_meta_timestamps: TableMetaTimestamps,
    is_column_oriented: bool,
}

impl<B: SegmentBuilder> TransformSerializeSegment<B> {
    pub fn new(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table: &FuseTable,
        thresholds: BlockThresholds,
        segment_builder: B,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Self {
        let table_meta = &table.table_info.meta;
        let virtual_column_accumulator = VirtualColumnAccumulator::new(&table_meta.virtual_schema);

        let default_cluster_key_id = table.cluster_key_id();

        TransformSerializeSegment {
            input,
            output,
            output_data: None,
            data_accessor: table.get_operator(),
            meta_locations: table.meta_location_generator().clone(),
            state: State::None,
<<<<<<< HEAD
            segment_builder,
=======
            accumulator: Default::default(),
            virtual_column_accumulator,
>>>>>>> 31bba40384 (refactor(query): auto generate virtual columns for variant column)
            thresholds,
            default_cluster_key_id,
            table_meta_timestamps,
            is_column_oriented: table.is_column_oriented(),
        }
    }

    pub fn into_processor(self) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(self)))
    }

    pub fn into_pipe_item(self) -> PipeItem {
        let input = self.input.clone();
        let output = self.output.clone();
        let processor_ptr = ProcessorPtr::create(Box::new(self));
        PipeItem::create(processor_ptr, vec![input], vec![output])
    }
}

pub fn new_serialize_segment_processor(
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    table: &FuseTable,
    thresholds: BlockThresholds,
    table_meta_timestamps: TableMetaTimestamps,
) -> Result<ProcessorPtr> {
    match table.segment_format {
        FuseSegmentFormat::Row => {
            let processor = TransformSerializeSegment::new(
                input,
                output,
                table,
                thresholds,
                RowOrientedSegmentBuilder::default(),
                table_meta_timestamps,
            );
            Ok(ProcessorPtr::create(Box::new(processor)))
        }
        FuseSegmentFormat::Column => {
            let processor = TransformSerializeSegment::new(
                input,
                output,
                table,
                thresholds,
                ColumnOrientedSegmentBuilder::new(table.schema(), thresholds.block_per_segment),
                table_meta_timestamps,
            );
            Ok(ProcessorPtr::create(Box::new(processor)))
        }
    }
}

pub fn new_serialize_segment_pipe_item(
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    table: &FuseTable,
    thresholds: BlockThresholds,
    table_meta_timestamps: TableMetaTimestamps,
) -> Result<PipeItem> {
    match table.segment_format {
        FuseSegmentFormat::Row => {
            let processor = TransformSerializeSegment::new(
                input,
                output,
                table,
                thresholds,
                RowOrientedSegmentBuilder::default(),
                table_meta_timestamps,
            );
            Ok(processor.into_pipe_item())
        }
        FuseSegmentFormat::Column => {
            let processor = TransformSerializeSegment::new(
                input,
                output,
                table,
                thresholds,
                ColumnOrientedSegmentBuilder::new(table.schema(), thresholds.block_per_segment),
                table_meta_timestamps,
            );
            Ok(processor.into_pipe_item())
        }
    }
}

#[async_trait]
impl<B: SegmentBuilder> Processor for TransformSerializeSegment<B> {
    fn name(&self) -> String {
        "TransformSerializeSegment".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(
            &self.state,
            State::GenerateSegment | State::PreCommitSegment { .. }
        ) {
            return Ok(Event::Sync);
        }

        if matches!(&self.state, State::SerializedSegment { .. }) {
            return Ok(Event::Async);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            if self.segment_builder.block_count() != 0 {
                self.state = State::GenerateSegment;
                return Ok(Event::Sync);
            }
            self.output.finish();
            self.state = State::Finished;
            return Ok(Event::Finished);
        }

        if self.input.has_data() {
            let input_meta = self
                .input
                .pull_data()
                .unwrap()?
                .get_meta()
                .cloned()
                .ok_or_else(|| ErrorCode::Internal("No block meta. It's a bug"))?;
            let extended_block_meta = ExtendedBlockMeta::downcast_ref_from(&input_meta)
                .ok_or_else(|| ErrorCode::Internal("No commit meta. It's a bug"))?
                .clone();

<<<<<<< HEAD
            self.segment_builder.add_block(block_meta)?;
            if self.segment_builder.block_count() >= self.thresholds.block_per_segment {
=======
            if let Some(draft_virtual_block_meta) = extended_block_meta.draft_virtual_block_meta {
                let mut block_meta = extended_block_meta.block_meta.clone();

                // generate ColumnId for virtual columns.
                let mut virtual_column_metas = HashMap::new();
                for draft_virtual_column_meta in &draft_virtual_block_meta.virtual_col_metas {
                    self.virtual_column_accumulator.add_virtual_column_meta(
                        draft_virtual_column_meta,
                        &mut virtual_column_metas,
                    );
                }

                let virtual_block_meta = VirtualBlockMeta {
                    virtual_col_metas: virtual_column_metas,
                    virtual_col_size: draft_virtual_block_meta.virtual_col_size,
                    virtual_location: draft_virtual_block_meta.virtual_location.clone(),
                };
                block_meta.virtual_block_meta = Some(virtual_block_meta);

                self.accumulator.add_with_block_meta(block_meta);
            } else {
                self.accumulator
                    .add_with_block_meta(extended_block_meta.block_meta);
            }

            if self.accumulator.summary_block_count >= self.thresholds.block_per_segment as u64 {
>>>>>>> 31bba40384 (refactor(query): auto generate virtual columns for variant column)
                self.state = State::GenerateSegment;
                return Ok(Event::Sync);
            }
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::GenerateSegment => {
                let segment_info = self
                    .segment_builder
                    .build(self.thresholds, self.default_cluster_key_id)?;

                self.state = State::SerializedSegment {
                    data: segment_info.serialize()?,
                    location: self.meta_locations.gen_segment_info_location(
                        self.table_meta_timestamps,
                        self.is_column_oriented,
                    ),
                    segment: segment_info,
                }
            }
            State::PreCommitSegment { location, segment } => {
                // if let Some(segment_cache) = SegmentInfo::cache() {
                //     segment_cache.insert(location.clone(), segment.as_ref().try_into()?);
                // }

                let format_version = SegmentInfo::VERSION;

                // emit log entry.
                // for newly created segment, always use the latest version
                let meta = MutationLogs {
                    entries: vec![MutationLogEntry::AppendSegment {
                        segment_location: location,
                        format_version,
<<<<<<< HEAD
                        summary: segment.summary().clone(),
=======
                        summary: segment.summary.clone(),
                        virtual_schema: Some(
                            self.virtual_column_accumulator.virtual_schema.clone(),
                        ),
>>>>>>> 31bba40384 (refactor(query): auto generate virtual columns for variant column)
                    }],
                };

                self.output_data = Some(DataBlock::empty_with_meta(Box::new(meta)));
            }
            _state => {
                return Err(ErrorCode::Internal("Unknown state for fuse table sink"));
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::SerializedSegment {
                data,
                location,
                segment,
            } => {
                self.data_accessor.write(&location, data).await?;
                info!("fuse append wrote down segment {} ", location);

                self.state = State::PreCommitSegment { location, segment };
            }
            _state => {
                return Err(ErrorCode::Internal("Unknown state for fuse table sink."));
            }
        }

        Ok(())
    }
}
