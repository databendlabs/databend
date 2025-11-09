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
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_storages_common_table_meta::meta::column_oriented_segment::*;
use databend_storages_common_table_meta::meta::AdditionalStatsMeta;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::SegmentStatistics;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::VirtualBlockMeta;
use log::info;
use opendal::Operator;

use crate::io::TableMetaLocationGenerator;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::statistics::ColumnHLLAccumulator;
use crate::statistics::RowOrientedSegmentBuilder;
use crate::statistics::VirtualColumnAccumulator;
use crate::FuseSegmentFormat;
use crate::FuseTable;

enum State<B: SegmentBuilder> {
    None,
    GenerateSegment,
    SerializedSegment {
        data: Vec<u8>,
        location: String,
        segment: B::Segment,

        stats: Option<(String, Vec<u8>, BlockHLL)>,
    },
    PreCommitSegment {
        location: String,
        segment: B::Segment,
        hll: BlockHLL,
    },
    Finished,
}

pub struct TransformSerializeSegment<B: SegmentBuilder> {
    data_accessor: Operator,
    meta_locations: TableMetaLocationGenerator,
    segment_builder: B,
    virtual_column_accumulator: Option<VirtualColumnAccumulator>,
    hll_accumulator: ColumnHLLAccumulator,
    state: State<B>,

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
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table: &FuseTable,
        thresholds: BlockThresholds,
        segment_builder: B,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Self {
        let table_meta = &table.table_info.meta;
        let virtual_column_accumulator = VirtualColumnAccumulator::try_create(
            ctx,
            &table_meta.schema,
            &table_meta.virtual_schema,
        );

        let default_cluster_key_id = table.cluster_key_id();

        TransformSerializeSegment {
            input,
            output,
            output_data: None,
            data_accessor: table.get_operator(),
            meta_locations: table.meta_location_generator().clone(),
            state: State::None,
            segment_builder,
            virtual_column_accumulator,
            hll_accumulator: ColumnHLLAccumulator::default(),
            thresholds,
            default_cluster_key_id,
            table_meta_timestamps,
            is_column_oriented: table.is_column_oriented(),
        }
    }
}

pub fn new_serialize_segment_processor(
    ctx: Arc<dyn TableContext>,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    table: &FuseTable,
    thresholds: BlockThresholds,
    table_meta_timestamps: TableMetaTimestamps,
) -> Result<ProcessorPtr> {
    match table.segment_format {
        FuseSegmentFormat::Row => {
            let processor = TransformSerializeSegment::new(
                ctx,
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
                ctx,
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

            let virtual_column_accumulator = std::mem::take(&mut self.virtual_column_accumulator);
            if let Some(virtual_column_accumulator) = virtual_column_accumulator {
                if let Some(virtual_schema) =
                    virtual_column_accumulator.build_virtual_schema_with_block_number()
                {
                    // emit log entry.
                    // for newly created virtual schema.
                    let meta = MutationLogs {
                        entries: vec![MutationLogEntry::AppendVirtualSchema { virtual_schema }],
                    };
                    let data_block = DataBlock::empty_with_meta(Box::new(meta));
                    self.output.push_data(Ok(data_block));
                    return Ok(Event::NeedConsume);
                }
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
                .ok_or_else(|| ErrorCode::Internal("No block meta. It's a bug"))?
                .clone();

            if let Some(draft_virtual_block_meta) = extended_block_meta.draft_virtual_block_meta {
                let mut block_meta = extended_block_meta.block_meta.clone();
                if let Some(ref mut virtual_column_accumulator) = self.virtual_column_accumulator {
                    // generate ColumnId for virtual columns.
                    let virtual_column_metas = virtual_column_accumulator
                        .add_virtual_column_metas(&draft_virtual_block_meta.virtual_column_metas);

                    let virtual_block_meta = VirtualBlockMeta {
                        virtual_column_metas,
                        virtual_column_size: draft_virtual_block_meta.virtual_column_size,
                        virtual_location: draft_virtual_block_meta.virtual_location.clone(),
                    };
                    block_meta.virtual_block_meta = Some(virtual_block_meta);
                }

                self.segment_builder.add_block(block_meta)?;
            } else {
                self.segment_builder
                    .add_block(extended_block_meta.block_meta)?;
            }

            if let Some(hll) = extended_block_meta.column_hlls {
                self.hll_accumulator.add_hll(hll)?;
            }
            if self.segment_builder.block_count() >= self.thresholds.block_per_segment {
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
                let location = self
                    .meta_locations
                    .gen_segment_info_location(self.table_meta_timestamps, self.is_column_oriented);

                let mut additional_stats_meta = None;
                let mut stats = None;
                if !self.hll_accumulator.is_empty() {
                    let segment_stats_location = TableMetaLocationGenerator::gen_segment_stats_location_from_segment_location(location.as_str());
                    let stats_data = self.hll_accumulator.build().to_bytes()?;
                    let stats_summary = self.hll_accumulator.take_summary();
                    additional_stats_meta = Some(AdditionalStatsMeta {
                        size: stats_data.len() as u64,
                        location: (segment_stats_location.clone(), SegmentStatistics::VERSION),
                        ..Default::default()
                    });
                    stats = Some((segment_stats_location, stats_data, stats_summary));
                }

                let segment_info = self.segment_builder.build(
                    self.thresholds,
                    self.default_cluster_key_id,
                    additional_stats_meta,
                )?;

                self.state = State::SerializedSegment {
                    data: segment_info.serialize()?,
                    location,
                    segment: segment_info,
                    stats,
                }
            }
            State::PreCommitSegment {
                location,
                segment,
                hll,
            } => {
                let format_version = SegmentInfo::VERSION;

                // emit log entry.
                // for newly created segment, always use the latest version
                let meta = MutationLogs {
                    entries: vec![MutationLogEntry::AppendSegment {
                        segment_location: location,
                        format_version,
                        summary: segment.summary().clone(),
                        hll,
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
                stats,
            } => {
                self.data_accessor.write(&location, data).await?;
                let mut hll = HashMap::new();
                if let Some((location, stats, summary)) = stats {
                    self.data_accessor.write(&location, stats).await?;
                    hll = summary;
                }
                info!("fuse append wrote down segment {} ", location);

                self.state = State::PreCommitSegment {
                    location,
                    segment,
                    hll,
                };
            }
            _state => {
                return Err(ErrorCode::Internal("Unknown state for fuse table sink."));
            }
        }

        Ok(())
    }
}
