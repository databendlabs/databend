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
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use common_base::base::ProgressValues;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockThresholds;
use common_expression::DataBlock;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::OutputPort;
use opendal::Operator;
use storages_common_cache::CacheAccessor;
use storages_common_cache_manager::CachedObject;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::Versioned;
use tracing::info;

use crate::io::write_data;
use crate::io::BlockBuilder;
use crate::io::BlockSerialization;
use crate::metrics::metrics_inc_block_index_write_bytes;
use crate::metrics::metrics_inc_block_index_write_milliseconds;
use crate::metrics::metrics_inc_block_index_write_nums;
use crate::metrics::metrics_inc_block_write_bytes;
use crate::metrics::metrics_inc_block_write_milliseconds;
use crate::metrics::metrics_inc_block_write_nums;
use crate::operations::common::AppendOperationLogEntry;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;
use crate::statistics::ClusterStatsGenerator;
use crate::statistics::StatisticsAccumulator;
use crate::FuseTable;

enum State {
    None,
    NeedSerialize(DataBlock),
    Serialized(BlockSerialization),
    GenerateSegment,
    SerializedSegment {
        data: Vec<u8>,
        location: String,
        segment: Arc<SegmentInfo>,
    },
    PreCommitSegment {
        location: String,
        segment: Arc<SegmentInfo>,
    },
    Finished,
}

pub struct AppendTransform {
    data_accessor: Operator,
    accumulator: StatisticsAccumulator,
    block_builder: BlockBuilder,
    state: State,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
}

impl AppendTransform {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table: &FuseTable,
        cluster_stats_gen: ClusterStatsGenerator,
        thresholds: BlockThresholds,
    ) -> Self {
        let source_schema = Arc::new(table.table_info.schema().remove_virtual_computed_fields());
        let block_builder = BlockBuilder {
            ctx,
            meta_locations: table.meta_location_generator().clone(),
            source_schema,
            write_settings: table.get_write_settings(),
            cluster_stats_gen,
        };

        AppendTransform {
            input,
            output,
            output_data: None,
            data_accessor: table.get_operator(),
            block_builder,
            state: State::None,
            accumulator: StatisticsAccumulator::new(thresholds),
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

    pub fn get_block_builder(&self) -> BlockBuilder {
        self.block_builder.clone()
    }
}

#[async_trait]
impl Processor for AppendTransform {
    fn name(&self) -> String {
        "AppendTransform".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(
            &self.state,
            State::NeedSerialize(_) | State::GenerateSegment | State::PreCommitSegment { .. }
        ) {
            return Ok(Event::Sync);
        }

        if matches!(
            &self.state,
            State::Serialized { .. } | State::SerializedSegment { .. }
        ) {
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
            if self.accumulator.summary_row_count != 0 {
                self.state = State::GenerateSegment;
                return Ok(Event::Sync);
            }
            self.output.finish();
            self.state = State::Finished;
            return Ok(Event::Finished);
        }

        if !self.input.has_data() {
            self.input.set_need_data();
            return Ok(Event::NeedData);
        }

        let data_block = self.input.pull_data().unwrap()?;
        if data_block.is_empty() {
            // data source like
            //  `select number from numbers(3000000) where number >=2000000 and number < 3000000`
            // may generate empty data blocks
            Ok(Event::NeedData)
        } else {
            self.state = State::NeedSerialize(data_block);
            Ok(Event::Sync)
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::NeedSerialize(data_block) => {
                let serialized = self.block_builder.build(data_block, |block, generator| {
                    generator.gen_stats_for_append(block)
                })?;

                let progress_values = ProgressValues {
                    rows: serialized.block_meta.row_count as usize,
                    bytes: serialized.block_meta.block_size as usize,
                };
                self.block_builder
                    .ctx
                    .get_write_progress()
                    .incr(&progress_values);

                self.state = State::Serialized(serialized);
            }
            State::GenerateSegment => {
                let acc = std::mem::take(&mut self.accumulator);
                let col_stats = acc.summary();

                let segment_info = SegmentInfo::new(acc.blocks_metas, Statistics {
                    row_count: acc.summary_row_count,
                    block_count: acc.summary_block_count,
                    perfect_block_count: acc.perfect_block_count,
                    uncompressed_byte_size: acc.in_memory_size,
                    compressed_byte_size: acc.file_size,
                    index_size: acc.index_size,
                    col_stats,
                });

                self.state = State::SerializedSegment {
                    data: segment_info.to_bytes()?,
                    location: self
                        .block_builder
                        .meta_locations
                        .gen_segment_info_location(),
                    segment: Arc::new(segment_info),
                }
            }
            State::PreCommitSegment { location, segment } => {
                if let Some(segment_cache) = SegmentInfo::cache() {
                    segment_cache.put(location.clone(), Arc::new(segment.as_ref().try_into()?));
                }

                // emit log entry.
                // for newly created segment, always use the latest version
                let log_entry =
                    AppendOperationLogEntry::new(location, segment, SegmentInfo::VERSION);
                let meta = MutationLogs {
                    entries: vec![MutationLogEntry::Append(log_entry)],
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
            State::Serialized(serialized) => {
                let start = Instant::now();

                // write block data.
                let raw_block_data = serialized.block_raw_data;
                let data_size = raw_block_data.len();
                let path = serialized.block_meta.location.0.as_str();
                write_data(raw_block_data, &self.data_accessor, path).await?;

                // Perf.
                {
                    metrics_inc_block_write_nums(1);
                    metrics_inc_block_write_bytes(data_size as u64);
                    metrics_inc_block_write_milliseconds(start.elapsed().as_millis() as u64);
                }

                // write index data.
                let bloom_index_state = serialized.bloom_index_state;
                if let Some(bloom_index_state) = bloom_index_state {
                    let index_size = bloom_index_state.data.len();
                    write_data(
                        bloom_index_state.data,
                        &self.data_accessor,
                        &bloom_index_state.location.0,
                    )
                    .await?;

                    // Perf.
                    {
                        metrics_inc_block_index_write_nums(1);
                        metrics_inc_block_index_write_bytes(index_size as u64);
                        metrics_inc_block_index_write_milliseconds(
                            start.elapsed().as_millis() as u64
                        );
                    }
                }

                self.accumulator.add_with_block_meta(serialized.block_meta);

                if self.accumulator.summary_block_count
                    >= self.block_builder.write_settings.block_per_seg as u64
                {
                    self.state = State::GenerateSegment;
                }
            }
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
