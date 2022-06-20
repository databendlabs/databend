//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::sync::Arc;

use async_trait::async_trait;
use common_arrow::parquet::metadata::ThriftFileMetaData;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use opendal::Operator;

use super::AppendOperationLogEntry;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::processor::Event;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::Processor;
use crate::sessions::QueryContext;
use crate::storages::fuse::io::serialize_data_blocks;
use crate::storages::fuse::io::TableMetaLocationGenerator;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::Statistics;
use crate::storages::fuse::statistics::accumulator::BlockStatistics;
use crate::storages::fuse::statistics::StatisticsAccumulator;
use crate::storages::index::ClusterKeyInfo;

enum State {
    None,
    NeedSerialize(DataBlock),
    Serialized {
        data: Vec<u8>,
        size: u64,
        meta_data: Box<ThriftFileMetaData>,
        block_statistics: BlockStatistics,
    },
    GenerateSegment,
    SerializedSegment {
        data: Vec<u8>,
        location: String,
        segment: Arc<SegmentInfo>,
    },
    Finished,
}

pub struct FuseTableSink {
    state: State,
    input: Arc<InputPort>,
    ctx: Arc<QueryContext>,
    data_accessor: Operator,
    num_block_threshold: u64,
    meta_locations: TableMetaLocationGenerator,
    accumulator: StatisticsAccumulator,
    cluster_key_info: Option<ClusterKeyInfo>,
}

impl FuseTableSink {
    pub fn create(
        input: Arc<InputPort>,
        ctx: Arc<QueryContext>,
        num_block_threshold: usize,
        data_accessor: Operator,
        meta_locations: TableMetaLocationGenerator,
        cluster_key_info: Option<ClusterKeyInfo>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(FuseTableSink {
            ctx,
            input,
            data_accessor,
            meta_locations,
            state: State::None,
            accumulator: Default::default(),
            num_block_threshold: num_block_threshold as u64,
            cluster_key_info,
        })))
    }
}

#[async_trait]
impl Processor for FuseTableSink {
    fn name(&self) -> &'static str {
        "FuseSink"
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(
            &self.state,
            State::NeedSerialize(_) | State::GenerateSegment
        ) {
            return Ok(Event::Sync);
        }

        if matches!(
            &self.state,
            State::Serialized { .. } | State::SerializedSegment { .. }
        ) {
            return Ok(Event::Async);
        }

        if self.input.is_finished() {
            if self.accumulator.summary_row_count != 0 {
                self.state = State::GenerateSegment;
                return Ok(Event::Sync);
            }

            self.state = State::Finished;
            return Ok(Event::Finished);
        }

        if !self.input.has_data() {
            self.input.set_need_data();
            return Ok(Event::NeedData);
        }

        self.state = State::NeedSerialize(self.input.pull_data().unwrap()?);
        Ok(Event::Sync)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::NeedSerialize(data_block) => {
                let mut cluster_stats = None;
                let mut block = data_block;
                if let Some(v) = &self.cluster_key_info {
                    cluster_stats = BlockStatistics::clusters_statistics(
                        v.cluster_key_id,
                        v.cluster_key_index.clone(),
                        block.clone(),
                    )?;

                    // Remove unused columns before serialize
                    if let Some(executor) = &v.expression_executor {
                        block = executor.execute(&block)?;
                    }
                }

                let location = self.meta_locations.gen_block_location();
                let block_statistics = BlockStatistics::from(&block, location, cluster_stats)?;

                // we need a configuration of block size threshold here
                let mut data = Vec::with_capacity(100 * 1024 * 1024);
                let schema = block.schema().clone();
                let (size, meta_data) = serialize_data_blocks(vec![block], &schema, &mut data)?;
                self.state = State::Serialized {
                    data,
                    size,
                    block_statistics,
                    meta_data: Box::new(meta_data),
                };
            }
            State::GenerateSegment => {
                let acc = std::mem::take(&mut self.accumulator);
                let col_stats = acc.summary()?;

                let segment_info = SegmentInfo::new(acc.blocks_metas, Statistics {
                    row_count: acc.summary_row_count,
                    block_count: acc.summary_block_count,
                    uncompressed_byte_size: acc.in_memory_size,
                    compressed_byte_size: acc.file_size,
                    col_stats,
                });

                self.state = State::SerializedSegment {
                    data: serde_json::to_vec(&segment_info)?,
                    location: self.meta_locations.gen_segment_info_location(),
                    segment: Arc::new(segment_info),
                }
            }
            _state => {
                return Err(ErrorCode::LogicalError("Unknown state for fuse table sink"));
            }
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::Serialized {
                data,
                size,
                meta_data,
                block_statistics,
            } => {
                self.data_accessor
                    .object(&block_statistics.block_file_location)
                    .write(data)
                    .await?;

                self.accumulator
                    .add_block(size, *meta_data, block_statistics)?;
                if self.accumulator.summary_block_count >= self.num_block_threshold {
                    self.state = State::GenerateSegment;
                }
            }
            State::SerializedSegment {
                data,
                location,
                segment,
            } => {
                self.data_accessor.object(&location).write(data).await?;

                // TODO: dyn operation for table trait
                let log_entry = AppendOperationLogEntry::new(location, segment);
                self.ctx
                    .push_precommit_block(DataBlock::try_from(log_entry)?);
            }
            _state => {
                return Err(ErrorCode::LogicalError(
                    "Unknown state for fuse table sink.",
                ));
            }
        }

        Ok(())
    }
}
