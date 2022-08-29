//  Copyright 2022 Datafuse Labs.
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

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use common_datablocks::serialize_data_blocks;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::Statistics as FuseMetaStatistics;
use common_planners::PartInfoPtr;
use common_planners::Projection;
use opendal::Operator;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::storages::fuse::io::BlockReader;
use crate::storages::fuse::statistics::BlockStatistics;
use crate::storages::fuse::statistics::StatisticsAccumulator;
use crate::storages::fuse::FuseTable;
use crate::storages::result::block_buffer::BlockBuffer;
use crate::storages::result::block_buffer::BlockInfo;
use crate::storages::result::result_locations::ResultLocations;
use crate::storages::result::result_table::ResultStorageInfo;
use crate::storages::result::result_table::ResultTableMeta;
use crate::storages::result::ResultQueryInfo;

enum State {
    None,
    NeedSerialize(DataBlock),
    Serialized {
        block: DataBlock,
        data: Vec<u8>,
        location: String,
    },
    GenerateMeta,
    SerializedMeta {
        data: Vec<u8>,
        location: String,
    },
    Finished,
}

pub struct ResultTableSink {
    state: State,
    input: Arc<InputPort>,
    #[allow(unused)]
    ctx: Arc<QueryContext>,
    data_accessor: Operator,
    #[allow(unused)]
    query_info: ResultQueryInfo,
    locations: ResultLocations,
    accumulator: StatisticsAccumulator,
    block_buffer: Arc<BlockBuffer>,
    block_reader: Arc<BlockReader>,
}

impl ResultTableSink {
    #[allow(unused)]
    pub fn create(
        input: Arc<InputPort>,
        ctx: Arc<QueryContext>,
        data_accessor: Operator,
        query_info: ResultQueryInfo,
        block_buffer: Arc<BlockBuffer>,
    ) -> Result<ProcessorPtr> {
        let schema = query_info.schema.clone();
        let query_id = query_info.query_id.clone();
        let indices = (0..schema.fields().len())
            .into_iter()
            .collect::<Vec<usize>>();
        let projection = Projection::Columns(indices);

        let block_reader = BlockReader::create(ctx.get_storage_operator()?, schema, projection)?;
        Ok(ProcessorPtr::create(Box::new(ResultTableSink {
            ctx,
            input,
            data_accessor,
            state: State::None,
            accumulator: Default::default(),
            query_info,
            block_buffer,
            locations: ResultLocations::new(&query_id),
            block_reader,
        })))
    }
    pub fn get_last_part_info(&mut self) -> PartInfoPtr {
        let meta = self.accumulator.blocks_metas.last().unwrap();
        FuseTable::all_columns_part(meta)
    }

    async fn push(&mut self, block: DataBlock) -> Result<()> {
        if !self.block_buffer.try_push_block(block).await {
            let part_info = self.get_last_part_info();
            let block_info = BlockInfo {
                part_info,
                reader: self.block_reader.clone(),
            };
            self.block_buffer.push_info(block_info).await;
        }
        Ok(())
    }

    pub fn gen_meta(&mut self) -> Result<(Vec<u8>, String)> {
        let acc = std::mem::take(&mut self.accumulator);
        let col_stats = acc.summary()?;
        let segment_info = SegmentInfo::new(acc.blocks_metas, FuseMetaStatistics {
            row_count: acc.summary_row_count,
            block_count: acc.summary_block_count,
            uncompressed_byte_size: acc.in_memory_size,
            compressed_byte_size: acc.file_size,
            col_stats,
            index_size: 0,
        });

        let meta = ResultTableMeta {
            query: self.query_info.clone(),
            storage: ResultStorageInfo::FuseSegment(segment_info),
        };
        let meta_data = serde_json::to_vec(&meta)?;
        let meta_location = self.locations.get_meta_location();
        Ok((meta_data, meta_location))
    }
}

#[async_trait]
impl Processor for ResultTableSink {
    fn name(&self) -> &'static str {
        "ResultTableSink"
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        match &mut self.state {
            State::NeedSerialize(_) | State::GenerateMeta => Ok(Event::Sync),
            State::Serialized { .. } | State::SerializedMeta { .. } => Ok(Event::Async),
            State::Finished => Ok(Event::Finished),
            State::None => {
                if self.input.is_finished() {
                    self.state = State::GenerateMeta;
                    Ok(Event::Sync)
                } else if !self.input.has_data() {
                    self.input.set_need_data();
                    Ok(Event::NeedData)
                } else {
                    self.state = State::NeedSerialize(self.input.pull_data().unwrap()?);
                    Ok(Event::Sync)
                }
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::NeedSerialize(block) => {
                let location = self.locations.gen_block_location();
                let block_statistics = BlockStatistics::from(&block, location.clone(), None)?;

                let mut data = Vec::with_capacity(100 * 1024 * 1024);
                let schema = block.schema().clone();
                let (size, meta_data) =
                    serialize_data_blocks(vec![block.clone()], &schema, &mut data)?;

                let bloom_index_location = None;
                let bloom_index_size = 0_u64;
                self.accumulator.add_block(
                    size,
                    meta_data,
                    block_statistics,
                    bloom_index_location,
                    bloom_index_size,
                )?;

                self.state = State::Serialized {
                    block,
                    data,
                    location,
                };
            }
            State::GenerateMeta => {
                let (data, location) = self.gen_meta()?;
                self.state = State::SerializedMeta { data, location };
            }
            _state => {
                return Err(ErrorCode::LogicalError(
                    "Unknown state for result table sink",
                ));
            }
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::Serialized {
                block,
                data,
                location,
            } => {
                self.data_accessor.object(&location).write(data).await?;
                self.push(block).await?;
            }
            State::SerializedMeta { data, location } => {
                self.data_accessor.object(&location).write(data).await?;
                self.state = State::Finished;
            }
            _state => {
                return Err(ErrorCode::LogicalError(
                    "Unknown state for result table sink.",
                ));
            }
        }

        Ok(())
    }
}
