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

use std::any::Any;
use std::ops::Not;
use std::sync::Arc;

use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_datablocks::serialize_to_parquet;
use common_datablocks::DataBlock;
use common_datavalues::BooleanColumn;
use common_datavalues::ColumnRef;
use common_datavalues::DataSchemaRef;
use common_datavalues::Series;
use common_exception::ErrorCode;
use common_exception::Result;
use common_sql::evaluator::EvalNode;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::ClusterStatistics;
use opendal::Operator;

use super::deletion_meta::Deletion;
use super::deletion_meta::DeletionSourceMeta;
use super::deletion_part::DeletionPartInfo;
use crate::io::write_data;
use crate::io::BlockReader;
use crate::io::TableMetaLocationGenerator;
use crate::operations::util;
use crate::operations::BloomIndexState;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;
use crate::pruning::BlockIndex;
use crate::statistics::gen_columns_statistics;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseTable;
use crate::Table;

type DataChunks = Vec<(usize, Vec<u8>)>;

struct SerializeState {
    block_data: Vec<u8>,
    block_location: String,
    index_data: Vec<u8>,
    index_location: String,
}

enum State {
    ReadData(Option<PartInfoPtr>),
    FilterData(PartInfoPtr, DataChunks),
    ReadRemain {
        part: PartInfoPtr,
        data_block: DataBlock,
        filter: ColumnRef,
    },
    MergeRemain {
        part: PartInfoPtr,
        chunks: DataChunks,
        data_block: DataBlock,
        filter: ColumnRef,
    },
    NeedSerialize(DataBlock),
    Serialized(SerializeState, Arc<BlockMeta>),
    Generated(Deletion),
    Output(Option<PartInfoPtr>, DataBlock),
    Finish,
}

pub struct DeletionSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    output: Arc<OutputPort>,
    location_gen: TableMetaLocationGenerator,
    dal: Operator,
    block_reader: Arc<BlockReader>,
    filter: Arc<EvalNode>,
    remain_reader: Arc<Option<BlockReader>>,

    output_schema: DataSchemaRef,
    index: BlockIndex,
    cluster_stats_gen: ClusterStatsGenerator,
    origin_stats: Option<ClusterStatistics>,
}

impl DeletionSource {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        table: &FuseTable,
        block_reader: Arc<BlockReader>,
        filter: Arc<EvalNode>,
        remain_reader: Arc<Option<BlockReader>>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(DeletionSource {
            state: State::ReadData(None),
            ctx,
            output,
            location_gen: table.meta_location_generator().clone(),
            dal: table.get_operator(),
            block_reader,
            filter,
            remain_reader,
            output_schema: table.schema(),
            index: (0, 0),
            cluster_stats_gen: table.cluster_stats_gen()?,
            origin_stats: None,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for DeletionSource {
    fn name(&self) -> String {
        "DeletionSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::ReadData(None)) {
            self.state = match self.ctx.try_get_part() {
                None => State::Finish,
                Some(part) => State::ReadData(Some(part)),
            }
        }

        if matches!(self.state, State::Finish) {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if matches!(self.state, State::Output(_, _)) {
            if let State::Output(part, data_block) =
                std::mem::replace(&mut self.state, State::Finish)
            {
                self.state = match part {
                    None => State::Finish,
                    Some(part) => State::ReadData(Some(part)),
                };

                self.output.push_data(Ok(data_block));
                return Ok(Event::NeedConsume);
            }
        }

        if matches!(
            self.state,
            State::ReadData(_) | State::ReadRemain { .. } | State::Serialized(_, _)
        ) {
            Ok(Event::Async)
        } else {
            Ok(Event::Sync)
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::FilterData(part, chunks) => {
                let data_block = self.block_reader.deserialize(part.clone(), chunks)?;
                let filter_result = self
                    .filter
                    .eval(&self.ctx.try_get_function_context()?, &data_block)?
                    .vector;
                let predicates = DataBlock::cast_to_nonull_boolean(&filter_result)?;
                // reverse the filter
                let boolean_col: &BooleanColumn = Series::check_get(&predicates)?;
                let values = boolean_col.values();
                let filter: ColumnRef = Arc::new(BooleanColumn::from_arrow_data(values.not()));
                if !DataBlock::filter_exists(&filter)? {
                    // all the rows should be removed.
                    self.state = State::Generated(Deletion::Deleted);
                } else {
                    let num_rows = data_block.num_rows();
                    let data_block = DataBlock::filter_block(data_block, &filter)?;
                    if data_block.num_rows() == num_rows {
                        // none of the rows should be removed.
                        self.state = State::Generated(Deletion::DoNothing);
                    } else if self.remain_reader.is_none() {
                        let block = data_block.resort(self.output_schema.clone())?;
                        self.state = State::NeedSerialize(block);
                    } else {
                        self.state = State::ReadRemain {
                            part,
                            data_block,
                            filter,
                        }
                    }
                }
            }
            State::MergeRemain {
                part,
                chunks,
                mut data_block,
                filter,
            } => {
                let merged = if chunks.is_empty() {
                    data_block
                } else if let Some(remain_reader) = self.remain_reader.as_ref() {
                    let remain_block = remain_reader.deserialize(part, chunks)?;
                    let remain_block = DataBlock::filter_block(remain_block, &filter)?;
                    for (col, field) in remain_block
                        .columns()
                        .iter()
                        .zip(remain_block.schema().fields())
                    {
                        data_block = data_block.add_column(col.clone(), field.clone())?;
                    }
                    data_block
                } else {
                    return Err(ErrorCode::Internal("It's a bug. Need remain reader"));
                };

                let block = merged.resort(self.output_schema.clone())?;
                self.state = State::NeedSerialize(block);
            }
            State::NeedSerialize(block) => {
                let cluster_stats = self
                    .cluster_stats_gen
                    .gen_with_origin_stats(&block, std::mem::take(&mut self.origin_stats))?;

                let row_count = block.num_rows() as u64;
                let block_size = block.memory_size() as u64;
                let (block_location, block_id) = self.location_gen.gen_block_location();

                // build block index.
                let location = self.location_gen.block_bloom_index_location(&block_id);
                let (bloom_index_state, column_distinct_count) =
                    BloomIndexState::try_create(&block, location)?;
                let col_stats = gen_columns_statistics(&block, Some(column_distinct_count))?;

                // serialize data block.
                let mut block_data = Vec::with_capacity(100 * 1024 * 1024);
                let schema = block.schema().clone();
                let (file_size, meta_data) =
                    serialize_to_parquet(vec![block], &schema, &mut block_data)?;
                let col_metas = util::column_metas(&meta_data)?;

                // new block meta.
                let new_meta = Arc::new(BlockMeta::new(
                    row_count,
                    block_size,
                    file_size,
                    col_stats,
                    col_metas,
                    cluster_stats,
                    block_location.clone(),
                    Some(bloom_index_state.location.clone()),
                    bloom_index_state.size,
                ));

                self.state = State::Serialized(
                    SerializeState {
                        block_data,
                        block_location: block_location.0,
                        index_data: bloom_index_state.data,
                        index_location: bloom_index_state.location.0,
                    },
                    new_meta,
                );
            }
            State::Generated(op) => {
                let meta = DeletionSourceMeta::create(self.index, op);
                let new_part = self.ctx.try_get_part();
                self.state = State::Output(new_part, DataBlock::empty_with_meta(meta));
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadData(Some(part)) => {
                let deletion_part = DeletionPartInfo::from_part(&part)?;
                self.index = deletion_part.index;
                self.origin_stats = deletion_part.cluster_stats.clone();
                let part = deletion_part.inner_part.clone();
                let chunks = self.block_reader.read_columns_data(part.clone()).await?;
                self.state = State::FilterData(part, chunks);
            }
            State::ReadRemain {
                part,
                data_block,
                filter,
            } => {
                if let Some(remain_reader) = self.remain_reader.as_ref() {
                    let chunks = remain_reader.read_columns_data(part.clone()).await?;
                    self.state = State::MergeRemain {
                        part,
                        chunks,
                        data_block,
                        filter,
                    };
                } else {
                    return Err(ErrorCode::Internal("It's a bug. No remain reader"));
                }
            }
            State::Serialized(serialize_state, block_meta) => {
                // write block data.
                write_data(
                    &serialize_state.block_data,
                    &self.dal,
                    &serialize_state.block_location,
                )
                .await?;
                // write index data.
                write_data(
                    &serialize_state.index_data,
                    &self.dal,
                    &serialize_state.index_location,
                )
                .await?;
                self.state = State::Generated(Deletion::Replaced(block_meta));
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
