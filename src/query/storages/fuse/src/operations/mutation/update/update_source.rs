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

use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_sql::evaluator::ChunkOperator;
use common_sql::evaluator::EvalNode;
use common_storages_common::blocks_to_parquet;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::table::TableCompression;
use opendal::Operator;

use crate::fuse_part::FusePartInfo;
use crate::io::write_data;
use crate::io::BlockReader;
use crate::io::ReadSettings;
use crate::io::TableMetaLocationGenerator;
use crate::operations::mutation::DataChunks;
use crate::operations::mutation::Mutation;
use crate::operations::mutation::MutationPartInfo;
use crate::operations::mutation::MutationSourceMeta;
use crate::operations::mutation::SerializeState;
use crate::operations::util;
use crate::operations::BloomIndexState;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;
use crate::pruning::BlockIndex;
use crate::statistics::gen_columns_statistics;
use crate::FuseTable;

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
    UpdateData(DataBlock),
    NeedSerialize(DataBlock),
    Serialized(SerializeState, Arc<BlockMeta>),
    Generated(Mutation),
    Output(Option<PartInfoPtr>, DataBlock),
    Finish,
}

pub struct UpdateSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    scan_progress: Arc<Progress>,
    output: Arc<OutputPort>,
    location_gen: TableMetaLocationGenerator,
    dal: Operator,
    table_compression: TableCompression,

    block_reader: Arc<BlockReader>,
    filter: Arc<Option<EvalNode>>,
    remain_reader: Arc<Option<BlockReader>>,
    operators: Vec<ChunkOperator>,

    index: BlockIndex,
}

impl UpdateSource {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        table: &FuseTable,
        block_reader: Arc<BlockReader>,
        filter: Arc<Option<EvalNode>>,
        remain_reader: Arc<Option<BlockReader>>,
        operators: Vec<ChunkOperator>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        Ok(ProcessorPtr::create(Box::new(UpdateSource {
            state: State::ReadData(None),
            ctx: ctx.clone(),
            scan_progress,
            output,
            location_gen: table.meta_location_generator().clone(),
            dal: table.get_operator(),
            table_compression: table.table_compression,
            block_reader,
            filter,
            remain_reader,
            operators,
            index: (0, 0),
        })))
    }
}

#[async_trait::async_trait]
impl Processor for UpdateSource {
    fn name(&self) -> String {
        "UpdateSource".to_string()
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
                let data_block = self
                    .block_reader
                    .deserialize_parquet_chunks(part.clone(), chunks)?;
                if let Some(filter) = self.filter.as_ref() {
                    let filter_result = filter
                        .eval(&self.ctx.try_get_function_context()?, &data_block)?
                        .vector;
                    let filter = DataBlock::cast_to_nonull_boolean(&filter_result)?;
                    if DataBlock::filter_exists(&filter)? {
                        let col: &BooleanColumn = Series::check_get(&filter)?;
                        let progress_values = ProgressValues {
                            rows: col.len() - col.values().unset_bits(),
                            bytes: 0,
                        };
                        self.scan_progress.incr(&progress_values);

                        if self.remain_reader.is_none() {
                            self.state = State::MergeRemain {
                                part,
                                chunks: vec![],
                                data_block,
                                filter,
                            };
                        } else {
                            self.state = State::ReadRemain {
                                part,
                                data_block,
                                filter,
                            };
                        }
                    } else {
                        self.state = State::Generated(Mutation::DoNothing);
                    }
                } else {
                    let progress_values = ProgressValues {
                        rows: data_block.num_rows(),
                        bytes: 0,
                    };
                    self.scan_progress.incr(&progress_values);
                    self.state = State::UpdateData(data_block);
                }
            }
            State::MergeRemain {
                part,
                chunks,
                mut data_block,
                filter,
            } => {
                if let Some(remain_reader) = self.remain_reader.as_ref() {
                    let remain_block = remain_reader.deserialize_parquet_chunks(part, chunks)?;
                    for (col, field) in remain_block
                        .columns()
                        .iter()
                        .zip(remain_block.schema().fields())
                    {
                        data_block = data_block.add_column(col.clone(), field.clone())?;
                    }
                }

                let field = DataField::new("_predicate", bool::to_data_type());
                data_block = data_block.add_column(filter, field)?;

                self.state = State::UpdateData(data_block);
            }
            State::UpdateData(data_block) => {
                let func_ctx = self.ctx.try_get_function_context()?;
                let block = self
                    .operators
                    .iter()
                    .try_fold(data_block, |input, op| op.execute(&func_ctx, input))?;
                self.state = State::NeedSerialize(block);
            }
            State::NeedSerialize(block) => {
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
                let (file_size, meta_data) = blocks_to_parquet(
                    &schema,
                    vec![block],
                    &mut block_data,
                    self.table_compression,
                )?;
                let col_metas = util::column_metas(&meta_data)?;

                // new block meta.
                let new_meta = Arc::new(BlockMeta::new(
                    row_count,
                    block_size,
                    file_size,
                    col_stats,
                    col_metas,
                    None,
                    block_location.clone(),
                    Some(bloom_index_state.location.clone()),
                    bloom_index_state.size,
                    self.table_compression.into(),
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
                let meta = MutationSourceMeta::create(self.index, op);
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
                let settings = ReadSettings::from_ctx(&self.ctx)?;
                let part = MutationPartInfo::from_part(&part)?;
                self.index = part.index;
                let inner_part = part.inner_part.clone();
                let fuse_part = FusePartInfo::from_part(&inner_part)?;

                let read_res = self
                    .block_reader
                    .read_columns_data_by_merge_io(
                        &settings,
                        &fuse_part.location,
                        &fuse_part.columns_meta,
                    )
                    .await?;
                let chunks = read_res
                    .columns_chunks()?
                    .into_iter()
                    .map(|(column_idx, column_chunk)| (column_idx, column_chunk.to_vec()))
                    .collect::<Vec<_>>();
                self.state = State::FilterData(inner_part, chunks);
            }
            State::ReadRemain {
                part,
                data_block,
                filter,
            } => {
                if let Some(remain_reader) = self.remain_reader.as_ref() {
                    let fuse_part = FusePartInfo::from_part(&part)?;

                    let settings = ReadSettings::from_ctx(&self.ctx)?;
                    let read_res = remain_reader
                        .read_columns_data_by_merge_io(
                            &settings,
                            &fuse_part.location,
                            &fuse_part.columns_meta,
                        )
                        .await?;
                    let chunks = read_res
                        .columns_chunks()?
                        .into_iter()
                        .map(|(column_idx, column_chunk)| (column_idx, column_chunk.to_vec()))
                        .collect::<Vec<_>>();

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
                self.state = State::Generated(Mutation::Replaced(block_meta));
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
