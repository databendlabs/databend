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
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::serialize_to_parquet;
use common_expression::types::AnyType;
use common_expression::Chunk;
use common_expression::Column;
use common_expression::DataSchema;
use common_expression::Evaluator;
use common_expression::FunctionContext;
use common_expression::RemoteExpr;
use common_expression::TableSchemaRef;
use common_expression::Value;
use common_functions_v2::scalars::BUILTIN_FUNCTIONS;
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
        chunk: Chunk,
        filter: Value<AnyType>,
    },
    MergeRemain {
        part: PartInfoPtr,
        chunks: DataChunks,
        chunk: Chunk,
        filter: Value<AnyType>,
    },
    NeedSerialize(Chunk),
    Serialized(SerializeState, Arc<BlockMeta>),
    Generated(Deletion),
    Output(Option<PartInfoPtr>, Chunk),
    Finish,
}

pub struct DeletionSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    output: Arc<OutputPort>,
    location_gen: TableMetaLocationGenerator,
    dal: Operator,
    block_reader: Arc<BlockReader>,
    filter: Arc<RemoteExpr<String>>,
    remain_reader: Arc<Option<BlockReader>>,

    source_schema: TableSchemaRef,
    output_schema: TableSchemaRef,
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
        filter: Arc<RemoteExpr<String>>,
        remain_reader: Arc<Option<BlockReader>>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(DeletionSource {
            state: State::ReadData(None),
            ctx: ctx.clone(),
            output,
            location_gen: table.meta_location_generator().clone(),
            dal: table.get_operator(),
            block_reader,
            filter,
            remain_reader,
            source_schema: table.table_info.schema(),
            output_schema: table.schema(),
            index: (0, 0),
            cluster_stats_gen: table.cluster_stats_gen(ctx)?,
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
            if let State::Output(part, chunk) = std::mem::replace(&mut self.state, State::Finish) {
                self.state = match part {
                    None => State::Finish,
                    Some(part) => State::ReadData(Some(part)),
                };

                self.output.push_data(Ok(chunk));
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
                let chunk = self.block_reader.deserialize(part.clone(), chunks)?;

                let evaluator =
                    Evaluator::new(&chunk, FunctionContext::default(), &BUILTIN_FUNCTIONS);
                let expr = self
                    .filter
                    .into_expr(&BUILTIN_FUNCTIONS)
                    .unwrap()
                    .project_column_ref(|name| self.source_schema.index_of(name).unwrap());
                let res = evaluator.run(&expr).map_err(|(_, e)| {
                    ErrorCode::Internal(format!("eval try eval const failed: {}.", e))
                })?;
                let predicates =
                    Chunk::<String>::cast_to_nonull_boolean(&res).ok_or_else(|| {
                        ErrorCode::BadArguments(
                            "Result of filter expression cannot be converted to boolean.",
                        )
                    })?;

                let predicate_col = predicates.into_column().unwrap();
                let filter = Value::Column(Column::Boolean(predicate_col.not()));
                if !Chunk::<usize>::filter_exists(&filter)? {
                    // all the rows should be removed.
                    self.state = State::Generated(Deletion::Deleted);
                } else {
                    let num_rows = chunk.num_rows();
                    let chunk = chunk.filter(&filter)?;
                    if chunk.num_rows() == num_rows {
                        // none of the rows should be removed.
                        self.state = State::Generated(Deletion::DoNothing);
                    } else if self.remain_reader.is_none() {
                        let src_schema = self.block_reader.data_schema();
                        let dest_schema = self.output_schema.clone().into();
                        let chunk = chunk.resort(&src_schema, &dest_schema)?;
                        self.state = State::NeedSerialize(chunk);
                    } else {
                        self.state = State::ReadRemain {
                            part,
                            chunk,
                            filter,
                        }
                    }
                }
            }
            State::MergeRemain {
                part,
                chunks,
                mut chunk,
                filter,
            } => {
                let mut fields = self.block_reader.data_fields();
                let merged = if chunks.is_empty() {
                    chunk
                } else if let Some(remain_reader) = self.remain_reader.as_ref() {
                    let mut remain_fields = remain_reader.data_fields();
                    fields.append(&mut remain_fields);
                    let remain_chunk = remain_reader.deserialize(part, chunks)?;
                    let remain_chunk = remain_chunk.filter(&filter)?;
                    for col in remain_chunk.columns() {
                        chunk.add_column(col.clone());
                    }
                    chunk
                } else {
                    return Err(ErrorCode::Internal("It's a bug. Need remain reader"));
                };

                let src_schema = DataSchema::new(fields);
                let dest_schema = self.output_schema.clone().into();
                let chunk = merged.resort(&src_schema, &dest_schema)?;
                self.state = State::NeedSerialize(chunk);
            }
            State::NeedSerialize(chunk) => {
                let cluster_stats = self
                    .cluster_stats_gen
                    .gen_with_origin_stats(&chunk, std::mem::take(&mut self.origin_stats))?;

                let row_count = chunk.num_rows() as u64;
                let chunk_size = chunk.memory_size() as u64;
                let (chunk_location, chunk_id) = self.location_gen.gen_block_location();

                // build block index.
                let location = self.location_gen.block_bloom_index_location(&chunk_id);
                let (bloom_index_state, column_distinct_count) =
                    BloomIndexState::try_create(self.source_schema.clone(), &chunk, location)?;
                let col_stats = gen_columns_statistics(&chunk, Some(column_distinct_count))?;

                // serialize data block.
                let mut block_data = Vec::with_capacity(100 * 1024 * 1024);
                let schema = self.source_schema.clone();
                let (file_size, meta_data) =
                    serialize_to_parquet(vec![chunk], &schema, &mut block_data)?;
                let col_metas = util::column_metas(&meta_data)?;

                // new block meta.
                let new_meta = Arc::new(BlockMeta::new(
                    row_count,
                    chunk_size,
                    file_size,
                    col_stats,
                    col_metas,
                    cluster_stats,
                    chunk_location.clone(),
                    Some(bloom_index_state.location.clone()),
                    bloom_index_state.size,
                ));

                self.state = State::Serialized(
                    SerializeState {
                        block_data,
                        block_location: chunk_location.0,
                        index_data: bloom_index_state.data,
                        index_location: bloom_index_state.location.0,
                    },
                    new_meta,
                );
            }
            State::Generated(op) => {
                let meta = DeletionSourceMeta::create(self.index, op);
                let new_part = self.ctx.try_get_part();
                self.state = State::Output(new_part, Chunk::empty_with_meta(meta));
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
                let chunks = self
                    .block_reader
                    .read_columns_data(self.ctx.clone(), part.clone())
                    .await?;
                self.state = State::FilterData(part, chunks);
            }
            State::ReadRemain {
                part,
                chunk,
                filter,
            } => {
                if let Some(remain_reader) = self.remain_reader.as_ref() {
                    let chunks = remain_reader
                        .read_columns_data(self.ctx.clone(), part.clone())
                        .await?;
                    self.state = State::MergeRemain {
                        part,
                        chunks,
                        chunk,
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
