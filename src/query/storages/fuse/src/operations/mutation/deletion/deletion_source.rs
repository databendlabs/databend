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
use common_expression::types::AnyType;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::TableSchemaRef;
use common_expression::Value;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_storages_table_meta::meta::ClusterStatistics;

use crate::fuse_part::FusePartInfo;
use crate::io::BlockReader;
use crate::io::ReadSettings;
use crate::operations::mutation::DataChunks;
use crate::operations::mutation::MutationPartInfo;
use crate::operations::mutation::SerializeDataMeta;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;
use crate::pruning::BlockIndex;

enum State {
    ReadData(Option<PartInfoPtr>),
    FilterData(PartInfoPtr, DataChunks),
    ReadRemain {
        part: PartInfoPtr,
        data_block: DataBlock,
        filter: Value<AnyType>,
    },
    MergeRemain {
        part: PartInfoPtr,
        chunks: DataChunks,
        data_block: DataBlock,
        filter: Value<AnyType>,
    },
    Output(Option<PartInfoPtr>, DataBlock),
    Finish,
}

pub struct DeletionSource {
    state: State,
    output: Arc<OutputPort>,

    ctx: Arc<dyn TableContext>,
    filter: Arc<Expr>,
    block_reader: Arc<BlockReader>,
    remain_reader: Arc<Option<BlockReader>>,

    output_schema: TableSchemaRef,
    index: BlockIndex,
    origin_stats: Option<ClusterStatistics>,
}

impl DeletionSource {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        filter: Arc<Expr>,
        block_reader: Arc<BlockReader>,
        remain_reader: Arc<Option<BlockReader>>,
        output_schema: TableSchemaRef,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(DeletionSource {
            state: State::ReadData(None),
            output,
            ctx: ctx.clone(),
            filter,
            block_reader,
            remain_reader,
            output_schema,
            index: (0, 0),
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

        if matches!(self.state, State::ReadData(_) | State::ReadRemain { .. }) {
            Ok(Event::Async)
        } else {
            Ok(Event::Sync)
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::FilterData(part, chunks) => {
                let mut data_block = self
                    .block_reader
                    .deserialize_parquet_chunks(part.clone(), chunks)?;

                let func_ctx = self.ctx.try_get_function_context()?;
                let evaluator = Evaluator::new(&data_block, func_ctx, &BUILTIN_FUNCTIONS);

                let res = evaluator
                    .run(&self.filter)
                    .map_err(|(_, e)| ErrorCode::Internal(format!("eval filter failed: {}.", e)))?;
                let predicates = DataBlock::cast_to_nonull_boolean(&res).ok_or_else(|| {
                    ErrorCode::BadArguments(
                        "Result of filter expression cannot be converted to boolean.",
                    )
                })?;

                let predicate_col = predicates.into_column().unwrap();
                let filter = Value::Column(Column::Boolean(predicate_col.not()));
                if !DataBlock::filter_exists(&filter)? {
                    // all the rows should be removed.
                    let meta = SerializeDataMeta::create(self.index, self.origin_stats);
                    self.state =
                        State::Output(self.ctx.try_get_part(), DataBlock::empty_with_meta(meta));
                } else {
                    let num_rows = data_block.num_rows();
                    data_block = data_block.filter(&filter)?;
                    if data_block.num_rows() == num_rows {
                        // none of the rows should be removed.
                        self.state = State::Output(self.ctx.try_get_part(), DataBlock::empty());
                    } else if self.remain_reader.is_none() {
                        let meta = SerializeDataMeta::create(self.index, self.origin_stats);
                        self.state = State::Output(
                            self.ctx.try_get_part(),
                            data_block.add_meta(Some(meta))?,
                        );
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
                let mut fields = self.block_reader.data_fields();
                let merged = if chunks.is_empty() {
                    data_block
                } else if let Some(remain_reader) = self.remain_reader.as_ref() {
                    let mut remain_fields = remain_reader.data_fields();
                    fields.append(&mut remain_fields);
                    let remain_block = remain_reader.deserialize_parquet_chunks(part, chunks)?;
                    let remain_block = remain_block.filter(&filter)?;
                    for col in remain_block.columns() {
                        data_block.add_column(col.clone());
                    }
                    data_block
                } else {
                    return Err(ErrorCode::Internal("It's a bug. Need remain reader"));
                };

                let src_schema = DataSchema::new(fields);
                let dest_schema = self.output_schema.clone().into();
                let block = merged.resort(&src_schema, &dest_schema)?;

                let meta = SerializeDataMeta::create(self.index, self.origin_stats);
                self.state = State::Output(self.ctx.try_get_part(), block.add_meta(Some(meta))?);
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadData(Some(part)) => {
                let settings = ReadSettings::from_ctx(&self.ctx)?;
                let deletion_part = MutationPartInfo::from_part(&part)?;
                self.index = deletion_part.index;
                self.origin_stats = deletion_part.cluster_stats.clone();
                let part = deletion_part.inner_part.clone();
                let fuse_part = FusePartInfo::from_part(&part)?;

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

                self.state = State::FilterData(part, chunks);
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
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
