//  Copyright 2023 Datafuse Labs.
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
use std::ops::BitAnd;
use std::ops::Not;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_base::base::tokio;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::Value;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_io::constants::DEFAULT_BLOCK_DELETE_MARK_SIZE;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use opendal::Operator;
use storages_common_blocks::blocks_to_parquet;
use storages_common_pruner::BlockMetaIndex;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::table::TableCompression;

use crate::fuse_part::FusePartInfo;
use crate::io::write_data;
use crate::io::BlockReader;
use crate::io::DeleteMarkReader;
use crate::io::ReadSettings;
use crate::io::TableMetaLocationGenerator;
use crate::operations::mutation::refactor::Mutation;
use crate::operations::mutation::refactor::MutationPartInfo;
use crate::operations::mutation::refactor::MutationSourceMeta;
use crate::FuseStorageFormat;
use crate::FuseTable;
use crate::MergeIOReadResult;

enum State {
    ReadData,
    ReadMark(ReadDataInfo),
    FilterData(Option<Arc<Bitmap>>, ReadDataInfo),
    SerializeMark {
        block_index: BlockMetaIndex,
        location: Location,
        size: u64,
        data: Vec<u8>,
    },
    Output(BlockMetaIndex, Mutation),
    Finish,
}

struct ReadDataInfo {
    part: PartInfoPtr,
    index: BlockMetaIndex,
    chunk: MergeIOReadResult,
}

pub struct MutationSource {
    state: State,
    block_reader: Arc<BlockReader>,
    location_gen: TableMetaLocationGenerator,
    dal: Operator,
    ctx: Arc<dyn TableContext>,
    filter: Arc<Expr>,
    storage_format: FuseStorageFormat,

    output: Arc<OutputPort>,

    batch_size: usize,
    read_datas: Vec<ReadDataInfo>,
}

impl MutationSource {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        table: &FuseTable,
        filter: Arc<Expr>,
        block_reader: Arc<BlockReader>,
        storage_format: FuseStorageFormat,
    ) -> Result<ProcessorPtr> {
        let batch_size = ctx.get_settings().get_storage_fetch_part_num()? as usize;
        Ok(ProcessorPtr::create(Box::new(MutationSource {
            state: State::ReadData,
            block_reader,
            location_gen: table.meta_location_generator().clone(),
            dal: table.get_operator(),
            ctx: ctx.clone(),
            filter,
            output,
            batch_size,
            read_datas: vec![],
            storage_format,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for MutationSource {
    fn name(&self) -> String {
        String::from("DeserializeDataTransform")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
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
            if let State::Output(index, op) = std::mem::replace(&mut self.state, State::Finish) {
                self.state = if let Some(data) = self.read_datas.pop() {
                    State::ReadMark(data)
                } else {
                    State::ReadData
                };
                let meta = MutationSourceMeta::create(index, op);
                self.output.push_data(Ok(DataBlock::empty_with_meta(meta)));
                return Ok(Event::NeedConsume);
            }
        }

        if matches!(self.state, State::FilterData(..)) {
            return Ok(Event::Sync);
        }
        Ok(Event::Async)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::FilterData(mark, ReadDataInfo { part, index, chunk }) => {
                let chunks = chunk.columns_chunks()?;
                let mut data_block = self.block_reader.deserialize_chunks(
                    part.clone(),
                    chunks,
                    &self.storage_format,
                )?;

                let num_rows = data_block.num_rows();
                let (num_exists_rows, entry) = mark.as_deref().map_or(
                    (num_rows, BlockEntry {
                        data_type: DataType::Boolean,
                        value: Value::Scalar(Scalar::Boolean(true)),
                    }),
                    |v| {
                        (num_rows - v.unset_bits(), BlockEntry {
                            data_type: DataType::Boolean,
                            value: Value::Column(Column::Boolean(v.clone())),
                        })
                    },
                );
                data_block.add_column(entry);

                let func_ctx = self.ctx.get_function_context()?;
                let evaluator = Evaluator::new(&data_block, func_ctx, &BUILTIN_FUNCTIONS);

                let predicates = evaluator
                    .run(&self.filter)
                    .map_err(|e| e.add_message("eval filter failed:"))?
                    .try_downcast::<BooleanType>()
                    .unwrap();

                let affect_rows = match &predicates {
                    Value::Scalar(v) => {
                        if *v {
                            num_exists_rows
                        } else {
                            0
                        }
                    }
                    Value::Column(bitmap) => bitmap.len() - bitmap.unset_bits(),
                };

                if affect_rows == 0 {
                    self.state = State::Output(index, Mutation::DoNothing);
                } else if affect_rows == num_exists_rows {
                    // all removed.
                    self.state = State::Output(index, Mutation::Deleted);
                } else {
                    // build delete mark file.
                    let location = self.location_gen.gen_delete_mark_location();

                    let filter_res = predicates.into_column().unwrap().not();
                    let res = mark.map_or(filter_res.clone(), |v| v.bitand(&filter_res));
                    let mark_block = DataBlock::new(
                        vec![BlockEntry {
                            data_type: DataType::Boolean,
                            value: Value::Column(Column::Boolean(res)),
                        }],
                        num_rows,
                    );
                    let mut data = Vec::with_capacity(DEFAULT_BLOCK_DELETE_MARK_SIZE);
                    let mark_schema = Arc::new(TableSchema::new(vec![TableField::new(
                        "_row_exists",
                        TableDataType::Boolean,
                    )]));
                    let (size, _) = blocks_to_parquet(
                        mark_schema,
                        vec![mark_block],
                        &mut data,
                        TableCompression::None,
                    )?;
                    self.state = State::SerializeMark {
                        block_index: index,
                        location,
                        size,
                        data,
                    }
                }
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadData => {
                let parts = self.ctx.get_partitions(self.batch_size);
                if !parts.is_empty() {
                    let mut chunks = Vec::with_capacity(parts.len());
                    let mut part_indices = Vec::with_capacity(parts.len());

                    for part in &parts {
                        let part = MutationPartInfo::from_part(part)?;
                        let inner_part = part.inner_part.clone();

                        part_indices.push((inner_part.clone(), part.index.clone()));

                        let block_reader = self.block_reader.clone();
                        let settings = ReadSettings::from_ctx(&self.ctx)?;
                        chunks.push(async move {
                            tokio::spawn(async move {
                                let fuse_part = FusePartInfo::from_part(&inner_part)?;
                                block_reader
                                    .read_columns_data_by_merge_io(
                                        &settings,
                                        &fuse_part.location,
                                        &fuse_part.columns_meta,
                                    )
                                    .await
                            })
                            .await
                            .unwrap()
                        });
                    }
                    let chunks = futures::future::try_join_all(chunks).await?;
                    self.read_datas = chunks.into_iter().zip(part_indices.into_iter()).fold(
                        Vec::with_capacity(parts.len()),
                        |mut acc, (chunk, (part, index))| {
                            acc.push(ReadDataInfo { part, index, chunk });
                            acc
                        },
                    );
                    self.state = State::ReadMark(self.read_datas.pop().unwrap());
                }
            }
            State::ReadMark(read_res) => {
                let fuse_part = FusePartInfo::from_part(&read_res.part)?;
                let mark = if let Some((location, length)) = &fuse_part.delete_mark {
                    let mark = location
                        .read_delete_mark(self.dal.clone(), *length, fuse_part.nums_rows)
                        .await?;
                    Some(mark)
                } else {
                    None
                };
                self.state = State::FilterData(mark, read_res);
            }
            State::SerializeMark {
                block_index,
                location,
                size,
                data,
            } => {
                write_data(&data, &self.dal, &location.0).await?;
                self.state = State::Output(block_index, Mutation::Replaced(location, size));
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
