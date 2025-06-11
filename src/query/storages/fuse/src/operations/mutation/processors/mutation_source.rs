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
use std::ops::Not;
use std::sync::Arc;

use databend_common_base::base::ProgressValues;
use databend_common_catalog::plan::build_origin_block_row_num;
use databend_common_catalog::plan::gen_mutation_stream_meta;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_storage::MutationStatus;
use databend_storages_common_io::ReadSettings;

use crate::fuse_part::FuseBlockPartInfo;
use crate::io::BlockReader;
use crate::operations::common::BlockMetaIndex;
use crate::operations::mutation::ClusterStatsGenType;
use crate::operations::mutation::Mutation;
use crate::operations::mutation::SerializeBlock;
use crate::operations::mutation::SerializeDataMeta;
use crate::BlockReadResult;
use crate::FuseStorageFormat;

#[derive(Debug, Clone, PartialEq)]
pub enum MutationAction {
    Deletion,
    Update,
}

enum State {
    ReadData(Option<PartInfoPtr>),
    FilterData(PartInfoPtr, BlockReadResult),
    ReadRemain {
        part: PartInfoPtr,
        data_block: DataBlock,
        filter: Option<Value<BooleanType>>,
    },
    MergeRemain {
        part: PartInfoPtr,
        block_read_result: BlockReadResult,
        data_block: DataBlock,
        filter: Option<Value<BooleanType>>,
    },
    PerformOperator(DataBlock, String),
    Output(Option<PartInfoPtr>, DataBlock),
    Finish,
}

pub struct MutationSource {
    state: State,
    output: Arc<OutputPort>,

    ctx: Arc<dyn TableContext>,
    filter: Arc<Option<Expr>>,
    block_reader: Arc<BlockReader>,
    remain_reader: Arc<Option<BlockReader>>,
    operators: Vec<BlockOperator>,
    storage_format: FuseStorageFormat,
    action: MutationAction,

    index: BlockMetaIndex,
    stats_type: ClusterStatsGenType,
}

impl MutationSource {
    #![allow(clippy::too_many_arguments)]
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        action: MutationAction,
        output: Arc<OutputPort>,
        filter: Arc<Option<Expr>>,
        block_reader: Arc<BlockReader>,
        remain_reader: Arc<Option<BlockReader>>,
        operators: Vec<BlockOperator>,
        storage_format: FuseStorageFormat,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(MutationSource {
            state: State::ReadData(None),
            output,
            ctx: ctx.clone(),
            filter,
            block_reader,
            remain_reader,
            operators,
            storage_format,
            action,
            index: BlockMetaIndex::default(),
            stats_type: ClusterStatsGenType::Generally,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for MutationSource {
    fn name(&self) -> String {
        "MutationSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::ReadData(None)) {
            self.state = self
                .ctx
                .get_partition()
                .map_or(State::Finish, |part| State::ReadData(Some(part)));
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
                self.state = part.map_or(State::Finish, |part| State::ReadData(Some(part)));

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
            State::FilterData(part, read_res) => {
                let chunks = read_res.columns_chunks()?;
                let mut data_block = self.block_reader.deserialize_chunks_with_part_info(
                    part.clone(),
                    chunks,
                    &self.storage_format,
                )?;
                let rows = data_block.num_rows();
                self.ctx.get_scan_progress().incr(&ProgressValues {
                    rows,
                    bytes: data_block.memory_size(),
                });

                let fuse_part = FuseBlockPartInfo::from_part(&part)?;
                if let Some(filter) = self.filter.as_ref() {
                    assert_eq!(filter.data_type(), &DataType::Boolean);

                    let func_ctx = self.ctx.get_function_context()?;
                    let evaluator = Evaluator::new(&data_block, &func_ctx, &BUILTIN_FUNCTIONS);

                    let predicates = evaluator
                        .run(filter)
                        .map_err(|e| e.add_message("eval filter failed:"))?
                        .try_downcast::<BooleanType>()
                        .unwrap();

                    let affect_rows = match &predicates {
                        Value::Scalar(v) => {
                            if *v {
                                rows
                            } else {
                                0
                            }
                        }
                        Value::Column(bitmap) => bitmap.len() - bitmap.null_count(),
                    };

                    if affect_rows != 0 {
                        self.update_mutation_status(affect_rows);

                        match self.action {
                            MutationAction::Deletion => {
                                if affect_rows == rows {
                                    // all the rows should be removed.
                                    let meta = Box::new(SerializeDataMeta::SerializeBlock(
                                        SerializeBlock::create(
                                            self.index.clone(),
                                            self.stats_type.clone(),
                                        ),
                                    ));
                                    self.state = State::Output(
                                        self.ctx.get_partition(),
                                        DataBlock::empty_with_meta(meta),
                                    );
                                } else {
                                    if self.block_reader.update_stream_columns {
                                        let row_num = build_origin_block_row_num(rows);
                                        data_block.add_column(row_num);
                                    }

                                    let predicate_col = predicates.into_column().unwrap();
                                    let filter = predicate_col.not();
                                    data_block = data_block.filter_with_bitmap(&filter)?;
                                    if self.remain_reader.is_none() {
                                        self.state = State::PerformOperator(
                                            data_block,
                                            fuse_part.location.clone(),
                                        );
                                    } else {
                                        self.state = State::ReadRemain {
                                            part,
                                            data_block,
                                            filter: Some(Value::Column(filter)),
                                        }
                                    }
                                }
                            }

                            MutationAction::Update => {
                                data_block.add_column(BlockEntry::from_arg_value(predicates));
                                if self.remain_reader.is_none() {
                                    self.state = State::PerformOperator(
                                        data_block,
                                        fuse_part.location.clone(),
                                    );
                                } else {
                                    self.state = State::ReadRemain {
                                        part,
                                        data_block,
                                        filter: None,
                                    };
                                }
                            }
                        }
                    } else {
                        // Do nothing.
                        self.state = State::Output(self.ctx.get_partition(), DataBlock::empty());
                    }
                } else {
                    self.update_mutation_status(rows);
                    self.state = State::PerformOperator(data_block, fuse_part.location.clone());
                }
            }
            State::MergeRemain {
                part,
                block_read_result,
                mut data_block,
                filter,
            } => {
                let path = FuseBlockPartInfo::from_part(&part)?.location.clone();
                if let Some(remain_reader) = self.remain_reader.as_ref() {
                    let chunks = block_read_result.columns_chunks()?;
                    let remain_block = remain_reader.deserialize_chunks_with_part_info(
                        part,
                        chunks,
                        &self.storage_format,
                    )?;

                    // Record the remain memory size, rows has been recorded.
                    self.ctx.get_scan_progress().incr(&ProgressValues {
                        rows: 0,
                        bytes: remain_block.memory_size(),
                    });

                    let remain_block = if let Some(filter) = filter {
                        // for deletion.
                        remain_block.filter_boolean_value(&filter)?
                    } else {
                        remain_block
                    };

                    for col in remain_block.columns() {
                        data_block.add_column(col.clone());
                    }
                } else {
                    return Err(ErrorCode::Internal("It's a bug. Need remain reader"));
                };

                self.state = State::PerformOperator(data_block, path);
            }
            State::PerformOperator(data_block, path) => {
                let func_ctx = self.ctx.get_function_context()?;
                let block = self
                    .operators
                    .iter()
                    .try_fold(data_block, |input, op| op.execute(&func_ctx, input))?;
                let inner_meta = Box::new(SerializeDataMeta::SerializeBlock(
                    SerializeBlock::create(self.index.clone(), self.stats_type.clone()),
                ));
                let meta: BlockMetaInfoPtr = if self.block_reader.update_stream_columns() {
                    Box::new(gen_mutation_stream_meta(Some(inner_meta), &path)?)
                } else {
                    inner_meta
                };
                self.state = State::Output(self.ctx.get_partition(), block.add_meta(Some(meta))?);
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadData(Some(part)) => {
                let settings = ReadSettings::from_ctx(&self.ctx)?;
                match Mutation::from_part(&part)? {
                    Mutation::MutationDeletedSegment(deleted_segment) => {
                        self.update_mutation_status(deleted_segment.summary.row_count as usize);
                        self.state = State::Output(
                            self.ctx.get_partition(),
                            DataBlock::empty_with_meta(Box::new(
                                SerializeDataMeta::DeletedSegment(deleted_segment.clone()),
                            )),
                        )
                    }
                    Mutation::MutationPartInfo(part) => {
                        self.index = BlockMetaIndex {
                            segment_idx: part.index.segment_idx,
                            block_idx: part.index.block_idx,
                        };
                        if matches!(self.action, MutationAction::Deletion) {
                            self.stats_type =
                                ClusterStatsGenType::WithOrigin(part.cluster_stats.clone());
                        }

                        let inner_part = part.inner_part.clone();
                        let fuse_part = FuseBlockPartInfo::from_part(&inner_part)?;

                        if part.whole_block_mutation
                            && matches!(self.action, MutationAction::Deletion)
                        {
                            // whole block deletion.
                            self.update_mutation_status(fuse_part.nums_rows);
                            let meta = Box::new(SerializeDataMeta::SerializeBlock(
                                SerializeBlock::create(self.index.clone(), self.stats_type.clone()),
                            ));
                            self.state = State::Output(
                                self.ctx.get_partition(),
                                DataBlock::empty_with_meta(meta),
                            );
                        } else {
                            let read_res = self
                                .block_reader
                                .read_columns_data_by_merge_io(
                                    &settings,
                                    &fuse_part.location,
                                    &fuse_part.columns_meta,
                                    &None,
                                )
                                .await?;
                            self.state = State::FilterData(inner_part, read_res);
                        }
                    }
                }
            }
            State::ReadRemain {
                part,
                data_block,
                filter,
            } => {
                if let Some(remain_reader) = self.remain_reader.as_ref() {
                    let fuse_part = FuseBlockPartInfo::from_part(&part)?;

                    let settings = ReadSettings::from_ctx(&self.ctx)?;
                    let read_res = remain_reader
                        .read_columns_data_by_merge_io(
                            &settings,
                            &fuse_part.location,
                            &fuse_part.columns_meta,
                            &None,
                        )
                        .await?;
                    self.state = State::MergeRemain {
                        part,
                        block_read_result: read_res,
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

impl MutationSource {
    fn update_mutation_status(&self, num_rows: usize) {
        let (update_rows, deleted_rows) = if self.action == MutationAction::Update {
            (num_rows as u64, 0)
        } else {
            (0, num_rows as u64)
        };
        self.ctx.add_mutation_status(MutationStatus {
            insert_rows: 0,
            update_rows,
            deleted_rows,
        });
    }
}
