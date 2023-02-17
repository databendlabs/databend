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
use std::ops::Not;
use std::sync::Arc;

use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::filter_helper::FilterHelpers;
use common_expression::types::AnyType;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::Value;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_sql::evaluator::BlockOperator;
use storages_common_pruner::BlockMetaIndex;
use storages_common_table_meta::meta::ClusterStatistics;

use crate::fuse_part::FusePartInfo;
use crate::io::BlockReader;
use crate::io::ReadSettings;
use crate::operations::mutation::MutationPartInfo;
use crate::operations::mutation::SerializeDataMeta;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;
use crate::MergeIOReadResult;

pub enum MutationAction {
    Deletion,
    Update,
}

enum State {
    ReadData(Option<PartInfoPtr>),
    FilterData(PartInfoPtr, MergeIOReadResult),
    ReadRemain {
        part: PartInfoPtr,
        data_block: DataBlock,
        filter: Value<AnyType>,
    },
    MergeRemain {
        part: PartInfoPtr,
        merged_io_read_result: MergeIOReadResult,
        data_block: DataBlock,
        filter: Value<AnyType>,
    },
    PerformOperator(DataBlock),
    Output(Option<PartInfoPtr>, DataBlock),
    Finish,
}

pub struct MutationSource {
    state: State,
    output: Arc<OutputPort>,
    scan_progress: Arc<Progress>,

    ctx: Arc<dyn TableContext>,
    filter: Arc<Option<Expr>>,
    block_reader: Arc<BlockReader>,
    remain_reader: Arc<Option<BlockReader>>,
    operators: Vec<BlockOperator>,
    action: MutationAction,

    index: BlockMetaIndex,
    origin_stats: Option<ClusterStatistics>,
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
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        Ok(ProcessorPtr::create(Box::new(MutationSource {
            state: State::ReadData(None),
            output,
            scan_progress,
            ctx: ctx.clone(),
            filter,
            block_reader,
            remain_reader,
            operators,
            action,
            index: BlockMetaIndex::default(),
            origin_stats: None,
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
            self.state = match self.ctx.get_partition() {
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
            State::FilterData(part, read_res) => {
                let chunks = read_res.columns_chunks()?;
                let mut data_block = self
                    .block_reader
                    .deserialize_parquet_chunks(part.clone(), chunks)?;
                let num_rows = data_block.num_rows();

                if let Some(filter) = self.filter.as_ref() {
                    let func_ctx = self.ctx.get_function_context()?;
                    let evaluator = Evaluator::new(&data_block, func_ctx, &BUILTIN_FUNCTIONS);

                    let res = evaluator
                        .run(filter)
                        .map_err(|e| e.add_message("eval filter failed:"))?;
                    let predicates =
                        FilterHelpers::cast_to_nonull_boolean(&res).ok_or_else(|| {
                            ErrorCode::BadArguments(
                                "Result of filter expression cannot be converted to boolean.",
                            )
                        })?;

                    let affect_rows = match &predicates {
                        Value::Scalar(v) => {
                            if *v {
                                num_rows
                            } else {
                                0
                            }
                        }
                        Value::Column(bitmap) => bitmap.len() - bitmap.unset_bits(),
                    };

                    if affect_rows != 0 {
                        let progress_values = ProgressValues {
                            rows: affect_rows,
                            bytes: 0,
                        };
                        self.scan_progress.incr(&progress_values);

                        match self.action {
                            MutationAction::Deletion => {
                                if affect_rows == num_rows {
                                    // all the rows should be removed.
                                    let meta = SerializeDataMeta::create(
                                        self.index.clone(),
                                        self.origin_stats.clone(),
                                    );
                                    self.state = State::Output(
                                        self.ctx.get_partition(),
                                        DataBlock::empty_with_meta(meta),
                                    );
                                } else {
                                    let predicate_col = predicates.into_column().unwrap();
                                    let filter =
                                        Value::Column(Column::Boolean(predicate_col.not()));
                                    data_block = data_block.filter(&filter)?;
                                    if self.remain_reader.is_none() {
                                        self.state = State::PerformOperator(data_block);
                                    } else {
                                        self.state = State::ReadRemain {
                                            part,
                                            data_block,
                                            filter,
                                        }
                                    }
                                }
                            }
                            MutationAction::Update => {
                                let filter = Value::upcast(predicates);
                                if self.remain_reader.is_none() {
                                    data_block.add_column(BlockEntry {
                                        data_type: DataType::Boolean,
                                        value: filter,
                                    });
                                    self.state = State::PerformOperator(data_block);
                                } else {
                                    self.state = State::ReadRemain {
                                        part,
                                        data_block,
                                        filter,
                                    };
                                }
                            }
                        }
                    } else {
                        // Do nothing.
                        self.state = State::Output(self.ctx.get_partition(), DataBlock::empty());
                    }
                } else {
                    let progress_values = ProgressValues {
                        rows: num_rows,
                        bytes: 0,
                    };
                    self.scan_progress.incr(&progress_values);
                    self.state = State::PerformOperator(data_block);
                }
            }
            State::MergeRemain {
                part,
                merged_io_read_result,
                mut data_block,
                filter,
            } => {
                if let Some(remain_reader) = self.remain_reader.as_ref() {
                    let chunks = merged_io_read_result.columns_chunks()?;
                    let remain_block = remain_reader.deserialize_parquet_chunks(part, chunks)?;

                    match self.action {
                        MutationAction::Deletion => {
                            let remain_block = remain_block.filter(&filter)?;
                            for col in remain_block.columns() {
                                data_block.add_column(col.clone());
                            }
                        }
                        MutationAction::Update => {
                            for col in remain_block.columns() {
                                data_block.add_column(col.clone());
                            }
                            data_block.add_column(BlockEntry {
                                data_type: DataType::Boolean,
                                value: filter,
                            });
                        }
                    }
                } else {
                    return Err(ErrorCode::Internal("It's a bug. Need remain reader"));
                };

                self.state = State::PerformOperator(data_block);
            }
            State::PerformOperator(data_block) => {
                let func_ctx = self.ctx.get_function_context()?;
                let block = self
                    .operators
                    .iter()
                    .try_fold(data_block, |input, op| op.execute(&func_ctx, input))?;
                let meta = SerializeDataMeta::create(self.index.clone(), self.origin_stats.clone());
                self.state = State::Output(self.ctx.get_partition(), block.add_meta(Some(meta))?);
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
                self.index = part.index.clone();
                self.origin_stats = part.cluster_stats.clone();
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
                self.state = State::FilterData(inner_part, read_res);
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
                    self.state = State::MergeRemain {
                        part,
                        merged_io_read_result: read_res,
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
