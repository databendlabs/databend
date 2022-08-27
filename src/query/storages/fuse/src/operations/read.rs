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
use std::sync::Arc;

use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::BooleanColumn;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::Series;
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_core::Pipeline;
use common_pipeline_core::SourcePipeBuilder;
use common_pipeline_transforms::processors::ExpressionExecutor;
use common_planners::Extras;
use common_planners::PartInfoPtr;
use common_planners::PrewhereInfo;
use common_planners::Projection;
use common_planners::ReadDataSourcePlan;

use crate::io::BlockReader;
use crate::operations::read::State::Generated;
use crate::FuseTable;

impl FuseTable {
    pub fn create_block_reader(
        &self,
        ctx: &Arc<dyn TableContext>,
        projection: Projection,
    ) -> Result<Arc<BlockReader>> {
        let operator = ctx.get_storage_operator()?;
        let table_schema = self.table_info.schema();
        BlockReader::create(operator, table_schema, projection)
    }

    pub fn projection_of_push_downs(&self, push_downs: &Option<Extras>) -> Projection {
        if let Some(Extras {
            projection: Some(prj),
            ..
        }) = push_downs
        {
            prj.clone()
        } else {
            let indices = (0..self.table_info.schema().fields().len())
                .into_iter()
                .collect::<Vec<usize>>();
            Projection::Columns(indices)
        }
    }

    pub fn prewhere_of_push_downs(&self, push_downs: &Option<Extras>) -> Option<PrewhereInfo> {
        if let Some(Extras { prewhere, .. }) = push_downs {
            prewhere.clone()
        } else {
            None
        }
    }

    #[inline]
    pub fn do_read2(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &ReadDataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let projection = self.projection_of_push_downs(&plan.push_downs);
        let prewhere_info = if let Some(prewhere) = self.prewhere_of_push_downs(&plan.push_downs) {
            let schema = self.table_info.schema();
            let expr_field = prewhere.filter.to_data_field(&schema)?;
            let expr_schema = DataSchemaRefExt::create(vec![expr_field]);

            let executor = ExpressionExecutor::try_create(
                ctx.clone(),
                "filter expression executor (prewhere) ",
                schema, // in fact, this field is not used
                expr_schema,
                vec![prewhere.filter.clone()],
                false,
            )?;

            let need_columns_block_reader =
                self.create_block_reader(&ctx, prewhere.need_columns.clone())?;
            let remain_columns_block_reader =
                self.create_block_reader(&ctx, prewhere.remain_columns.clone())?;

            Some(FusePrewhereInfo {
                filter: Arc::new(executor),
                need_columns_block_reader,
                remain_columns_block_reader,
            })
        } else {
            None
        };

        let block_reader = self.create_block_reader(&ctx, projection)?;

        let parts_len = plan.parts.len();
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_threads = std::cmp::min(parts_len, max_threads);

        let mut source_builder = SourcePipeBuilder::create();
        let prewhere_info = Arc::new(prewhere_info);

        for _index in 0..std::cmp::max(1, max_threads) {
            let output = OutputPort::create();
            source_builder.add_source(
                output.clone(),
                FuseTableSource::create(
                    ctx.clone(),
                    output,
                    block_reader.clone(),
                    prewhere_info.clone(),
                )?,
            );
        }

        pipeline.add_pipe(source_builder.finalize());
        Ok(())
    }
}

/// PrewhereExtraData is used for operations after prewhere filter
type PrewhereExtraData = (DataBlock, BooleanColumn);

enum State {
    PrewhereReadData(PartInfoPtr),
    PrewhereFilter(PartInfoPtr, DataBlock),
    ReadData(PartInfoPtr, Option<PrewhereExtraData>),
    Deserialize(
        PartInfoPtr,
        Vec<(usize, Vec<u8>)>,
        bool, // true: before prewhere filter, false: after prewhere filter
        Option<PrewhereExtraData>,
    ),
    Generated(Option<PartInfoPtr>, DataBlock),
    Finish,
}

#[derive(Clone)]
struct FusePrewhereInfo {
    need_columns_block_reader: Arc<BlockReader>,
    remain_columns_block_reader: Arc<BlockReader>,
    filter: Arc<ExpressionExecutor>,
}

struct FuseTableSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    scan_progress: Arc<Progress>,
    block_reader: Arc<BlockReader>,
    output: Arc<OutputPort>,
    prewhere_info: Arc<Option<FusePrewhereInfo>>,
}

impl FuseTableSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
        prewhere_info: Arc<Option<FusePrewhereInfo>>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        let mut partitions = ctx.try_get_partitions(1)?;
        match partitions.is_empty() {
            true => Ok(ProcessorPtr::create(Box::new(FuseTableSource {
                ctx,
                output,
                block_reader,
                scan_progress,
                state: State::Finish,
                prewhere_info,
            }))),
            false => Ok(ProcessorPtr::create(Box::new(FuseTableSource {
                ctx,
                output,
                block_reader,
                scan_progress,
                state: State::PrewhereReadData(partitions.remove(0)),
                prewhere_info,
            }))),
        }
    }
}

#[async_trait::async_trait]
impl Processor for FuseTableSource {
    fn name(&self) -> &'static str {
        "FuseEngineSource"
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

        if matches!(self.state, State::Generated(_, _)) {
            if let Generated(part, data_block) = std::mem::replace(&mut self.state, State::Finish) {
                self.state = match part {
                    None => State::Finish,
                    Some(part) => State::PrewhereReadData(part),
                };

                self.output.push_data(Ok(data_block));
                return Ok(Event::NeedConsume);
            }
        }

        match self.state {
            State::Finish => Ok(Event::Finished),
            State::PrewhereReadData(_) => Ok(Event::Async),
            State::PrewhereFilter(_, _) => Ok(Event::Sync),
            State::ReadData(_, _) => Ok(Event::Async),
            State::Deserialize(_, _, _, _) => Ok(Event::Sync),
            State::Generated(_, _) => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::Deserialize(part, chunks, before_prewhere_filter, extra) => {
                if before_prewhere_filter {
                    // before prewhere filter
                    if let Some(prewhere) = self.prewhere_info.as_ref() {
                        let data_block = prewhere
                            .need_columns_block_reader
                            .deserialize(part.clone(), chunks)?;
                        // the data deserialized is prewhere column data
                        // need to filter
                        self.state = State::PrewhereFilter(part, data_block);
                        Ok(())
                    } else {
                        Err(ErrorCode::LogicalError("It's a bug."))
                    }
                } else {
                    // after prewhere filter or no prewhere
                    let data_block = if let Some((prewhere_data_block, filter)) = extra {
                        // after prewhere and fetched remain columns
                        // need to merge two parts of columns
                        if let Some(prewhere) = self.prewhere_info.as_ref() {
                            let remain_data_block = prewhere
                                .remain_columns_block_reader
                                .deserialize(part, chunks)?;
                            let block = prewhere_data_block.append(remain_data_block)?;
                            DataBlock::filter_block_with_bool_column(block, &filter)?
                        } else {
                            return Err(ErrorCode::LogicalError("It's a bug."));
                        }
                    } else {
                        // no prewhere, return all columns directly
                        self.block_reader.deserialize(part, chunks)?
                    };

                    let mut partitions = self.ctx.try_get_partitions(1)?;

                    let progress_values = ProgressValues {
                        rows: data_block.num_rows(),
                        bytes: data_block.memory_size(),
                    };
                    self.scan_progress.incr(&progress_values);

                    self.state = match partitions.is_empty() {
                        true => State::Generated(None, data_block),
                        false => State::Generated(Some(partitions.remove(0)), data_block),
                    };
                    Ok(())
                }
            }
            State::PrewhereFilter(part, block) => {
                if let Some(prewhere) = self.prewhere_info.as_ref() {
                    let res = prewhere.filter.execute(&block)?;
                    let predicates = DataBlock::cast_to_nonull_boolean(res.column(0))?;
                    // shortcut, if predicates is const boolean (or can be cast to boolean)
                    if let Some(const_bool) = DataBlock::try_as_const_bool(&predicates)? {
                        if !const_bool {
                            // all rows in this block are filtered out
                            // turn to read next part
                            let mut partitions = self.ctx.try_get_partitions(1)?;
                            self.state = match partitions.is_empty() {
                                true => State::Generated(None, DataBlock::empty()),
                                false => {
                                    State::Generated(Some(partitions.remove(0)), DataBlock::empty())
                                }
                            };
                            return Ok(());
                        }
                    }

                    let boolean_col: &BooleanColumn = Series::check_get(&predicates)?;
                    let values = boolean_col.values().clone();
                    let filter = BooleanColumn::from_arrow_data(values);
                    self.state = State::ReadData(part, Some((block, filter)));
                    Ok(())
                } else {
                    Err(ErrorCode::LogicalError("It's a bug."))
                }
            }
            _ => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::PrewhereReadData(part) => {
                if let Some(prewhere) = self.prewhere_info.as_ref() {
                    let chunks = prewhere
                        .need_columns_block_reader
                        .read_columns_data(part.clone())
                        .await?;
                    self.state = State::Deserialize(part, chunks, true, None);
                } else {
                    // no need to do prewhere, read all data directly
                    self.state = State::ReadData(part, None);
                }
                Ok(())
            }
            State::ReadData(part, extra) => {
                if extra.is_none() {
                    // no extra prewhere info, read all columns
                    let chunks = self.block_reader.read_columns_data(part.clone()).await?;
                    self.state = State::Deserialize(part, chunks, false, None);
                    Ok(())
                } else {
                    // after prewhere filter, read remain columns
                    if let Some(prewhere) = self.prewhere_info.as_ref() {
                        let chunks = prewhere
                            .remain_columns_block_reader
                            .read_columns_data(part.clone())
                            .await?;
                        self.state = State::Deserialize(part, chunks, false, extra);
                        Ok(())
                    } else {
                        Err(ErrorCode::LogicalError("It's a bug."))
                    }
                }
            }
            _ => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }
}
