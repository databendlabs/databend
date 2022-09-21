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
use common_datavalues::ColumnRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_legacy_planners::Extras;
use common_legacy_planners::PartInfoPtr;
use common_legacy_planners::PrewhereInfo;
use common_legacy_planners::Projection;
use common_legacy_planners::ReadDataSourcePlan;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_core::Pipeline;
use common_pipeline_core::SourcePipeBuilder;
use common_pipeline_transforms::processors::ExpressionExecutor;
use common_storage::StorageParams;

use crate::io::BlockReader;
use crate::operations::read::State::Generated;
use crate::FuseTable;

impl FuseTable {
    pub fn create_block_reader(
        &self,
        ctx: &Arc<dyn TableContext>,
        projection: Projection,
    ) -> Result<Arc<BlockReader>> {
        let sp = self.table_info.meta.storage_params.clone();
        let operator = match sp {
            Some(p) => {
                let sp: StorageParams = serde_json::from_str(&p)?;
                ctx.get_storage_operator(Some(sp))?
            }
            None => ctx.get_storage_operator(None)?,
        };
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
        let table_schema = self.table_info.schema();
        let projection = self.projection_of_push_downs(&plan.push_downs);
        let output_reader = self.create_block_reader(&ctx, projection)?; // for deserialize output blocks

        let (output_reader, prewhere_reader, prewhere_filter, remain_reader) =
            if let Some(prewhere) = self.prewhere_of_push_downs(&plan.push_downs) {
                let prewhere_schema = prewhere.prewhere_columns.project_schema(&table_schema);
                let prewhere_schema = Arc::new(prewhere_schema);
                let expr_field = prewhere.filter.to_data_field(&prewhere_schema)?;
                let expr_schema = DataSchemaRefExt::create(vec![expr_field]);

                let executor = ExpressionExecutor::try_create(
                    ctx.clone(),
                    "filter expression executor (prewhere) ",
                    prewhere_schema,
                    expr_schema,
                    vec![prewhere.filter.clone()],
                    false,
                )?;
                let output_reader =
                    self.create_block_reader(&ctx, prewhere.output_columns.clone())?;
                let prewhere_reader =
                    self.create_block_reader(&ctx, prewhere.prewhere_columns.clone())?;
                let remain_reader = if prewhere.remain_columns.is_empty() {
                    None
                } else {
                    Some((&*self.create_block_reader(&ctx, prewhere.remain_columns)?).clone())
                };

                (
                    output_reader,
                    prewhere_reader,
                    Some(executor),
                    remain_reader,
                )
            } else {
                (output_reader.clone(), output_reader, None, None)
            };

        let prewhere_filter = Arc::new(prewhere_filter);
        let remain_reader = Arc::new(remain_reader);

        let parts_len = plan.parts.len();
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_threads = std::cmp::min(parts_len, max_threads);

        let mut source_builder = SourcePipeBuilder::create();

        for _index in 0..std::cmp::max(1, max_threads) {
            let output = OutputPort::create();
            source_builder.add_source(
                output.clone(),
                FuseTableSource::create(
                    ctx.clone(),
                    output,
                    output_reader.clone(),
                    prewhere_reader.clone(),
                    prewhere_filter.clone(),
                    remain_reader.clone(),
                )?,
            );
        }

        pipeline.add_pipe(source_builder.finalize());
        Ok(())
    }
}

type DataChunks = Vec<(usize, Vec<u8>)>;

struct PrewhereData {
    data_block: DataBlock,
    filter: ColumnRef,
}

enum State {
    ReadDataPrewhere(PartInfoPtr),
    ReadDataRemain(PartInfoPtr, PrewhereData),
    PrewhereFilter(PartInfoPtr, DataChunks),
    Deserialize(PartInfoPtr, DataChunks, Option<PrewhereData>),
    Generated(Option<PartInfoPtr>, DataBlock),
    Finish,
}

struct FuseTableSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    scan_progress: Arc<Progress>,
    output: Arc<OutputPort>,
    output_reader: Arc<BlockReader>,

    prewhere_reader: Arc<BlockReader>,
    prewhere_filter: Arc<Option<ExpressionExecutor>>,
    remain_reader: Arc<Option<BlockReader>>,
}

impl FuseTableSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        output_reader: Arc<BlockReader>,
        prewhere_reader: Arc<BlockReader>,
        prewhere_filter: Arc<Option<ExpressionExecutor>>,
        remain_reader: Arc<Option<BlockReader>>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        let mut partitions = ctx.try_get_partitions(1)?;
        match partitions.is_empty() {
            true => Ok(ProcessorPtr::create(Box::new(FuseTableSource {
                ctx,
                output,
                scan_progress,
                state: State::Finish,
                output_reader,
                prewhere_reader,
                prewhere_filter,
                remain_reader,
            }))),
            false => Ok(ProcessorPtr::create(Box::new(FuseTableSource {
                ctx,
                output,
                scan_progress,
                state: State::ReadDataPrewhere(partitions.remove(0)),
                output_reader,
                prewhere_reader,
                prewhere_filter,
                remain_reader,
            }))),
        }
    }

    fn generate_one_block(&mut self, block: DataBlock) -> Result<()> {
        let mut partitions = self.ctx.try_get_partitions(1)?;
        // resort and prune columns
        let block = block.resort(self.output_reader.schema())?;
        self.state = match partitions.is_empty() {
            true => State::Generated(None, block),
            false => State::Generated(Some(partitions.remove(0)), block),
        };
        Ok(())
    }

    fn generate_one_empty_block(&mut self) -> Result<()> {
        let mut partitions = self.ctx.try_get_partitions(1)?;
        self.state = match partitions.is_empty() {
            true => State::Generated(
                None,
                DataBlock::empty_with_schema(self.output_reader.schema()),
            ),
            false => State::Generated(
                Some(partitions.remove(0)),
                DataBlock::empty_with_schema(self.output_reader.schema()),
            ),
        };
        Ok(())
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
                    Some(part) => State::ReadDataPrewhere(part),
                };

                self.output.push_data(Ok(data_block));
                return Ok(Event::NeedConsume);
            }
        }

        match self.state {
            State::Finish => Ok(Event::Finished),
            State::ReadDataPrewhere(_) => Ok(Event::Async),
            State::ReadDataRemain(_, _) => Ok(Event::Async),
            State::PrewhereFilter(_, _) => Ok(Event::Sync),
            State::Deserialize(_, _, _) => Ok(Event::Sync),
            State::Generated(_, _) => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::Deserialize(part, chunks, prewhere_data) => {
                let data_block = if let Some(PrewhereData {
                    data_block: mut prewhere_blocks,
                    filter,
                }) = prewhere_data
                {
                    let block = if chunks.is_empty() {
                        prewhere_blocks
                    } else if let Some(remain_reader) = self.remain_reader.as_ref() {
                        let remain_block = remain_reader.deserialize(part, chunks)?;
                        for (col, field) in remain_block
                            .columns()
                            .iter()
                            .zip(remain_block.schema().fields())
                        {
                            prewhere_blocks =
                                prewhere_blocks.add_column(col.clone(), field.clone())?;
                        }
                        prewhere_blocks
                    } else {
                        return Err(ErrorCode::LogicalError("It's a bug. Need remain reader"));
                    };
                    // the last step of prewhere
                    let progress_values = ProgressValues {
                        rows: block.num_rows(),
                        bytes: block.memory_size(),
                    };
                    self.scan_progress.incr(&progress_values);
                    DataBlock::filter_block(block, &filter)?
                } else {
                    let block = self.output_reader.deserialize(part, chunks)?;
                    let progress_values = ProgressValues {
                        rows: block.num_rows(),
                        bytes: block.memory_size(),
                    };
                    self.scan_progress.incr(&progress_values);

                    block
                };

                self.generate_one_block(data_block)?;
                Ok(())
            }
            State::PrewhereFilter(part, chunks) => {
                // deserialize prewhere data block first
                let data_block = self.prewhere_reader.deserialize(part.clone(), chunks)?;
                if let Some(filter) = self.prewhere_filter.as_ref() {
                    // do filter
                    let res = filter.execute(&data_block)?;
                    let filter = DataBlock::cast_to_nonull_boolean(res.column(0))?;
                    // shortcut, if predicates is const boolean (or can be cast to boolean)
                    if !DataBlock::filter_exists(&filter)? {
                        // all rows in this block are filtered out
                        // turn to read next part
                        let progress_values = ProgressValues {
                            rows: data_block.num_rows(),
                            bytes: data_block.memory_size(),
                        };
                        self.scan_progress.incr(&progress_values);
                        self.generate_one_empty_block()?;
                        return Ok(());
                    }
                    if self.remain_reader.is_none() {
                        // shortcut, we don't need to read remain data
                        let progress_values = ProgressValues {
                            rows: data_block.num_rows(),
                            bytes: data_block.memory_size(),
                        };
                        self.scan_progress.incr(&progress_values);
                        let block = DataBlock::filter_block(data_block, &filter)?;
                        self.generate_one_block(block)?;
                    } else {
                        self.state =
                            State::ReadDataRemain(part, PrewhereData { data_block, filter });
                    }
                    Ok(())
                } else {
                    Err(ErrorCode::LogicalError(
                        "It's a bug. No need to do prewhere filter",
                    ))
                }
            }
            _ => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadDataPrewhere(part) => {
                let chunks = self.prewhere_reader.read_columns_data(part.clone()).await?;

                if self.prewhere_filter.is_some() {
                    self.state = State::PrewhereFilter(part, chunks);
                } else {
                    // all needed columns are read.
                    self.state = State::Deserialize(part, chunks, None)
                }
                Ok(())
            }
            State::ReadDataRemain(part, prewhere_data) => {
                if let Some(remain_reader) = self.remain_reader.as_ref() {
                    let chunks = remain_reader.read_columns_data(part.clone()).await?;
                    self.state = State::Deserialize(part, chunks, Some(prewhere_data));
                    Ok(())
                } else {
                    return Err(ErrorCode::LogicalError("It's a bug. No remain reader"));
                }
            }
            _ => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }
}
