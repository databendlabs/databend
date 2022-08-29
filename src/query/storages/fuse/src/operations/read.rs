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
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
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
        let table_schema = self.table_info.schema();
        let projection = self.projection_of_push_downs(&plan.push_downs);
        let output_schema = projection.project_schema(&table_schema);
        let output_schema = Arc::new(output_schema);
        let output_reader = self.create_block_reader(&ctx, projection)?; // for deserialize output blocks

        let (output_reader, prewhere_reader, prewhere_filter, remain_reader) =
            if let Some(prewhere) = self.prewhere_of_push_downs(&plan.push_downs) {
                let prewhere_schema = prewhere.need_columns.project_schema(&table_schema);
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

                let prewhere_reader =
                    self.create_block_reader(&ctx, prewhere.need_columns.clone())?;
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
                    output_schema.clone(),
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

enum State {
    ReadDataPrewhere(PartInfoPtr),
    ReadDataRemain(PartInfoPtr, DataChunks, ColumnRef),
    PrewhereFilter(PartInfoPtr, DataChunks),
    Deserialize(PartInfoPtr, DataChunks, Option<ColumnRef>),
    Generated(Option<PartInfoPtr>, DataBlock),
    Finish,
}

struct FuseTableSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    scan_progress: Arc<Progress>,
    output: Arc<OutputPort>,
    output_schema: DataSchemaRef,
    output_reader: Arc<BlockReader>,

    prewhere_reader: Arc<BlockReader>,
    prewhere_filter: Arc<Option<ExpressionExecutor>>,
    remain_reader: Arc<Option<BlockReader>>,
}

impl FuseTableSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        output_schema: DataSchemaRef,
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
                output_schema,
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
                output_schema,
                output_reader,
                prewhere_reader,
                prewhere_filter,
                remain_reader,
            }))),
        }
    }

    fn generate_one_block(&mut self, block: DataBlock) -> Result<()> {
        let mut partitions = self.ctx.try_get_partitions(1)?;

        let progress_values = ProgressValues {
            rows: block.num_rows(),
            bytes: block.memory_size(),
        };
        self.scan_progress.incr(&progress_values);

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
                DataBlock::empty_with_schema(self.output_schema.clone()),
            ),
            false => State::Generated(
                Some(partitions.remove(0)),
                DataBlock::empty_with_schema(self.output_schema.clone()),
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
            State::ReadDataRemain(_, _, _) => Ok(Event::Async),
            State::PrewhereFilter(_, _) => Ok(Event::Sync),
            State::Deserialize(_, _, _) => Ok(Event::Sync),
            State::Generated(_, _) => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::Deserialize(part, chunks, filter) => {
                let data_block = if let Some(filter) = filter {
                    let block = self.output_reader.deserialize(part, chunks)?;
                    // the last step of prewhere
                    DataBlock::filter_block(block, &filter)?
                } else {
                    self.output_reader.deserialize(part, chunks)?
                };

                self.generate_one_block(data_block)?;
                Ok(())
            }
            State::PrewhereFilter(part, chunks) => {
                // deserialize prewhere data block first
                let block = self
                    .prewhere_reader
                    .deserialize(part.clone(), chunks.clone())?;
                if let Some(filter) = self.prewhere_filter.as_ref() {
                    // do filter
                    let res = filter.execute(&block)?;
                    let filter = DataBlock::cast_to_nonull_boolean(res.column(0))?;
                    // shortcut, if predicates is const boolean (or can be cast to boolean)
                    if !DataBlock::filter_exists(&filter)? {
                        // all rows in this block are filtered out
                        // turn to read next part
                        self.generate_one_empty_block()?;
                        return Ok(());
                    }
                    if self.remain_reader.is_none() {
                        // shortcut, we don't need to read remain data
                        let block = DataBlock::filter_block(block, &filter)?;
                        self.generate_one_block(block)?;
                    } else {
                        self.state = State::ReadDataRemain(part, chunks, filter);
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
            State::ReadDataRemain(part, prewhere_chunks, filter) => {
                if let Some(remain_reader) = self.remain_reader.as_ref() {
                    let mut chunks = remain_reader.read_columns_data(part.clone()).await?;
                    // merge two parts of chunks
                    chunks.extend(prewhere_chunks);
                    self.state = State::Deserialize(part, chunks, Some(filter));
                    Ok(())
                } else {
                    return Err(ErrorCode::LogicalError("It's a bug. No remain reader"));
                }
            }
            _ => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }
}
