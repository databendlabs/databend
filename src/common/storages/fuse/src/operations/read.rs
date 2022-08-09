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
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_core::Pipeline;
use common_pipeline_core::SourcePipeBuilder;
use common_planners::Extras;
use common_planners::PartInfoPtr;
use common_planners::ReadDataSourcePlan;

use crate::io::BlockReader;
use crate::operations::read::State::Generated;
use crate::FuseTable;

impl FuseTable {
    pub fn create_block_reader(
        &self,
        ctx: &Arc<dyn TableContext>,
        projection: Vec<usize>,
    ) -> Result<Arc<BlockReader>> {
        let operator = ctx.get_storage_operator()?;
        let table_schema = self.table_info.schema();
        BlockReader::create(operator, table_schema, projection)
    }

    pub fn projection_of_push_downs(&self, push_downs: &Option<Extras>) -> Vec<usize> {
        if let Some(Extras {
            projection: Some(prj),
            ..
        }) = push_downs
        {
            prj.clone()
        } else {
            (0..self.table_info.schema().fields().len())
                .into_iter()
                .collect::<Vec<usize>>()
        }
    }

    #[inline]
    pub fn do_read2(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &ReadDataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let block_reader =
            self.create_block_reader(&ctx, self.projection_of_push_downs(&plan.push_downs))?;

        let parts_len = plan.parts.len();
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_threads = std::cmp::min(parts_len, max_threads);

        let mut source_builder = SourcePipeBuilder::create();

        for _index in 0..std::cmp::max(1, max_threads) {
            let output = OutputPort::create();
            source_builder.add_source(
                output.clone(),
                FuseTableSource::create(ctx.clone(), output, block_reader.clone())?,
            );
        }

        pipeline.add_pipe(source_builder.finalize());
        Ok(())
    }
}

enum State {
    ReadData(PartInfoPtr),
    Deserialize(PartInfoPtr, Vec<(usize, Vec<u8>)>),
    Generated(Option<PartInfoPtr>, DataBlock),
    Finish,
}

struct FuseTableSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    scan_progress: Arc<Progress>,
    block_reader: Arc<BlockReader>,
    output: Arc<OutputPort>,
}

impl FuseTableSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
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
            }))),
            false => Ok(ProcessorPtr::create(Box::new(FuseTableSource {
                ctx,
                output,
                block_reader,
                scan_progress,
                state: State::ReadData(partitions.remove(0)),
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
                    Some(part) => State::ReadData(part),
                };

                self.output.push_data(Ok(data_block));
                return Ok(Event::NeedConsume);
            }
        }

        match self.state {
            State::Finish => Ok(Event::Finished),
            State::ReadData(_) => Ok(Event::Async),
            State::Deserialize(_, _) => Ok(Event::Sync),
            State::Generated(_, _) => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::Deserialize(part, chunks) => {
                let data_block = self.block_reader.deserialize(part, chunks)?;
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
            _ => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadData(part) => {
                let chunks = self.block_reader.read_columns_data(part.clone()).await?;
                self.state = State::Deserialize(part, chunks);
                Ok(())
            }
            _ => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }
}
