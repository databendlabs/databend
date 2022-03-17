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
//

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Extras;
use common_planners::PartInfoPtr;
use common_planners::ReadDataSourcePlan;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing_futures::Instrument;
use futures::StreamExt;

use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::Event;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::Processor;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::SourcePipeBuilder;
use crate::sessions::QueryContext;
use crate::storages::fuse::io::BlockReader;
use crate::storages::fuse::operations::read::State::Generated;
use crate::storages::fuse::FuseTable;

impl FuseTable {
    #[inline]
    pub async fn do_read(
        &self,
        ctx: Arc<QueryContext>,
        push_downs: &Option<Extras>,
    ) -> Result<SendableDataBlockStream> {
        let block_reader = self.create_block_reader(&ctx, push_downs)?;

        let bite_size = ctx.get_settings().get_parallel_read_threads()?;
        let iter = std::iter::from_fn(move || match ctx.clone().try_get_partitions(bite_size) {
            Err(_) => None,
            Ok(parts) if parts.is_empty() => None,
            Ok(parts) => Some(parts),
        })
        .flatten();

        let part_stream = futures::stream::iter(iter);

        let stream = part_stream
            .map(move |part| {
                let block_reader = block_reader.clone();
                async move { block_reader.read(part).await }
            })
            .buffer_unordered(bite_size as usize)
            .instrument(common_tracing::tracing::Span::current());
        Ok(Box::pin(stream))
    }

    fn create_block_reader(
        &self,
        ctx: &Arc<QueryContext>,
        push_downs: &Option<Extras>,
    ) -> Result<Arc<BlockReader>> {
        let projection = if let Some(Extras {
            projection: Some(prj),
            ..
        }) = push_downs
        {
            prj.clone()
        } else {
            (0..self.table_info.schema().fields().len())
                .into_iter()
                .collect::<Vec<usize>>()
        };

        let operator = ctx.get_storage_operator()?;
        let table_schema = self.table_info.schema();
        BlockReader::create(ctx.clone(), operator, table_schema, projection)
    }

    #[inline]
    pub fn do_read2(
        &self,
        ctx: Arc<QueryContext>,
        plan: &ReadDataSourcePlan,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let block_reader = self.create_block_reader(&ctx, &plan.push_downs)?;

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
    Deserialize(PartInfoPtr, Vec<Vec<u8>>),
    Generated(Option<PartInfoPtr>, DataBlock),
    Finish,
}

struct FuseTableSource {
    state: State,
    ctx: Arc<QueryContext>,
    block_reader: Arc<BlockReader>,
    output: Arc<OutputPort>,
}

impl FuseTableSource {
    pub fn create(
        ctx: Arc<QueryContext>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
    ) -> Result<ProcessorPtr> {
        let mut partitions = ctx.try_get_partitions(1)?;
        match partitions.is_empty() {
            true => Ok(ProcessorPtr::create(Box::new(FuseTableSource {
                ctx,
                output,
                block_reader,
                state: State::Finish,
            }))),
            false => Ok(ProcessorPtr::create(Box::new(FuseTableSource {
                ctx,
                output,
                block_reader,
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
