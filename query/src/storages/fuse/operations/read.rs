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
        // let ctx_clone = ctx.clone();
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
        let block_reader = BlockReader::create(operator, table_schema, projection, ctx.clone())?;
        Ok(block_reader)
    }

    #[inline]
    pub fn do_read2(
        &self,
        ctx: Arc<QueryContext>,
        plan: &ReadDataSourcePlan,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let block_reader = self.create_block_reader(&ctx, &plan.push_downs)?;

        let mut source_builder = SourcePipeBuilder::create();
        for part in &plan.parts {
            let output = OutputPort::create();
            source_builder.add_source(
                output.clone(),
                FuseTableSource::create(output, block_reader.clone(), part.clone())?,
            );
        }

        pipeline.add_pipe(source_builder.finalize());
        pipeline.resize(ctx.get_settings().get_max_threads()? as usize)?;
        Ok(())
    }
}

enum State {
    ReadData,
    Deserialize(Vec<Vec<u8>>),
    Generated(DataBlock),
    Finish,
}

struct FuseTableSource {
    state: State,
    part: PartInfoPtr,
    block_reader: Arc<BlockReader>,
    output: Arc<OutputPort>,
}

impl FuseTableSource {
    pub fn create(
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
        part: PartInfoPtr,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(FuseTableSource {
            state: State::ReadData,
            part,
            block_reader,
            output,
        })))
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

        if matches!(self.state, State::Generated(_)) {
            println!("Generated");
            if let Generated(data_block) = std::mem::replace(&mut self.state, State::Finish) {
                println!("Generated {:?}", data_block);
                self.output.push_data(Ok(data_block));
                return Ok(Event::NeedConsume);
            }
        }

        match self.state {
            State::Finish => Ok(Event::Finished),
            State::ReadData => Ok(Event::Async),
            State::Deserialize(_) => Ok(Event::Sync),
            State::Generated(_) => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }

    fn process(&mut self) -> Result<()> {
        if let State::Deserialize(chunks) = std::mem::replace(&mut self.state, State::ReadData) {
            let part = self.part.clone();
            self.state = State::Generated(self.block_reader.deserialize(part, chunks)?);
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        if let State::ReadData = self.state {
            let part = self.part.clone();
            self.state = State::Deserialize(self.block_reader.read_columns_data(part).await?);
        }

        Ok(())
    }
}
