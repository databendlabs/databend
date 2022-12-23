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

use common_arrow::native::read::reader::PaReader;
use common_arrow::native::read::PaReadBuf;
use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Chunk;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_functions_v2::scalars::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;

use crate::io::BlockReader;

type DataChunks = Vec<(usize, PaReader<Box<dyn PaReadBuf + Send + Sync>>)>;

enum State {
    ReadData(Option<PartInfoPtr>),
    Deserialize(DataChunks),
    Generated(Chunk, DataChunks),
    Finish,
}

pub struct FuseNativeSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    scan_progress: Arc<Progress>,
    output: Arc<OutputPort>,
    output_reader: Arc<BlockReader>,

    prewhere_reader: Arc<BlockReader>,
    prewhere_filter: Arc<Option<Expr<usize>>>,
    remain_reader: Arc<Option<BlockReader>>,

    support_blocking: bool,
}

impl FuseNativeSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        output_reader: Arc<BlockReader>,
        prewhere_reader: Arc<BlockReader>,
        prewhere_filter: Arc<Option<Expr<usize>>>,
        remain_reader: Arc<Option<BlockReader>>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        let support_blocking = prewhere_reader.support_blocking_api();
        Ok(ProcessorPtr::create(Box::new(FuseNativeSource {
            ctx,
            output,
            scan_progress,
            state: State::ReadData(None),
            output_reader,
            prewhere_reader,
            prewhere_filter,
            remain_reader,
            support_blocking,
        })))
    }

    fn generate_one_block(&mut self, chunk: Chunk, chunks: DataChunks) -> Result<()> {
        // resort and prune columns
        // todo!("expression")
        // let chunk = chunk.resort(self.output_reader.schema())?;
        self.state = State::Generated(chunk, chunks);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Processor for FuseNativeSource {
    fn name(&self) -> String {
        "FuseEngineSource".to_string()
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

        if matches!(self.state, State::Generated(_, _)) {
            if let State::Generated(chunk, chunks) =
                std::mem::replace(&mut self.state, State::Finish)
            {
                self.state = State::Deserialize(chunks);
                self.output.push_data(Ok(chunk));
                return Ok(Event::NeedConsume);
            }
        }

        match self.state {
            State::Finish => Ok(Event::Finished),
            State::ReadData(_) => {
                if self.support_blocking {
                    Ok(Event::Sync)
                } else {
                    Ok(Event::Async)
                }
            }
            State::Deserialize(_) => Ok(Event::Sync),
            State::Generated(_, _) => Err(ErrorCode::Internal("It's a bug.")),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::Deserialize(mut chunks) => {
                let prewhere_idx = self.prewhere_reader.schema().num_fields();
                let mut prewhere_chunks = Vec::with_capacity(prewhere_idx);

                for (index, chunk) in chunks.iter_mut().take(prewhere_idx) {
                    // No data anymore
                    if !chunk.has_next() {
                        self.state = State::ReadData(None);
                        return Ok(());
                    }
                    prewhere_chunks.push((*index, chunk.next_array()?));
                }

                let mut chunk = self.prewhere_reader.build_block(prewhere_chunks)?;

                if let Some(remain_reader) = self.remain_reader.as_ref() {
                    let mut remain_chunks = Vec::with_capacity(prewhere_idx);
                    for (index, chunk) in chunks.iter_mut().skip(prewhere_idx) {
                        assert!(chunk.has_next());
                        remain_chunks.push((*index, chunk.next_array()?));
                    }
                    let remain_chunk = remain_reader.build_block(remain_chunks)?;
                    for col in remain_chunk.columns() {
                        chunk.add_column(col.clone());
                    }
                }

                if let Some(filter) = self.prewhere_filter.as_ref() {
                    // do filter
                    let evaluator =
                        Evaluator::new(&chunk, FunctionContext::default(), &BUILTIN_FUNCTIONS);
                    let predicate = evaluator.run(filter).map_err(|(_, e)| {
                        ErrorCode::Internal(format!("eval prewhere filter failed: {}.", e))
                    })?;
                    chunk = chunk.filter(&predicate)?;
                }

                // the last step of prewhere
                let progress_values = ProgressValues {
                    rows: chunk.num_rows(),
                    bytes: chunk.memory_size(),
                };
                self.scan_progress.incr(&progress_values);

                self.generate_one_block(chunk, chunks)?;
                Ok(())
            }

            State::ReadData(Some(part)) => {
                let mut chunks = self
                    .prewhere_reader
                    .sync_read_native_columns_data(part.clone())?;

                if let Some(r) = self.remain_reader.as_ref() {
                    let cs = r.sync_read_native_columns_data(part.clone())?;
                    chunks.extend(cs)
                }

                self.state = State::Deserialize(chunks);
                Ok(())
            }
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadData(Some(part)) => {
                let mut chunks = self
                    .prewhere_reader
                    .async_read_native_columns_data(part.clone())
                    .await?;

                if let Some(r) = self.remain_reader.as_ref() {
                    let cs = r.async_read_native_columns_data(part.clone()).await?;
                    chunks.extend(cs)
                }

                self.state = State::Deserialize(chunks);
                Ok(())
            }
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }
}
