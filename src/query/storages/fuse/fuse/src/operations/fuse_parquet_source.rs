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
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::AnyType;
use common_expression::Chunk;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::Value;
use common_functions_v2::scalars::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;

use crate::io::BlockReader;

type DataChunks = Vec<(usize, Vec<u8>)>;

pub struct PrewhereData<'a> {
    chunk: Chunk,
    filter: &'a Value<AnyType>,
}

enum State<'a> {
    ReadDataPrewhere(Option<PartInfoPtr>),
    ReadDataRemain(PartInfoPtr, PrewhereData<'a>),
    PrewhereFilter(PartInfoPtr, DataChunks),
    Deserialize(PartInfoPtr, DataChunks, Option<PrewhereData<'a>>),
    Generated(Option<PartInfoPtr>, Chunk),
    Finish,
}

pub struct FuseParquetSource<'a> {
    state: State<'a>,
    ctx: Arc<dyn TableContext>,
    scan_progress: Arc<Progress>,
    output: Arc<OutputPort>,
    output_reader: Arc<BlockReader>,

    prewhere_reader: Arc<BlockReader>,
    prewhere_filter: Arc<Option<Expr<usize>>>,
    remain_reader: Arc<Option<BlockReader>>,

    support_blocking: bool,
}

impl FuseParquetSource<'_> {
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
        Ok(ProcessorPtr::create(Box::new(FuseParquetSource {
            ctx,
            output,
            scan_progress,
            state: State::ReadDataPrewhere(None),
            output_reader,
            prewhere_reader,
            prewhere_filter,
            remain_reader,
            support_blocking,
        })))
    }

    fn generate_one_block(&mut self, chunk: Chunk) -> Result<()> {
        let new_part = self.ctx.try_get_part();
        // resort and prune columns
        // todo!("expression")
        // let chunk = chunk.resort(self.output_reader.schema())?;
        self.state = State::Generated(new_part, chunk);
        Ok(())
    }

    fn generate_one_empty_block(&mut self) -> Result<()> {
        let new_part = self.ctx.try_get_part();
        self.state = State::Generated(new_part, Chunk::empty());
        Ok(())
    }
}

#[async_trait::async_trait]
impl Processor for FuseParquetSource<'_> {
    fn name(&self) -> String {
        "FuseEngineSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::ReadDataPrewhere(None)) {
            self.state = match self.ctx.try_get_part() {
                None => State::Finish,
                Some(part) => State::ReadDataPrewhere(Some(part)),
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
            if let State::Generated(part, chunk) = std::mem::replace(&mut self.state, State::Finish)
            {
                self.state = match part {
                    None => State::Finish,
                    Some(part) => State::ReadDataPrewhere(Some(part)),
                };

                self.output.push_data(Ok(chunk));
                return Ok(Event::NeedConsume);
            }
        }

        match self.state {
            State::Finish => Ok(Event::Finished),
            State::ReadDataPrewhere(_) => {
                if self.support_blocking {
                    Ok(Event::Sync)
                } else {
                    Ok(Event::Async)
                }
            }
            State::ReadDataRemain(_, _) => {
                if self.support_blocking {
                    Ok(Event::Sync)
                } else {
                    Ok(Event::Async)
                }
            }
            State::PrewhereFilter(_, _) => Ok(Event::Sync),
            State::Deserialize(_, _, _) => Ok(Event::Sync),
            State::Generated(_, _) => Err(ErrorCode::Internal("It's a bug.")),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::Deserialize(part, chunks, prewhere_data) => {
                let chunk = if let Some(PrewhereData {
                    chunk: mut prewhere_chunks,
                    filter,
                }) = prewhere_data
                {
                    let chunk = if chunks.is_empty() {
                        prewhere_chunks
                    } else if let Some(remain_reader) = self.remain_reader.as_ref() {
                        let remain_chunk = remain_reader.deserialize(part, chunks)?;
                        for col in remain_chunk.columns() {
                            prewhere_chunks.add_column(col.clone());
                        }
                        prewhere_chunks
                    } else {
                        return Err(ErrorCode::Internal("It's a bug. Need remain reader"));
                    };
                    // the last step of prewhere
                    let progress_values = ProgressValues {
                        rows: chunk.num_rows(),
                        bytes: chunk.memory_size(),
                    };
                    self.scan_progress.incr(&progress_values);
                    chunk.filter(&filter)?
                } else {
                    let chunk = self.output_reader.deserialize(part, chunks)?;
                    let progress_values = ProgressValues {
                        rows: chunk.num_rows(),
                        bytes: chunk.memory_size(),
                    };
                    self.scan_progress.incr(&progress_values);

                    chunk
                };

                self.generate_one_block(chunk)?;
                Ok(())
            }
            State::PrewhereFilter(part, chunks) => {
                // deserialize prewhere data block first
                let chunk = self.prewhere_reader.deserialize(part.clone(), chunks)?;
                if let Some(filter) = self.prewhere_filter.as_ref() {
                    // do filter
                    let evaluator =
                        Evaluator::new(&chunk, FunctionContext::default(), &BUILTIN_FUNCTIONS);
                    let predicate = evaluator.run(filter).map_err(|(_, e)| {
                        ErrorCode::Internal(format!("eval prewhere filter failed: {}.", e))
                    })?;

                    // shortcut, if predicates is const boolean (or can be cast to boolean)
                    if !Chunk::<usize>::filter_exists(&predicate)? {
                        // all rows in this block are filtered out
                        // turn to read next part
                        let progress_values = ProgressValues {
                            rows: chunk.num_rows(),
                            bytes: chunk.memory_size(),
                        };
                        self.scan_progress.incr(&progress_values);
                        self.generate_one_empty_block()?;
                        return Ok(());
                    }
                    if self.remain_reader.is_none() {
                        // shortcut, we don't need to read remain data
                        let progress_values = ProgressValues {
                            rows: chunk.num_rows(),
                            bytes: chunk.memory_size(),
                        };
                        self.scan_progress.incr(&progress_values);
                        let chunk = chunk.filter(&predicate)?;
                        self.generate_one_block(chunk)?;
                    } else {
                        self.state = State::ReadDataRemain(part, PrewhereData {
                            chunk,
                            filter: &predicate,
                        });
                    }
                    Ok(())
                } else {
                    Err(ErrorCode::Internal(
                        "It's a bug. No need to do prewhere filter",
                    ))
                }
            }

            State::ReadDataPrewhere(Some(part)) => {
                let chunks = self.prewhere_reader.sync_read_columns_data(part.clone())?;

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
                    let chunks = remain_reader.sync_read_columns_data(part.clone())?;
                    self.state = State::Deserialize(part, chunks, Some(prewhere_data));
                    Ok(())
                } else {
                    Err(ErrorCode::Internal("It's a bug. No remain reader"))
                }
            }
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadDataPrewhere(Some(part)) => {
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
                    Err(ErrorCode::Internal("It's a bug. No remain reader"))
                }
            }
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }
}
