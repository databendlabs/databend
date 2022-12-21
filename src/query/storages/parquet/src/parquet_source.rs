//  Copyright 2022 Datafuse Labs.
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
use common_expression::types::BooleanType;
use common_expression::Chunk;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::Function;
use common_expression::FunctionContext;
use common_expression::TableSchemaRef;
use common_expression::Value;
use common_functions_v2::scalars::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;

use crate::parquet_part::ParquetRowGroupPart;
use crate::parquet_reader::IndexedChunk;
use crate::parquet_reader::ParquetReader;
use crate::parquet_source::State::Generated;

struct PrewhereData {
    chunk: Chunk<String>,
    filter: Value<BooleanType>,
}

/// The states for [`ParquetSource`]. The states will recycle for each row group of a parquet file.
enum State {
    ReadDataPrewhere(Option<PartInfoPtr>),
    ReadDataRemain(PartInfoPtr, PrewhereData),
    PrewhereFilter(PartInfoPtr, Vec<IndexedChunk>),
    Deserialize(PartInfoPtr, Vec<IndexedChunk>, Option<PrewhereData>),
    Generated(Option<PartInfoPtr>, Chunk<String>),
    Finish,
}

pub struct ParquetSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    scan_progress: Arc<Progress>,
    output: Arc<OutputPort>,
    output_schema: TableSchemaRef,

    prewhere_reader: Arc<ParquetReader>,
    prewhere_filter: Arc<Option<Expr<String>>>,
    remain_reader: Arc<Option<ParquetReader>>,
}

impl ParquetSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        output_schema: TableSchemaRef,
        prewhere_reader: Arc<ParquetReader>,
        prewhere_filter: Arc<Option<Expr<String>>>,
        remain_reader: Arc<Option<ParquetReader>>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        Ok(ProcessorPtr::create(Box::new(ParquetSource {
            ctx,
            output,
            scan_progress,
            state: State::ReadDataPrewhere(None),
            output_schema,
            prewhere_reader,
            prewhere_filter,
            remain_reader,
        })))
    }

    #[inline]
    pub fn output_schema(&self) -> TableSchemaRef {
        self.output_schema.clone()
    }

    fn do_prewhere_filter(
        &mut self,
        part: PartInfoPtr,
        raw_chunks: Vec<IndexedChunk>,
    ) -> Result<()> {
        let rg_part = ParquetRowGroupPart::from_part(&part)?;
        // deserialize prewhere data block first
        let chunk = self
            .prewhere_reader
            .deserialize(rg_part, raw_chunks, None)?;
        if let Some(filter) = self.prewhere_filter.as_ref() {
            // do filter
            let evaluator = Evaluator::new(&chunk, FunctionContext::default(), &BUILTIN_FUNCTIONS);

            let res = evaluator.run(filter).map_err(|(_, e)| {
                ErrorCode::Internal(format!("eval prewhere filter failed: {}.", e))
            })?;
            let filter = Chunk::<String>::cast_to_nonull_boolean(&res).ok_or_else(|| {
                ErrorCode::BadArguments(
                    "Result of filter expression cannot be converted to boolean.",
                )
            })?;

            let all_filtered = match &filter {
                Value::Scalar(v) => !v,
                Value::Column(bitmap) => bitmap.unset_bits() == bitmap.len(),
            };

            if all_filtered {
                // shortcut:
                // all rows in this block are filtered out
                // turn to begin the next state cycle.
                let progress_values = ProgressValues {
                    rows: chunk.num_rows(),
                    bytes: chunk.memory_size(),
                };
                self.scan_progress.incr(&progress_values);

                // Generate a empty block.
                self.state = Generated(self.ctx.try_get_part(), Chunk::empty());
                return Ok(());
            }

            let (rows, bytes) = if self.remain_reader.is_none() {
                (chunk.num_rows(), chunk.memory_size())
            } else {
                (0, 0)
            };

            let filtered_chunk = match &filter {
                Value::Scalar(_) => chunk,
                Value::Column(bitmap) => Chunk::filter_chunk_with_bitmap(chunk, bitmap)?,
            };

            if self.remain_reader.is_none() {
                // shortcut, we don't need to read remain data
                let progress_values = ProgressValues { rows, bytes };
                self.scan_progress.incr(&progress_values);
                // In this case, schema of prewhere reading is the final output schema,
                // so don't need to resort by schema.
                self.state = Generated(self.ctx.try_get_part(), filtered_chunk);
            } else {
                self.state = State::ReadDataRemain(part, PrewhereData {
                    chunk: filtered_chunk,
                    filter,
                });
            }
            Ok(())
        } else {
            Err(ErrorCode::Internal(
                "It's a bug. No need to do prewhere filter",
            ))
        }
    }

    fn do_deserialize(
        &mut self,
        part: PartInfoPtr,
        raw_chunks: Vec<IndexedChunk>,
        prewhere_data: Option<PrewhereData>,
    ) -> Result<()> {
        let rg_part = ParquetRowGroupPart::from_part(&part)?;
        let output_chunk = if let Some(PrewhereData {
            chunk: mut prewhere_chunk,
            filter,
        }) = prewhere_data
        {
            let chunk = if raw_chunks.is_empty() {
                prewhere_chunk
            } else if let Some(remain_reader) = self.remain_reader.as_ref() {
                let remain_chunk = match filter {
                    Value::Scalar(_) => {
                        // The case of all filtered is already covered in `do_prewhere_filter`.
                        // don't need filter
                        remain_reader.deserialize(rg_part, raw_chunks, None)?
                    }
                    Value::Column(bitmap) => {
                        if bitmap.unset_bits() != 0 {
                            // don't need filter
                            remain_reader.deserialize(rg_part, raw_chunks, None)?
                        } else {
                            remain_reader.deserialize(rg_part, raw_chunks, Some(bitmap))?
                        }
                    }
                };

                assert_eq!(
                    prewhere_chunk.num_rows(),
                    remain_chunk.num_rows(),
                    "prewhere and remain chunks should have same row number. (prewhere: {}, remain: {})",
                    prewhere_chunk.num_rows(),
                    remain_chunk.num_rows()
                );

                // Combine two blocks.
                for col in remain_chunk.columns() {
                    prewhere_chunk.add_column(col.clone());
                }
                prewhere_chunk
            } else {
                return Err(ErrorCode::Internal("It's a bug. Need remain reader"));
            };
            // the last step of prewhere
            let progress_values = ProgressValues {
                rows: chunk.num_rows(),
                bytes: chunk.memory_size(),
            };
            self.scan_progress.incr(&progress_values);
            chunk
        } else {
            // There is only prewhere reader.
            assert!(self.remain_reader.is_none());
            let chunk = self
                .prewhere_reader
                .deserialize(rg_part, raw_chunks, None)?;
            let progress_values = ProgressValues {
                rows: chunk.num_rows(),
                bytes: chunk.memory_size(),
            };
            self.scan_progress.incr(&progress_values);

            chunk
        };

        self.state = State::Generated(
            self.ctx.try_get_part(),
            output_chunk.resort(self.output_schema().as_ref())?,
        );
        Ok(())
    }
}

#[async_trait::async_trait]
impl Processor for ParquetSource {
    fn name(&self) -> String {
        "ParquetSource".to_string()
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
            if let Generated(part, chunk) = std::mem::replace(&mut self.state, State::Finish) {
                if let Some(part) = part {
                    self.state = State::ReadDataPrewhere(Some(part));
                }
                todo!("expression");
                // self.output.push_data(Ok(data_block));
                // return Ok(Event::NeedConsume);
            }
        }

        match self.state {
            State::Finish => Ok(Event::Finished),
            State::ReadDataPrewhere(_)
            | State::ReadDataRemain(_, _)
            | State::PrewhereFilter(_, _)
            | State::Deserialize(_, _, _) => Ok(Event::Sync),
            State::Generated(_, _) => Err(ErrorCode::Internal("It's a bug.")),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadDataPrewhere(Some(part)) => {
                let rg_part = ParquetRowGroupPart::from_part(&part)?;
                let chunks = self.prewhere_reader.sync_read_columns(rg_part)?;
                if self.prewhere_filter.is_some() {
                    self.state = State::PrewhereFilter(part, chunks);
                } else {
                    // If there is no prewhere filter, it means there is only the prewhere reader.
                    assert!(self.remain_reader.is_none());
                    // So all the needed columns are read.
                    self.state = State::Deserialize(part, chunks, None)
                }
                Ok(())
            }
            State::ReadDataRemain(part, prewhere_data) => {
                if let Some(remain_reader) = self.remain_reader.as_ref() {
                    let rg_part = ParquetRowGroupPart::from_part(&part)?;
                    let chunks = remain_reader.sync_read_columns(rg_part)?;
                    self.state = State::Deserialize(part, chunks, Some(prewhere_data));
                    Ok(())
                } else {
                    Err(ErrorCode::Internal("It's a bug. No remain reader"))
                }
            }
            State::PrewhereFilter(part, chunks) => self.do_prewhere_filter(part, chunks),
            State::Deserialize(part, chunks, prewhere_data) => {
                self.do_deserialize(part, chunks, prewhere_data)
            }
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }
}
