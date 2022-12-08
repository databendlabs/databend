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

use common_arrow::parquet::metadata::RowGroupMetaData;
use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::ColumnRef;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionContext;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_sql::evaluator::EvalNode;

use crate::parquet_reader::IndexedChunk;
use crate::parquet_reader::ParquetReader;
use crate::parquet_source::State::Generated;
use crate::ParquetPart;

struct PrewhereData {
    data_block: DataBlock,
    filter: ColumnRef,
}

/// Hold the row groups and record which row group is needed to read.
struct RowGroupState {
    location: String,

    row_groups: Vec<RowGroupMetaData>,
    index: usize,
}

impl RowGroupState {
    fn new(location: String, row_groups: Vec<RowGroupMetaData>) -> Self {
        Self {
            location,
            row_groups,
            index: 0,
        }
    }

    #[inline]
    fn get(&self) -> &RowGroupMetaData {
        assert!(self.index < self.row_groups.len());
        unsafe { self.row_groups.get_unchecked(self.index) }
    }

    #[inline]
    fn advance(&mut self) {
        self.index += 1;
    }

    #[inline]
    fn finished(&self) -> bool {
        self.index >= self.row_groups.len()
    }
}

/// The states for [`ParquetSource`]. The states will recycle for each row group of a parquet file.
enum State {
    Prepare(Option<PartInfoPtr>), // prapare meta data of current part (parquet file).
    ReadDataPrewhere(RowGroupState),
    ReadDataRemain(RowGroupState, PrewhereData),
    PrewhereFilter(RowGroupState, Vec<IndexedChunk>),
    Deserialize(RowGroupState, Vec<IndexedChunk>, Option<PrewhereData>),
    Generated(RowGroupState, DataBlock),
    Finish,
}

pub struct ParquetSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    scan_progress: Arc<Progress>,
    output: Arc<OutputPort>,
    output_schema: DataSchemaRef,

    prewhere_reader: Arc<ParquetReader>,
    prewhere_filter: Arc<Option<EvalNode>>,
    remain_reader: Arc<Option<ParquetReader>>,
}

impl ParquetSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        output_schema: DataSchemaRef,
        prewhere_reader: Arc<ParquetReader>,
        prewhere_filter: Arc<Option<EvalNode>>,
        remain_reader: Arc<Option<ParquetReader>>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        Ok(ProcessorPtr::create(Box::new(ParquetSource {
            ctx,
            output,
            scan_progress,
            state: State::Prepare(None),
            output_schema,
            prewhere_reader,
            prewhere_filter,
            remain_reader,
        })))
    }

    #[inline]
    pub fn output_schema(&self) -> DataSchemaRef {
        self.output_schema.clone()
    }

    fn do_prewhere_filter(&mut self, rg: RowGroupState, chunks: Vec<IndexedChunk>) -> Result<()> {
        // deserialize prewhere data block first
        let data_block = self.prewhere_reader.deserialize(rg.get(), chunks)?;
        if let Some(filter) = self.prewhere_filter.as_ref() {
            // do filter
            let res = filter
                .eval(&FunctionContext::default(), &data_block)?
                .vector;
            let filter = DataBlock::cast_to_nonull_boolean(&res)?;
            // shortcut, if predicates is const boolean (or can be cast to boolean)
            if !DataBlock::filter_exists(&filter)? {
                // all rows in this block are filtered out
                // turn to begin the next state cycle.
                let progress_values = ProgressValues {
                    rows: data_block.num_rows(),
                    bytes: data_block.memory_size(),
                };
                self.scan_progress.incr(&progress_values);

                // Generate a empty block.
                self.state = Generated(rg, DataBlock::empty_with_schema(self.output_schema()));
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
                self.state = State::Generated(rg, block.resort(self.output_schema())?);
            } else {
                self.state = State::ReadDataRemain(rg, PrewhereData { data_block, filter });
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
        rg: RowGroupState,
        chunks: Vec<IndexedChunk>,
        prewhere_data: Option<PrewhereData>,
    ) -> Result<()> {
        let data_block = if let Some(PrewhereData {
            data_block: mut prewhere_blocks,
            filter,
        }) = prewhere_data
        {
            let block = if chunks.is_empty() {
                prewhere_blocks
            } else if let Some(remain_reader) = self.remain_reader.as_ref() {
                let remain_block = remain_reader.deserialize(rg.get(), chunks)?;
                for (col, field) in remain_block
                    .columns()
                    .iter()
                    .zip(remain_block.schema().fields())
                {
                    prewhere_blocks = prewhere_blocks.add_column(col.clone(), field.clone())?;
                }
                prewhere_blocks
            } else {
                return Err(ErrorCode::Internal("It's a bug. Need remain reader"));
            };
            // the last step of prewhere
            let progress_values = ProgressValues {
                rows: block.num_rows(),
                bytes: block.memory_size(),
            };
            self.scan_progress.incr(&progress_values);
            DataBlock::filter_block(block, &filter)?
        } else {
            // There is only prewhere reader.
            assert!(self.remain_reader.is_none());
            let block = self.prewhere_reader.deserialize(rg.get(), chunks)?;
            let progress_values = ProgressValues {
                rows: block.num_rows(),
                bytes: block.memory_size(),
            };
            self.scan_progress.incr(&progress_values);

            block
        };

        self.state = State::Generated(rg, data_block.resort(self.output_schema())?);
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
        if matches!(self.state, State::Prepare(None)) {
            self.state = match self.ctx.try_get_part() {
                None => State::Finish,
                Some(part) => State::Prepare(Some(part)),
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
            if let Generated(mut rg, data_block) = std::mem::replace(&mut self.state, State::Finish)
            {
                rg.advance();
                if rg.finished() {
                    if let Some(part) = self.ctx.try_get_part() {
                        self.state = State::Prepare(Some(part))
                    }
                    // otherwise the state is `State::Finish`.
                } else {
                    self.state = State::ReadDataPrewhere(rg)
                }

                self.output.push_data(Ok(data_block));
                return Ok(Event::NeedConsume);
            }
        }

        match self.state {
            State::Finish => Ok(Event::Finished),
            State::Prepare(_)
            | State::ReadDataPrewhere(_)
            | State::ReadDataRemain(_, _)
            | State::PrewhereFilter(_, _)
            | State::Deserialize(_, _, _) => Ok(Event::Sync),
            State::Generated(_, _) => Err(ErrorCode::Internal("It's a bug.")),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::Prepare(Some(part)) => {
                // Read meta first;
                let part = ParquetPart::from_part(&part)?;
                let meta = ParquetReader::read_meta(&part.location)?;
                self.state = State::ReadDataPrewhere(RowGroupState::new(
                    part.location.clone(),
                    meta.row_groups,
                ));
                Ok(())
            }
            State::ReadDataPrewhere(rg) => {
                let chunks = self
                    .prewhere_reader
                    .sync_read_columns_data(&rg.location, rg.get())?;
                if self.prewhere_filter.is_some() {
                    self.state = State::PrewhereFilter(rg, chunks);
                } else {
                    // If there is no prewhere filter, it means there is only the prewhere reader.
                    assert!(self.remain_reader.is_none());
                    // So all the needed columns are read.
                    self.state = State::Deserialize(rg, chunks, None)
                }
                Ok(())
            }
            State::ReadDataRemain(rg, prewhere_data) => {
                if let Some(remain_reader) = self.remain_reader.as_ref() {
                    let chunks = remain_reader.sync_read_columns_data(&rg.location, rg.get())?;
                    self.state = State::Deserialize(rg, chunks, Some(prewhere_data));
                    Ok(())
                } else {
                    Err(ErrorCode::Internal("It's a bug. No remain reader"))
                }
            }
            State::PrewhereFilter(rg, chunks) => self.do_prewhere_filter(rg, chunks),
            State::Deserialize(rg, chunks, prewhere_data) => {
                self.do_deserialize(rg, chunks, prewhere_data)
            }
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }
}
