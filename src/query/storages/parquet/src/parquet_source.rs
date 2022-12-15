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
use common_datablocks::DataBlock;
use common_datavalues::BooleanColumn;
use common_datavalues::ColumnRef;
use common_datavalues::DataSchemaRef;
use common_datavalues::Series;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionContext;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_sql::evaluator::EvalNode;

use crate::parquet_part::ParquetRowGroupPart;
use crate::parquet_reader::IndexedChunk;
use crate::parquet_reader::ParquetReader;
use crate::parquet_source::State::Generated;

struct PrewhereData {
    data_block: DataBlock,
    filter: ColumnRef,
}

/// The states for [`ParquetSource`]. The states will recycle for each row group of a parquet file.
enum State {
    ReadDataPrewhere(Option<PartInfoPtr>),
    ReadDataRemain(PartInfoPtr, PrewhereData),
    PrewhereFilter(PartInfoPtr, Vec<IndexedChunk>),
    Deserialize(PartInfoPtr, Vec<IndexedChunk>, Option<PrewhereData>),
    Generated(Option<PartInfoPtr>, DataBlock),
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
            state: State::ReadDataPrewhere(None),
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

    fn do_prewhere_filter(&mut self, part: PartInfoPtr, chunks: Vec<IndexedChunk>) -> Result<()> {
        let rg_part = ParquetRowGroupPart::from_part(&part)?;
        // deserialize prewhere data block first
        let data_block = self.prewhere_reader.deserialize(rg_part, chunks, None)?;
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
                self.state = Generated(
                    self.ctx.try_get_part(),
                    DataBlock::empty_with_schema(self.output_schema()),
                );
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
                self.state =
                    Generated(self.ctx.try_get_part(), block.resort(self.output_schema())?);
            } else {
                let data_block = DataBlock::filter_block(data_block, &filter)?;
                self.state = State::ReadDataRemain(part, PrewhereData { data_block, filter });
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
        chunks: Vec<IndexedChunk>,
        prewhere_data: Option<PrewhereData>,
    ) -> Result<()> {
        let rg_part = ParquetRowGroupPart::from_part(&part)?;
        let data_block = if let Some(PrewhereData {
            data_block: mut prewhere_blocks,
            filter,
        }) = prewhere_data
        {
            let block = if chunks.is_empty() {
                prewhere_blocks
            } else if let Some(remain_reader) = self.remain_reader.as_ref() {
                // filter is already converted to non-null boolean column
                let remain_block = if filter.is_const() && filter.get_bool(0)? {
                    // don't need filter
                    remain_reader.deserialize(rg_part, chunks, None)?
                } else {
                    let boolean_col = Series::check_get::<BooleanColumn>(&filter)?;
                    let bitmap = boolean_col.values();
                    if bitmap.unset_bits() == 0 {
                        // don't need filter
                        remain_reader.deserialize(rg_part, chunks, None)?
                    } else {
                        remain_reader.deserialize(rg_part, chunks, Some(bitmap.clone()))?
                    }
                };
                assert!(
                    prewhere_blocks.num_rows() == remain_block.num_rows(),
                    "prewhere and remain blocks should have same row number. (prewhere: {}, remain: {})",
                    prewhere_blocks.num_rows(),
                    remain_block.num_rows()
                );

                // Combine two blocks.
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
            block
        } else {
            // There is only prewhere reader.
            assert!(self.remain_reader.is_none());
            let block = self.prewhere_reader.deserialize(rg_part, chunks, None)?;
            let progress_values = ProgressValues {
                rows: block.num_rows(),
                bytes: block.memory_size(),
            };
            self.scan_progress.incr(&progress_values);

            block
        };

        self.state = State::Generated(
            self.ctx.try_get_part(),
            data_block.resort(self.output_schema())?,
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
            if let Generated(part, data_block) = std::mem::replace(&mut self.state, State::Finish) {
                if let Some(part) = part {
                    self.state = State::ReadDataPrewhere(Some(part));
                }
                self.output.push_data(Ok(data_block));
                return Ok(Event::NeedConsume);
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
                let chunks = self.prewhere_reader.sync_read_columns_data(rg_part)?;
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
                    let chunks = remain_reader.sync_read_columns_data(rg_part)?;
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
