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

use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::ColumnRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionContext;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_sql::evaluator::EvalNode;

use crate::io::BlockReader;
use crate::operations::State::Generated;

type DataChunks = Vec<(usize, Vec<u8>)>;

pub struct PrewhereData {
    data_block: DataBlock,
    filter: ColumnRef,
}

pub enum State {
    ReadDataPrewhere(Option<PartInfoPtr>),
    ReadDataRemain(PartInfoPtr, PrewhereData),
    DeserAndFilter(PartInfoPtr),
    Generated(Option<PartInfoPtr>, DataBlock),
    Finish,
}

struct Deserializers {
    prewhere: RowGroupDeserializer,
    remain: Option<RowGroupDeserializer>,
}

pub struct FuseTableSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    scan_progress: Arc<Progress>,
    output: Arc<OutputPort>,
    output_reader: Arc<BlockReader>,

    prewhere_reader: Arc<BlockReader>,
    prewhere_filter: Arc<Option<EvalNode>>,
    remain_reader: Arc<Option<BlockReader>>,

    deserializers: Option<Deserializers>,

    support_blocking: bool,
    max_block_size: usize,
}

impl FuseTableSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        output_reader: Arc<BlockReader>,
        prewhere_reader: Arc<BlockReader>,
        prewhere_filter: Arc<Option<EvalNode>>,
        remain_reader: Arc<Option<BlockReader>>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        let support_blocking = prewhere_reader.support_blocking_api();
        let max_block_size = ctx.get_settings().get_max_block_size()? as usize;
        Ok(ProcessorPtr::create(Box::new(FuseTableSource {
            ctx,
            output,
            scan_progress,
            state: State::ReadDataPrewhere(None),
            output_reader,
            prewhere_reader,
            prewhere_filter,
            remain_reader,
            deserializers: None,
            support_blocking,
            max_block_size,
        })))
    }

    fn generate_one_block(&mut self, part: PartInfoPtr, block: DataBlock) -> Result<()> {
        // resort and prune columns
        let block = block.resort(self.output_reader.schema())?;
        self.state = State::Generated(Some(part), block);
        Ok(())
    }

    fn init_remain_deser_and_generate_one_block(
        &mut self,
        part: PartInfoPtr,
        chunks: DataChunks,
        prewhere_data: PrewhereData,
    ) -> Result<()> {
        let remain_reader = self.remain_reader.as_ref().as_ref().unwrap();
        let mut de = remain_reader.get_deserializer(
            &part,
            chunks,
            Arc::new(|_, _| true),
            Some(self.max_block_size),
        )?;

        // Generate one block because prewhere has already produced one block.
        let remain_block = remain_reader.try_next_block(&mut de)?.ok_or_else(|| {
            ErrorCode::Internal("It's a bug. Should get one block from remain reader.")
        })?;

        // Unwrap safety: this function is only called after ReadDataPrewhere.
        let mut desers = self.deserializers.as_mut().unwrap();
        desers.remain = Some(de);

        let PrewhereData {
            data_block: prewhere_block,
            filter,
        } = prewhere_data;

        self.combine_and_generate_filtered_block(part, prewhere_block, Some(remain_block), filter)
    }

    fn combine_and_generate_filtered_block(
        &mut self,
        part: PartInfoPtr,
        prewhere_block: DataBlock,
        remain_block: Option<DataBlock>,
        filter: ColumnRef,
    ) -> Result<()> {
        let mut block = prewhere_block;
        if let Some(remain_block) = remain_block {
            for (col, field) in remain_block
                .columns()
                .iter()
                .zip(remain_block.schema().fields())
            {
                block = block.add_column(col.clone(), field.clone())?;
            }
        }
        let progress_values = ProgressValues {
            rows: block.num_rows(),
            bytes: block.memory_size(),
        };
        self.scan_progress.incr(&progress_values);
        let block = DataBlock::filter_block(block, &filter)?;
        self.generate_one_block(part, block)
    }
}

#[async_trait::async_trait]
impl Processor for FuseTableSource {
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
            if let Generated(part, data_block) = std::mem::replace(&mut self.state, State::Finish) {
                self.state = match part {
                    None => State::Finish,
                    Some(part) => State::DeserAndFilter(part),
                };

                self.output.push_data(Ok(data_block));
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
            State::DeserAndFilter(_) => Ok(Event::Sync),
            State::Generated(_, _) => Err(ErrorCode::Internal("It's a bug.")),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::DeserAndFilter(part) => {
                // Unwrap safety: the prewhere deserilizer is initialized.
                let prewhere_deser = &mut self.deserializers.as_mut().unwrap().prewhere;
                // Deserialize one block of prewhere columns.
                let data_block = self.prewhere_reader.try_next_block(prewhere_deser)?;
                if data_block.is_none() {
                    // Current part is finished.
                    let next_part = self.ctx.try_get_part();
                    self.state = match next_part {
                        None => State::Finish,
                        part => State::ReadDataPrewhere(part),
                    };
                    return Ok(());
                }
                let data_block = data_block.unwrap();

                if let Some(filter) = self.prewhere_filter.as_ref() {
                    // do filter
                    let res = filter
                        .eval(&FunctionContext::default(), &data_block)?
                        .vector;
                    let filter = DataBlock::cast_to_nonull_boolean(&res)?;

                    // TODO: enable the shortcut below:
                    // let all_filtered = !DataBlock::filter_exists(&filter)?;

                    if self.remain_reader.is_none() {
                        // shortcut, we don't need to read remain data
                        self.combine_and_generate_filtered_block(part, data_block, None, filter)?;
                    } else if self.deserializers.as_ref().unwrap().remain.is_none() {
                        // If the remain deserializer is not initialized, we should read remain columns and init the deserializer.
                        // Unwrap safety: the prewhere deserilizer is initialized.
                        self.state =
                            State::ReadDataRemain(part, PrewhereData { data_block, filter });
                    } else {
                        let remain_reader = self.remain_reader.as_ref().as_ref().unwrap();
                        let remain_deser = self
                            .deserializers
                            .as_mut()
                            .unwrap()
                            .remain
                            .as_mut()
                            .unwrap();
                        let remain_block =
                            remain_reader.try_next_block(remain_deser)?.ok_or_else(|| {
                                ErrorCode::Internal(
                                    "It's a bug. Should get one block from remain reader.",
                                )
                            })?;
                        self.combine_and_generate_filtered_block(
                            part,
                            data_block,
                            Some(remain_block),
                            filter,
                        )?;
                    }
                } else {
                    // no filters. This means all required columns are contained in `self.prewhere_reader`.
                    let progress_values = ProgressValues {
                        rows: data_block.num_rows(),
                        bytes: data_block.memory_size(),
                    };
                    self.scan_progress.incr(&progress_values);
                    self.generate_one_block(part, data_block)?;
                }
                Ok(())
            }

            State::ReadDataPrewhere(Some(part)) => {
                let chunks = self.prewhere_reader.sync_read_columns_data(part.clone())?;
                let de = self.prewhere_reader.get_deserializer(
                    &part,
                    chunks,
                    Arc::new(|_, _| true),
                    Some(self.max_block_size),
                )?;
                self.deserializers = Some(Deserializers {
                    prewhere: de,
                    remain: None,
                });
                self.state = State::DeserAndFilter(part);
                Ok(())
            }
            State::ReadDataRemain(part, prewhere_data) => {
                // Unwrap safety: only if self.remain_reader is not None will get into this branch.
                let remain_reader = self.remain_reader.as_ref().as_ref().unwrap();
                let chunks = remain_reader.sync_read_columns_data(part.clone())?;
                self.init_remain_deser_and_generate_one_block(part, chunks, prewhere_data)
            }
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadDataPrewhere(Some(part)) => {
                let chunks = self.prewhere_reader.read_columns_data(part.clone()).await?;
                let de = self.prewhere_reader.get_deserializer(
                    &part,
                    chunks,
                    Arc::new(|_, _| true),
                    Some(self.max_block_size),
                )?;
                self.deserializers = Some(Deserializers {
                    prewhere: de,
                    remain: None,
                });
                self.state = State::DeserAndFilter(part);
                Ok(())
            }
            State::ReadDataRemain(part, prewhere_data) => {
                // Unwrap safety: only if self.remain_reader is not None will get into this branch.
                let remain_reader = self.remain_reader.as_ref().as_ref().unwrap();
                let chunks = remain_reader.read_columns_data(part.clone()).await?;
                self.init_remain_deser_and_generate_one_block(part, chunks, prewhere_data)
            }
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }
}
