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
    // Part(Option<PartInfoPtr>),
    // ReadDataRemain(PartInfoPtr, PrewhereData),
    // PrewhereFilter(PartInfoPtr, DataChunks),
    // Deserialize(PartInfoPtr, DataChunks, Option<PrewhereData>),
    None,
    Part(PartInfoPtr),
    ReadData(RowGroupDeserializer),
    Generated(RowGroupDeserializer, DataBlock),
    Finish,
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

    support_blocking: bool,
    cnt : usize,
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
        Ok(ProcessorPtr::create(Box::new(FuseTableSource {
            ctx,
            output,
            scan_progress,
            state: State::None,
            output_reader,
            prewhere_reader,
            prewhere_filter,
            remain_reader,
            support_blocking,
            cnt: 0
        })))
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
        if matches!(self.state, State::None) {
            self.state = match self.ctx.try_get_part() {
                None => State::Finish,
                Some(part) => State::Part(part),
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
            if let Generated(se, data_block) = std::mem::replace(&mut self.state, State::None) {
                self.state = State::ReadData(se);

                self.output.push_data(Ok(data_block));
                return Ok(Event::NeedConsume);
            }
        }

        match &self.state {
            State::Finish => Ok(Event::Finished),
            State::Part(_) => {
                if self.support_blocking {
                    Ok(Event::Sync)
                } else {
                    Ok(Event::Async)
                }
            }
            State::ReadData(de) => {
                if self.support_blocking {
                    Ok(Event::Sync)
                } else {
                    Ok(Event::Async)
                }
            }
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::Part(part) => {
                let chunks = self.output_reader.sync_read_columns_data(part.clone())?;
                
                let se = self.output_reader.deserialize(part.clone(), chunks)?;
                self.state = State::ReadData(se);
                Ok(())
            }
            State::ReadData(mut de) => {
                let block = self.output_reader.next_block(&mut de)?;
                self.state = match block {
                    Some(block) => {
                        self.cnt += 1;
                        println!("got next count: {}, block: {}, total: {}",self.cnt, block.num_rows(), de.num_rows());
                        State::Generated(de, block)
                    },
                    None => State::None,
                };
                Ok(())
            }
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::Part(part) => {
                let chunks = self.output_reader.read_columns_data(part.clone()).await?;
                let se = self.output_reader.deserialize(part.clone(), chunks)?;
                self.state = State::ReadData(se);
                Ok(())
            }
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }
}
