// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::sync::Arc;

use common_arrow::parquet::metadata::FileMetaData;
use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PartInfoPtr;

use crate::catalogs::hive::hive_table_source::State::Generated;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;
use crate::sessions::TableContext;
use crate::storages::hive::HiveParquetBlockReader;

enum State {
    ReadData(PartInfoPtr),
    Deserialize(FileMetaData, Vec<Vec<u8>>),
    Generated(Option<PartInfoPtr>, DataBlock),
    Finish,
}

pub struct HiveTableSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    scan_progress: Arc<Progress>,
    block_reader: Arc<HiveParquetBlockReader>,
    output: Arc<OutputPort>,
}

impl HiveTableSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        block_reader: Arc<HiveParquetBlockReader>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        let mut partitions = ctx.try_get_partitions(1)?;
        match partitions.is_empty() {
            true => Ok(ProcessorPtr::create(Box::new(HiveTableSource {
                ctx,
                output,
                block_reader,
                scan_progress,
                state: State::Finish,
            }))),
            false => Ok(ProcessorPtr::create(Box::new(HiveTableSource {
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
impl Processor for HiveTableSource {
    fn name(&self) -> &'static str {
        "HiveEngineSource"
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
            State::Deserialize(meta, chunks) => {
                let data_block = self.block_reader.deserialize(chunks, meta)?;
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
                let (meta, chunks) = self.block_reader.read_columns_data(part.clone()).await?;
                self.state = State::Deserialize(meta, chunks);
                Ok(())
            }
            _ => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }
}
