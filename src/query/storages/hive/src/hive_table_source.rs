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

use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_planners::PartInfoPtr;

use crate::hive_parquet_block_reader::HiveParquetBlockReader;
use crate::HiveBlocks;
use crate::HivePartInfo;

enum State {
    ReadMeta(PartInfoPtr),
    ReadData(HiveBlocks),
    Deserialize(HiveBlocks, Vec<Vec<u8>>),
    Generated(HiveBlocks, DataBlock),
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
                state: State::ReadMeta(partitions.remove(0)),
            }))),
        }
    }

    fn try_get_partitions(&mut self) -> Result<()> {
        let partitions = self.ctx.try_get_partitions(1)?;
        match partitions.is_empty() {
            true => {
                self.state = State::Finish;
            }
            false => {
                self.state = State::ReadMeta(partitions[0].clone());
            }
        }

        Ok(())
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
        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if matches!(self.state, State::Finish) {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if matches!(self.state, State::Generated(_, _)) {
            if let State::Generated(mut hive_blocks, data_block) =
                std::mem::replace(&mut self.state, State::Finish)
            {
                self.output.push_data(Ok(data_block));

                hive_blocks.advance();
                match hive_blocks.has_blocks() {
                    true => {
                        self.state = State::ReadData(hive_blocks);
                    }
                    false => {
                        self.try_get_partitions()?;
                    }
                }

                return Ok(Event::NeedConsume);
            }
        }

        match self.state {
            State::Finish => Ok(Event::Finished),
            State::ReadMeta(_) => Ok(Event::Async),
            State::ReadData(_) => Ok(Event::Async),
            State::Deserialize(_, _) => Ok(Event::Sync),
            State::Generated(_, _) => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::Deserialize(hive_blocks, chunks) => {
                let row_group = hive_blocks.get_current_row_group_meta_data();
                let part = hive_blocks.get_part_info();
                let data_block = self.block_reader.deserialize(chunks, row_group, part)?;
                let progress_values = ProgressValues {
                    rows: data_block.num_rows(),
                    bytes: data_block.memory_size(),
                };
                self.scan_progress.incr(&progress_values);
                self.state = State::Generated(hive_blocks, data_block);
                Ok(())
            }
            _ => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadMeta(part) => {
                let part = HivePartInfo::from_part(&part)?;
                let file_meta = self.block_reader.read_meta_data(&part.filename).await?;
                let mut hive_blocks = HiveBlocks::create(file_meta, part.clone());
                match hive_blocks.prune() {
                    true => {
                        self.state = State::ReadData(hive_blocks);
                    }
                    false => {
                        self.try_get_partitions()?;
                    }
                }
                Ok(())
            }
            State::ReadData(hive_blocks) => {
                let row_group = hive_blocks.get_current_row_group_meta_data();
                let part = hive_blocks.get_part_info();
                let chunks = self
                    .block_reader
                    .read_columns_data(row_group, &part)
                    .await?;
                self.state = State::Deserialize(hive_blocks, chunks);
                Ok(())
            }
            _ => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }
}
