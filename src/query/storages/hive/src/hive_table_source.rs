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

use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
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
    /// Read parquet file meta data
    /// IO bound
    ReadMeta(PartInfoPtr),

    /// Read blocks from data groups (without deserialization)
    /// IO bound
    ReadData(HiveBlocks),

    /// Deserialize block from the given data groups
    /// CPU bound
    Deserialize(HiveBlocks, RowGroupDeserializer),

    /// `(_, _, Some(_))` indicates that a data block is ready, and needs to be consumed
    ///
    /// `(_, _, None)` indicates that there are no more blocks left for the current row group of `HiveBlocks`
    Generated(HiveBlocks, RowGroupDeserializer, Option<DataBlock>),
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

        if matches!(self.state, State::Generated(_, _, _)) {
            if let State::Generated(mut hive_blocks, row_group_deserializer, maybe_data_block) =
                std::mem::replace(&mut self.state, State::Finish)
            {
                if let Some(data_block) = maybe_data_block {
                    // NOTE: data blocks are supposed to be pushed to down stream one by one (not in a batch)
                    self.output.push_data(Ok(data_block));
                    // blocks of current row group are not exhausted, transfer to Deserialize state for deserializing the next block
                    self.state = State::Deserialize(hive_blocks, row_group_deserializer);
                    return Ok(Event::NeedConsume);
                } else {
                    // no blocks left in the current row group, trying the next row group
                    hive_blocks.advance();
                    match hive_blocks.has_blocks() {
                        true => {
                            // other groups are also covered by this HiveBlock, try read them
                            self.state = State::ReadData(hive_blocks);
                        }
                        false => {
                            self.try_get_partitions()?;
                        }
                    }
                }
            }
        }

        match self.state {
            State::Finish => {
                self.output.finish();
                Ok(Event::Finished)
            }
            State::ReadMeta(_) => Ok(Event::Async),
            State::ReadData(_) => Ok(Event::Async),
            State::Deserialize(_, _) => Ok(Event::Sync),
            State::Generated(_, _, _) => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::Deserialize(hive_blocks, mut row_group_deserializer) => {
                let row_group = hive_blocks.get_current_row_group_meta_data();
                let part = hive_blocks.get_part_info();
                let maybe_data_block = self.block_reader.deserialize_next_block(
                    &mut row_group_deserializer,
                    row_group,
                    part,
                )?;
                if let Some(data_block) = &maybe_data_block {
                    let progress_values = ProgressValues {
                        rows: data_block.num_rows(),
                        bytes: data_block.memory_size(),
                    };
                    self.scan_progress.incr(&progress_values);
                }
                self.state =
                    State::Generated(hive_blocks, row_group_deserializer, maybe_data_block);
                Ok(())
            }
            _ => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadMeta(part) => {
                let part = HivePartInfo::from_part(&part)?;
                let file_meta = self
                    .block_reader
                    .read_meta_data(self.ctx.clone(), &part.filename)
                    .await?;
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
                let column_chunks = self
                    .block_reader
                    .read_columns_data(row_group, &part)
                    .await?;
                let row_group_deserializer =
                    RowGroupDeserializer::new(column_chunks, row_group.num_rows(), None);
                self.state = State::Deserialize(hive_blocks, row_group_deserializer);
                Ok(())
            }
            _ => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }
}
