// Copyright 2021 Datafuse Labs
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
use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
use databend_common_base::base::ProgressValues;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_io::constants::DEFAULT_BLOCK_ROW_COUNT;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_storage::MutationStatus;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use opendal::Operator;

use crate::io::BlockSerialization;
use crate::io::BlockWriter;
use crate::io::StreamBlockBuilder;
use crate::io::StreamBlockProperties;
use crate::FuseTable;
use crate::FUSE_OPT_KEY_ROW_PER_BLOCK;

#[allow(clippy::large_enum_variant)]
enum State {
    Consume,
    Serialize(DataBlock),
    Flush,
    Write(BlockSerialization),
}

pub struct TransformBlockWriter {
    state: State,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    properties: Arc<StreamBlockProperties>,

    builder: Option<StreamBlockBuilder>,
    dal: Operator,
    // Only used in multi table insert
    table_id: Option<u64>,

    max_block_size: usize,
    input_data: VecDeque<DataBlock>,
    output_data: Option<DataBlock>,
}

impl TransformBlockWriter {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table: &FuseTable,
        table_meta_timestamps: TableMetaTimestamps,
        with_tid: bool,
    ) -> Result<ProcessorPtr> {
        let max_block_size = std::cmp::min(
            ctx.get_settings().get_max_block_size()? as usize,
            table.get_option(FUSE_OPT_KEY_ROW_PER_BLOCK, DEFAULT_BLOCK_ROW_COUNT),
        );
        let properties = StreamBlockProperties::try_create(ctx, table, table_meta_timestamps)?;
        Ok(ProcessorPtr::create(Box::new(TransformBlockWriter {
            state: State::Consume,
            input,
            output,
            properties,
            builder: None,
            dal: table.get_operator(),
            table_id: if with_tid { Some(table.get_id()) } else { None },
            input_data: VecDeque::new(),
            output_data: None,
            max_block_size,
        })))
    }

    fn get_or_create_builder(&mut self) -> Result<&mut StreamBlockBuilder> {
        if self.builder.is_none() {
            self.builder = Some(StreamBlockBuilder::try_new_with_config(
                self.properties.clone(),
            )?);
        }
        Ok(self.builder.as_mut().unwrap())
    }
}

#[async_trait]
impl Processor for TransformBlockWriter {
    fn name(&self) -> String {
        "TransformBlockWriter".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::Serialize { .. } | State::Flush) {
            return Ok(Event::Sync);
        }

        if matches!(self.state, State::Write { .. }) {
            return Ok(Event::Async);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if let Some(block) = self.input_data.pop_front() {
            self.state = State::Serialize(block);
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            if self.builder.is_some() {
                self.state = State::Flush;
                return Ok(Event::Sync);
            }
            self.output.finish();
            return Ok(Event::Finished);
        }

        if !self.input.has_data() {
            self.input.set_need_data();
            return Ok(Event::NeedData);
        }

        let input_data = self.input.pull_data().unwrap()?;
        self.state = State::Serialize(input_data);
        Ok(Event::Sync)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::Serialize(block) => {
                // Check if the datablock is valid, this is needed to ensure data is correct
                block.check_valid()?;
                let blocks = block.split_by_rows_no_tail(self.max_block_size);
                let mut blocks = VecDeque::from(blocks);

                let builder = self.get_or_create_builder()?;
                while let Some(b) = blocks.pop_front() {
                    builder.write(b)?;

                    if builder.need_flush() {
                        self.state = State::Flush;

                        for left in blocks {
                            self.input_data.push_back(left);
                        }
                        return Ok(());
                    }
                }
            }
            State::Flush => {
                let builder = self.builder.take().unwrap();
                if !builder.is_empty() {
                    let serialized = builder.finish()?;
                    self.state = State::Write(serialized);
                }
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::Write(serialized) => {
                let block_meta = BlockWriter::write_down(&self.dal, serialized).await?;

                self.properties
                    .ctx
                    .get_write_progress()
                    .incr(&ProgressValues {
                        rows: block_meta.row_count as usize,
                        bytes: block_meta.block_size as usize,
                    });

                // appending new data block
                if let Some(tid) = self.table_id {
                    self.properties
                        .ctx
                        .update_multi_table_insert_status(tid, block_meta.row_count);
                } else {
                    self.properties.ctx.add_mutation_status(MutationStatus {
                        insert_rows: block_meta.row_count,
                        update_rows: 0,
                        deleted_rows: 0,
                    });
                }

                self.output_data = Some(DataBlock::empty_with_meta(Box::new(block_meta)));
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
