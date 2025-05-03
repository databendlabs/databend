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
use databend_common_metrics::storage::metrics_inc_recluster_write_block_nums;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storage::MutationStatus;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use opendal::Operator;

use crate::io::BlockSerialization;
use crate::io::BlockWriter;
use crate::io::StreamBlockBuilder;
use crate::io::StreamBlockProperties;
use crate::operations::MutationLogEntry;
use crate::operations::MutationLogs;
use crate::FuseTable;
use crate::FUSE_OPT_KEY_ROW_PER_BLOCK;

#[allow(clippy::large_enum_variant)]
enum State {
    Consume,
    Collect(DataBlock),
    Serialize,
    Finalize,
    Flush,
    Write(BlockSerialization),
}

pub struct TransformBlockWriter {
    state: State,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    kind: MutationKind,

    properties: Arc<StreamBlockProperties>,

    builder: Option<StreamBlockBuilder>,
    need_flush: bool,
    input_data_size: usize,
    input_num_rows: usize,

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
        kind: MutationKind,
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
            kind,
            properties,
            builder: None,
            dal: table.get_operator(),
            need_flush: false,
            table_id: if with_tid { Some(table.get_id()) } else { None },
            input_data: VecDeque::new(),
            input_data_size: 0,
            input_num_rows: 0,
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

    fn calc_max_block_size(&self, block: &DataBlock) -> usize {
        let min_bytes_per_block = self.properties.block_thresholds.min_bytes_per_block;
        let block_size = block.estimate_block_size();
        if block_size < min_bytes_per_block {
            return self.max_block_size;
        }
        let num_rows = block.num_rows();
        let average_row_size = block_size.div_ceil(num_rows);
        let max_rows = min_bytes_per_block.div_ceil(average_row_size);
        self.max_block_size.min(max_rows)
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
        match &self.state {
            State::Collect(_) | State::Serialize | State::Flush | State::Finalize => {
                return Ok(Event::Sync)
            }
            State::Write(_) => return Ok(Event::Async),
            _ => {}
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

        // To avoid tail fragments, flush only when the input is large enough.
        if self.need_flush
            && self
                .properties
                .block_thresholds
                .check_large_enough(self.input_num_rows, self.input_data_size)
        {
            self.state = State::Flush;
            return Ok(Event::Sync);
        }

        if !self.need_flush && !self.input_data.is_empty() {
            self.state = State::Serialize;
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let input_data = self.input.pull_data().unwrap()?;
            self.state = State::Collect(input_data);
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            if !self.input_data.is_empty() || self.builder.is_some() {
                self.state = State::Finalize;
                return Ok(Event::Sync);
            }
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::Collect(block) => {
                // Check if the datablock is valid, this is needed to ensure data is correct
                block.check_valid()?;
                self.input_data_size += block.estimate_block_size();
                self.input_num_rows += block.num_rows();
                let max_rows_per_block = self.calc_max_block_size(&block);
                let blocks = block.split_by_rows_no_tail(max_rows_per_block);
                self.input_data.extend(blocks);
            }
            State::Serialize => {
                while let Some(b) = self.input_data.pop_front() {
                    self.input_data_size -= b.estimate_block_size();
                    self.input_num_rows -= b.num_rows();

                    let builder = self.get_or_create_builder()?;
                    builder.write(b)?;

                    if builder.need_flush() {
                        self.need_flush = true;
                        return Ok(());
                    }
                }
            }
            State::Finalize => {
                while let Some(b) = self.input_data.pop_front() {
                    let builder = self.get_or_create_builder()?;
                    builder.write(b)?;
                }
                self.state = State::Flush;
            }
            State::Flush => {
                let builder = self.builder.take().unwrap();
                if !builder.is_empty() {
                    let serialized = builder.finish()?;
                    self.state = State::Write(serialized);
                }
                self.need_flush = false;
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::Write(serialized) => {
                let extended_block_meta = BlockWriter::write_down(&self.dal, serialized).await?;

                let bytes = if let Some(draft_virtual_block_meta) =
                    &extended_block_meta.draft_virtual_block_meta
                {
                    (extended_block_meta.block_meta.block_size
                        + draft_virtual_block_meta.virtual_column_size) as usize
                } else {
                    extended_block_meta.block_meta.block_size as usize
                };

                self.properties
                    .ctx
                    .get_write_progress()
                    .incr(&ProgressValues {
                        rows: extended_block_meta.block_meta.row_count as usize,
                        bytes,
                    });

                // appending new data block
                if let Some(tid) = self.table_id {
                    self.properties.ctx.update_multi_table_insert_status(
                        tid,
                        extended_block_meta.block_meta.row_count,
                    );
                } else {
                    self.properties.ctx.add_mutation_status(MutationStatus {
                        insert_rows: extended_block_meta.block_meta.row_count,
                        update_rows: 0,
                        deleted_rows: 0,
                    });
                }

                let output = if matches!(self.kind, MutationKind::Insert) {
                    DataBlock::empty_with_meta(Box::new(extended_block_meta))
                } else {
                    if matches!(self.kind, MutationKind::Recluster) {
                        metrics_inc_recluster_write_block_nums();
                    }

                    DataBlock::empty_with_meta(Box::new(MutationLogs {
                        entries: vec![MutationLogEntry::AppendBlock {
                            block_meta: Arc::new(extended_block_meta),
                        }],
                    }))
                };
                self.output_data = Some(output);
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
