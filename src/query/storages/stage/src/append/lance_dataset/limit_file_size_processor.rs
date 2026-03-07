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
use std::sync::Arc;

use databend_common_ast::ast::CopyIntoLocationOptions;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;

use crate::append::column_based::block_batch::BlockBatch;

struct SizedBlock {
    block: DataBlock,
    rows: usize,
    memory_size: usize,
}

pub(super) struct LimitFileSizeProcessor {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    // Each BlockBatch will be written as one .lance file.
    // Rows and size of a BlockBatch are limited by max_file_size and max_rows_per_file.
    max_file_size: usize,
    max_rows_per_file: usize,
    max_rows_per_block: usize,

    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,

    // since we only output one BlockBatch each time, the remaining blocks is kept here.
    // remember to flush it when input is finished
    blocks: Vec<SizedBlock>,
}

impl LimitFileSizeProcessor {
    pub(super) fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        max_file_size: usize,
        max_rows_per_file: usize,
        max_rows_per_block: usize,
    ) -> Result<ProcessorPtr> {
        let p = Self {
            input,
            output,
            max_file_size,
            max_rows_per_file,
            max_rows_per_block,
            input_data: None,
            output_data: None,
            blocks: Vec::new(),
        };
        Ok(ProcessorPtr::create(Box::new(p)))
    }

    pub fn build(
        pipeline: &mut Pipeline,
        mem_limit: usize,
        max_threads: usize,
        options: &CopyIntoLocationOptions,
    ) -> Result<Option<usize>> {
        let is_single = options.single;
        let max_file_size = options.max_file_size;
        let max_rows_per_file = 1024 * 1024;
        let max_rows_per_block = 128 * 1024;
        let mem_limit = mem_limit / 2;
        pipeline.try_resize(1)?;
        let max_file_size = if is_single {
            None
        } else {
            let max_file_size = if max_file_size == 0 {
                64 * 1024 * 1024
            } else {
                max_file_size.min(mem_limit)
            };
            pipeline.add_transform(|input, output| {
                Self::try_create(
                    input,
                    output,
                    max_file_size,
                    max_rows_per_file,
                    max_rows_per_block,
                )
            })?;

            let max_threads = max_threads.min(mem_limit / max_file_size).max(1);
            pipeline.try_resize(max_threads)?;
            Some(max_file_size)
        };
        Ok(max_file_size)
    }

    fn create_block_batch(blocks: Vec<SizedBlock>) -> DataBlock {
        BlockBatch::create_block(blocks.into_iter().map(|b| b.block).collect())
    }

    fn prefix_memory_for_rows(base: usize, remainder: usize, rows: usize) -> usize {
        base.saturating_mul(rows)
            .saturating_add(rows.min(remainder))
    }

    fn estimate_slice_memory(base: usize, remainder: usize, start: usize, end: usize) -> usize {
        Self::prefix_memory_for_rows(base, remainder, end)
            .saturating_sub(Self::prefix_memory_for_rows(base, remainder, start))
    }

    fn split_input_block(&self, block: DataBlock) -> Vec<SizedBlock> {
        let total_rows = block.num_rows();
        let total_memory_size = block.memory_size();
        let max_rows_per_slice = self.max_rows_per_block.min(self.max_rows_per_file).max(1);
        if total_rows <= max_rows_per_slice {
            return vec![SizedBlock {
                block,
                rows: total_rows,
                memory_size: total_memory_size,
            }];
        }

        // Record memory_size / rows from input block and estimate memory usage for each slice.
        let bytes_per_row = total_memory_size / total_rows;
        let bytes_remainder = total_memory_size % total_rows;

        let mut slices = Vec::with_capacity(total_rows.div_ceil(max_rows_per_slice));
        let mut offset = 0;
        while offset < total_rows {
            let mut slice_rows = (total_rows - offset).min(max_rows_per_slice);
            if slice_rows > 1 {
                let max_mem = self.max_file_size;
                let estimated = Self::estimate_slice_memory(
                    bytes_per_row,
                    bytes_remainder,
                    offset,
                    offset + slice_rows,
                );
                if estimated > max_mem {
                    let mut left = 1usize;
                    let mut right = slice_rows;
                    while left < right {
                        let mid = (left + right).div_ceil(2);
                        let mid_mem = Self::estimate_slice_memory(
                            bytes_per_row,
                            bytes_remainder,
                            offset,
                            offset + mid,
                        );
                        if mid_mem <= max_mem {
                            left = mid;
                        } else {
                            right = mid - 1;
                        }
                    }
                    slice_rows = left.max(1);
                }
            }

            let end = offset + slice_rows;
            let slice_memory_size =
                Self::estimate_slice_memory(bytes_per_row, bytes_remainder, offset, end);
            slices.push(SizedBlock {
                block: block.slice(offset..end),
                rows: slice_rows,
                memory_size: slice_memory_size,
            });
            offset = end;
        }

        slices
    }
}

impl Processor for LimitFileSizeProcessor {
    fn name(&self) -> String {
        String::from("LimitFileSizeProcessor")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            Ok(Event::Finished)
        } else if !self.output.can_push() {
            self.input.set_not_need_data();
            Ok(Event::NeedConsume)
        } else {
            match self.output_data.take() {
                Some(data) => {
                    self.output.push_data(Ok(data));
                    Ok(Event::NeedConsume)
                }
                None => {
                    if self.input_data.is_some() {
                        Ok(Event::Sync)
                    } else if self.input.has_data() {
                        self.input_data = Some(self.input.pull_data().unwrap()?);
                        Ok(Event::Sync)
                    } else if self.input.is_finished() {
                        if self.blocks.is_empty() {
                            self.output.finish();
                            Ok(Event::Finished)
                        } else {
                            // flush the remaining blocks
                            let blocks = std::mem::take(&mut self.blocks);
                            self.output.push_data(Ok(Self::create_block_batch(blocks)));
                            Ok(Event::NeedConsume)
                        }
                    } else {
                        self.input.set_need_data();
                        Ok(Event::NeedData)
                    }
                }
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        assert!(self.input_data.is_some());
        assert!(self.output_data.is_none());
        let input = self.input_data.take().unwrap();
        if input.is_empty() {
            return Ok(());
        }
        let mut blocks = std::mem::take(&mut self.blocks);
        blocks.extend(self.split_input_block(input));
        let mut break_point = blocks.len();
        let mut size = 0usize;
        let mut rows = 0usize;
        for (i, b) in blocks.iter().enumerate() {
            let next_size = size.saturating_add(b.memory_size);
            let next_rows = rows.saturating_add(b.rows);
            if next_size > self.max_file_size || next_rows > self.max_rows_per_file {
                break_point = i;
                break;
            }
            size = next_size;
            rows = next_rows;
        }
        if break_point == blocks.len() {
            self.blocks = blocks;
        } else if break_point == 0 {
            // Keep making progress even if one block is already larger than thresholds.
            let remain = blocks.split_off(1);
            self.output_data = Some(Self::create_block_batch(blocks));
            self.blocks = remain;
        } else {
            let remain = blocks.split_off(break_point);
            self.output_data = Some(Self::create_block_batch(blocks));
            self.blocks = remain;
        }
        Ok(())
    }
}
