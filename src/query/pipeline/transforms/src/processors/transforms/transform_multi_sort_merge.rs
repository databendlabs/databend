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
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::VecDeque;
use std::sync::Arc;

use common_arrow::arrow::compute::sort::row::RowConverter as ArrowRowConverter;
use common_arrow::arrow::compute::sort::row::Rows as ArrowRows;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::DateType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::types::TimestampType;
use common_expression::with_number_mapped_type;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::SortColumnDescription;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_core::Pipeline;

use super::sort::Cursor;
use super::sort::RowConverter;
use super::sort::Rows;
use super::sort::SimpleRowConverter;
use super::sort::SimpleRows;

pub fn try_add_multi_sort_merge(
    pipeline: &mut Pipeline,
    output_schema: DataSchemaRef,
    block_size: usize,
    limit: Option<usize>,
    sort_columns_descriptions: Vec<SortColumnDescription>,
) -> Result<()> {
    if pipeline.is_empty() {
        return Err(ErrorCode::Internal("Cannot resize empty pipe."));
    }

    match pipeline.output_len() {
        0 => Err(ErrorCode::Internal("Cannot resize empty pipe.")),
        1 => Ok(()),
        last_pipe_size => {
            let mut inputs_port = Vec::with_capacity(last_pipe_size);
            for _ in 0..last_pipe_size {
                inputs_port.push(InputPort::create());
            }
            let output_port = OutputPort::create();
            let processor = create_processor(
                inputs_port.clone(),
                output_port.clone(),
                output_schema,
                block_size,
                limit,
                sort_columns_descriptions,
            )?;

            pipeline.add_pipe(Pipe::create(inputs_port.len(), 1, vec![PipeItem::create(
                processor,
                inputs_port,
                vec![output_port],
            )]));

            Ok(())
        }
    }
}

fn create_processor(
    inputs: Vec<Arc<InputPort>>,
    output: Arc<OutputPort>,
    output_schema: DataSchemaRef,
    block_size: usize,
    limit: Option<usize>,
    sort_columns_descriptions: Vec<SortColumnDescription>,
) -> Result<ProcessorPtr> {
    Ok(if sort_columns_descriptions.len() == 1 {
        let sort_type = output_schema
            .field(sort_columns_descriptions[0].offset)
            .data_type();
        match sort_type {
            DataType::Number(num_ty) => with_number_mapped_type!(|NUM_TYPE| match num_ty {
                NumberDataType::NUM_TYPE =>
                    ProcessorPtr::create(Box::new(MultiSortMergeProcessor::<
                        SimpleRows<NumberType<NUM_TYPE>>,
                        SimpleRowConverter<NumberType<NUM_TYPE>>,
                    >::create(
                        inputs,
                        output,
                        output_schema,
                        block_size,
                        limit,
                        sort_columns_descriptions,
                    )?)),
            }),
            DataType::Date => ProcessorPtr::create(Box::new(MultiSortMergeProcessor::<
                SimpleRows<DateType>,
                SimpleRowConverter<DateType>,
            >::create(
                inputs,
                output,
                output_schema,
                block_size,
                limit,
                sort_columns_descriptions,
            )?)),
            DataType::Timestamp => ProcessorPtr::create(Box::new(MultiSortMergeProcessor::<
                SimpleRows<TimestampType>,
                SimpleRowConverter<TimestampType>,
            >::create(
                inputs,
                output,
                output_schema,
                block_size,
                limit,
                sort_columns_descriptions,
            )?)),
            DataType::String => ProcessorPtr::create(Box::new(MultiSortMergeProcessor::<
                SimpleRows<StringType>,
                SimpleRowConverter<StringType>,
            >::create(
                inputs,
                output,
                output_schema,
                block_size,
                limit,
                sort_columns_descriptions,
            )?)),
            _ => ProcessorPtr::create(Box::new(MultiSortMergeProcessor::<
                ArrowRows,
                ArrowRowConverter,
            >::create(
                inputs,
                output,
                output_schema,
                block_size,
                limit,
                sort_columns_descriptions,
            )?)),
        }
    } else {
        ProcessorPtr::create(Box::new(MultiSortMergeProcessor::<
            ArrowRows,
            ArrowRowConverter,
        >::create(
            inputs,
            output,
            output_schema,
            block_size,
            limit,
            sort_columns_descriptions,
        )?))
    })
}

/// TransformMultiSortMerge is a processor with multiple input ports;
pub struct MultiSortMergeProcessor<R, Converter>
where
    R: Rows,
    Converter: RowConverter<R>,
{
    /// Data from inputs (every input is sorted)
    inputs: Vec<Arc<InputPort>>,
    output: Arc<OutputPort>,
    output_schema: DataSchemaRef,
    /// Sort fields' indices in `output_schema`
    sort_field_indices: Vec<usize>,

    // Parameters
    block_size: usize,
    limit: Option<usize>,

    /// For each input port, maintain a dequeue of data blocks.
    blocks: Vec<VecDeque<DataBlock>>,
    /// Maintain a flag for each input denoting if the current cursor has finished
    /// and needs to pull data from input.
    cursor_finished: Vec<bool>,
    /// The accumulated rows for the next output data block.
    ///
    /// Data format: (input_index, block_index, row_index)
    in_progress_rows: Vec<(usize, usize, usize)>,
    /// Heap that yields [`Cursor`] in increasing order.
    heap: BinaryHeap<Reverse<Cursor<R>>>,
    /// If the input port is finished.
    input_finished: Vec<bool>,
    /// Used to convert columns to rows.
    row_converter: Converter,

    state: ProcessorState,
}

impl<R, Converter> MultiSortMergeProcessor<R, Converter>
where
    R: Rows,
    Converter: RowConverter<R>,
{
    pub fn create(
        inputs: Vec<Arc<InputPort>>,
        output: Arc<OutputPort>,
        output_schema: DataSchemaRef,
        block_size: usize,
        limit: Option<usize>,
        sort_columns_descriptions: Vec<SortColumnDescription>,
    ) -> Result<Self> {
        let input_size = inputs.len();
        let sort_field_indices = sort_columns_descriptions
            .iter()
            .map(|d| d.offset)
            .collect::<Vec<_>>();
        let row_converter = Converter::create(sort_columns_descriptions, output_schema.clone())?;
        Ok(Self {
            inputs,
            output,
            output_schema,
            sort_field_indices,
            block_size,
            limit,
            blocks: vec![VecDeque::with_capacity(2); input_size],
            heap: BinaryHeap::with_capacity(input_size),
            in_progress_rows: vec![],
            cursor_finished: vec![true; input_size],
            input_finished: vec![false; input_size],
            row_converter,
            state: ProcessorState::Consume,
        })
    }

    fn get_data_blocks(&mut self) -> Result<Vec<(usize, DataBlock)>> {
        let mut data = Vec::new();
        for (i, input) in self.inputs.iter().enumerate() {
            if input.is_finished() {
                self.input_finished[i] = true;
                continue;
            }

            input.set_need_data();
            if self.cursor_finished[i] && input.has_data() {
                data.push((i, input.pull_data().unwrap()?));
            }
        }
        Ok(data)
    }

    fn nums_active_inputs(&self) -> usize {
        self.input_finished
            .iter()
            .zip(self.cursor_finished.iter())
            .filter(|(f, c)| !**f || !**c)
            .count()
    }

    // Return if need output
    #[inline]
    fn drain_cursor(&mut self, mut cursor: Cursor<R>) -> bool {
        let input_index = cursor.input_index;
        let block_index = self.blocks[input_index].len() - 1;
        while !cursor.is_finished() {
            self.in_progress_rows
                .push((input_index, block_index, cursor.advance()));
            if let Some(limit) = self.limit {
                if self.in_progress_rows.len() == limit {
                    return true;
                }
            }
        }
        // We have read all rows of this block, need to read a new one.
        self.cursor_finished[input_index] = true;
        false
    }

    fn drain_heap(&mut self) {
        let nums_active_inputs = self.nums_active_inputs();
        let mut need_output = false;
        // Need to pop data to in_progress_rows.
        // Use `>=` because some of the input ports may be finished, but the data is still in the heap.
        while self.heap.len() >= nums_active_inputs && !need_output {
            match self.heap.pop() {
                Some(Reverse(mut cursor)) => {
                    let input_index = cursor.input_index;
                    if self.heap.is_empty() {
                        // If there is no other block in the heap, we can drain the whole block.
                        need_output = self.drain_cursor(cursor);
                    } else {
                        let next_cursor = &self.heap.peek().unwrap().0;
                        // If the last row of current block is smaller than the next cursor,
                        // we can drain the whole block.
                        if cursor.last().le(&next_cursor.current()) {
                            need_output = self.drain_cursor(cursor);
                        } else {
                            let block_index = self.blocks[input_index].len() - 1;
                            while !cursor.is_finished() && cursor.le(next_cursor) {
                                // If the cursor is smaller than the next cursor, don't need to push the cursor back to the heap.
                                self.in_progress_rows.push((
                                    input_index,
                                    block_index,
                                    cursor.advance(),
                                ));
                                if let Some(limit) = self.limit {
                                    if self.in_progress_rows.len() == limit {
                                        need_output = true;
                                        break;
                                    }
                                }
                            }
                            if !cursor.is_finished() {
                                self.heap.push(Reverse(cursor));
                            } else {
                                // We have read all rows of this block, need to read a new one.
                                self.cursor_finished[input_index] = true;
                            }
                        }
                    }
                    // Reach the block size, need to output.
                    if self.in_progress_rows.len() >= self.block_size {
                        need_output = true;
                        break;
                    }
                    if self.cursor_finished[input_index] && !self.input_finished[input_index] {
                        // Correctness: if input is not finished, we need to pull more data,
                        // or we can continue this loop.
                        break;
                    }
                }
                None => {
                    // Special case: self.heap.len() == 0 && nums_active_inputs == 0.
                    // `self.in_progress_rows` cannot be empty.
                    // If reach here, it means that all inputs are finished but `self.heap` is not empty before the while loop.
                    // Therefore, when reach here, data in `self.heap` is all drained into `self.in_progress_rows`.
                    debug_assert!(!self.in_progress_rows.is_empty());
                    self.state = ProcessorState::Output;
                    break;
                }
            }
        }
        if need_output {
            self.state = ProcessorState::Output;
        }
    }

    /// Drain `self.in_progress_rows` to build a output data block.
    fn build_block(&mut self) -> Result<DataBlock> {
        let num_rows = self.in_progress_rows.len();
        debug_assert!(num_rows > 0);

        let mut blocks_num_pre_sum = Vec::with_capacity(self.blocks.len());
        let mut len = 0;
        for block in self.blocks.iter() {
            blocks_num_pre_sum.push(len);
            len += block.len();
        }

        // Compute the indices of the output block.
        let first_row = &self.in_progress_rows[0];
        let mut index = blocks_num_pre_sum[first_row.0] + first_row.1;
        let mut start_row_index = first_row.2;
        let mut end_row_index = start_row_index + 1;
        let mut indices = Vec::new();
        for row in self.in_progress_rows.iter().skip(1) {
            let next_index = blocks_num_pre_sum[row.0] + row.1;
            if next_index == index {
                // Within a same block.
                end_row_index += 1;
                continue;
            }
            // next_index != index
            // Record a range in the block.
            indices.push((index, start_row_index, end_row_index - start_row_index));
            // Start to record a new block.
            index = next_index;
            start_row_index = row.2;
            end_row_index = start_row_index + 1;
        }
        indices.push((index, start_row_index, end_row_index - start_row_index));

        let columns = self
            .output_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(col_id, _)| {
                // Collect all rows for a certain column out of all preserved chunks.
                let candidate_cols = self
                    .blocks
                    .iter()
                    .flatten()
                    .map(|block| block.get_by_offset(col_id).clone())
                    .collect::<Vec<_>>();
                DataBlock::take_column_by_slices_limit(&candidate_cols, &indices, None)
            })
            .collect::<Vec<_>>();

        // Clear no need data.
        self.in_progress_rows.clear();
        // A cursor pointing to a new block is created only if the previous block is finished.
        // This means that all blocks except the last one for each input port are drained into the output block.
        // Therefore, the previous blocks can be cleared.
        for blocks in self.blocks.iter_mut() {
            if blocks.len() > 1 {
                blocks.drain(0..(blocks.len() - 1));
            }
        }

        Ok(DataBlock::new(columns, num_rows))
    }
}

#[async_trait::async_trait]
impl<R, Converter> Processor for MultiSortMergeProcessor<R, Converter>
where
    R: Rows + Send + 'static,
    Converter: RowConverter<R> + Send + 'static,
{
    fn name(&self) -> String {
        "MultiSortMerge".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            for input in self.inputs.iter() {
                input.finish();
            }
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(limit) = self.limit {
            if limit == 0 {
                for input in self.inputs.iter() {
                    input.finish();
                }
                self.output.finish();
                return Ok(Event::Finished);
            }
        }

        if matches!(self.state, ProcessorState::Generated(_)) {
            if let ProcessorState::Generated(data_block) =
                std::mem::replace(&mut self.state, ProcessorState::Consume)
            {
                self.limit = self.limit.map(|limit| {
                    if data_block.num_rows() > limit {
                        0
                    } else {
                        limit - data_block.num_rows()
                    }
                });
                self.output.push_data(Ok(data_block));
                return Ok(Event::NeedConsume);
            }
        }

        match &self.state {
            ProcessorState::Consume => {
                let data_blocks = self.get_data_blocks()?;
                if !data_blocks.is_empty() {
                    self.state = ProcessorState::Preserve(data_blocks);
                    return Ok(Event::Sync);
                }

                let active_inputs = self.nums_active_inputs();

                if active_inputs == 0 {
                    if !self.heap.is_empty() {
                        // The heap is not drained yet. Need to drain data into in_progress_rows.
                        self.state = ProcessorState::Preserve(vec![]);
                        return Ok(Event::Sync);
                    }
                    if !self.in_progress_rows.is_empty() {
                        // The in_progress_rows is not drained yet. Need to drain data into output.
                        self.state = ProcessorState::Output;
                        return Ok(Event::Sync);
                    }
                    self.output.finish();
                    Ok(Event::Finished)
                } else {
                    // `data_blocks` is empty
                    if !self.heap.is_empty() {
                        // The heap is not drained yet. Need to drain data into in_progress_rows.
                        self.state = ProcessorState::Preserve(vec![]);
                        Ok(Event::Sync)
                    } else {
                        Ok(Event::NeedData)
                    }
                }
            }
            ProcessorState::Output => Ok(Event::Sync),
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, ProcessorState::Consume) {
            ProcessorState::Preserve(blocks) => {
                for (input_index, block) in blocks.into_iter() {
                    if block.is_empty() {
                        continue;
                    }
                    let columns = self
                        .sort_field_indices
                        .iter()
                        .map(|i| block.get_by_offset(*i).clone())
                        .collect::<Vec<_>>();
                    let rows = self.row_converter.convert(&columns, block.num_rows())?;
                    let cursor = Cursor::try_create(input_index, rows);
                    self.heap.push(Reverse(cursor));
                    self.cursor_finished[input_index] = false;
                    self.blocks[input_index].push_back(block);
                }
                self.drain_heap();
                Ok(())
            }
            ProcessorState::Output => {
                let block = self.build_block()?;
                self.state = ProcessorState::Generated(block);
                Ok(())
            }
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }
}

enum ProcessorState {
    Consume,
    // Need to consume data from input.
    Preserve(Vec<(usize, DataBlock)>),
    // Need to preserve blocks in memory.
    Output,
    // Need to generate output block.
    Generated(DataBlock), // Need to push output block to output port.
}
