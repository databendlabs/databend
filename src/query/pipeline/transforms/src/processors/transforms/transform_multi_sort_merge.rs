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
use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::VecDeque;
use std::sync;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use common_arrow::arrow::compute::sort::row::Row;
use common_arrow::arrow::compute::sort::row::RowConverter;
use common_arrow::arrow::compute::sort::row::Rows;
use common_arrow::arrow::compute::sort::row::SortField;
use common_arrow::arrow::compute::sort::SortOptions;
use common_arrow::arrow::datatypes::DataType;
use common_datablocks::DataBlock;
use common_datablocks::SortColumnDescription;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_core::Pipe;
use common_pipeline_core::Pipeline;

pub fn try_add_multi_sort_merge(
    pipeline: &mut Pipeline,
    output_schema: DataSchemaRef,
    block_size: usize,
    limit: Option<usize>,
    sort_columns_descriptions: Vec<SortColumnDescription>,
) -> Result<()> {
    match pipeline.pipes.last() {
        None => Err(ErrorCode::Internal("Cannot resize empty pipe.")),
        Some(pipe) if pipe.output_size() == 0 => {
            Err(ErrorCode::Internal("Cannot resize empty pipe."))
        }
        Some(pipe) if pipe.output_size() == 1 => Ok(()),
        Some(pipe) => {
            let input_size = pipe.output_size();
            let mut inputs_port = Vec::with_capacity(input_size);
            for _ in 0..input_size {
                inputs_port.push(InputPort::create());
            }
            let output_port = OutputPort::create();
            let processor = MultiSortMergeProcessor::create(
                inputs_port.clone(),
                output_port.clone(),
                output_schema,
                block_size,
                limit,
                sort_columns_descriptions,
            )?;
            pipeline.pipes.push(Pipe::ResizePipe {
                inputs_port,
                outputs_port: vec![output_port],
                processor: ProcessorPtr::create(Box::new(processor)),
            });
            Ok(())
        }
    }
}

/// A cursor point to a certain row in a data block.
struct Cursor {
    pub input_index: usize,
    pub row_index: usize,

    num_rows: usize,

    rows: Rows,
}

impl Cursor {
    pub fn try_create(input_index: usize, rows: Rows) -> Cursor {
        Cursor {
            input_index,
            row_index: 0,
            num_rows: rows.len(),
            rows,
        }
    }

    #[inline]
    pub fn advance(&mut self) -> usize {
        let res = self.row_index;
        self.row_index += 1;
        res
    }

    #[inline]
    pub fn is_finished(&self) -> bool {
        self.num_rows == self.row_index
    }

    #[inline]
    pub fn current(&self) -> Row<'_> {
        self.rows.row_unchecked(self.row_index)
    }

    #[inline]
    pub fn last(&self) -> Row<'_> {
        self.rows.row_unchecked(self.num_rows - 1)
    }
}

impl Ord for Cursor {
    fn cmp(&self, other: &Self) -> Ordering {
        self.current()
            .cmp(&other.current())
            .then_with(|| self.input_index.cmp(&other.input_index))
    }
}

impl PartialEq for Cursor {
    fn eq(&self, other: &Self) -> bool {
        self.current() == other.current()
    }
}

impl Eq for Cursor {}

impl PartialOrd for Cursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// TransformMultiSortMerge is a processor with multiple input ports;
pub struct MultiSortMergeProcessor {
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
    in_progess_rows: Vec<(usize, usize, usize)>,
    /// Heap that yields [`Cursor`] in increasing order.
    heap: BinaryHeap<Reverse<Cursor>>,
    /// If the input port is finished.
    input_finished: Vec<bool>,
    /// Used to convert columns to rows.
    row_converter: RowConverter,

    state: ProcessorState,

    aborting: Arc<AtomicBool>,
}

impl MultiSortMergeProcessor {
    pub fn create(
        inputs: Vec<Arc<InputPort>>,
        output: Arc<OutputPort>,
        output_schema: DataSchemaRef,
        block_size: usize,
        limit: Option<usize>,
        sort_columns_descriptions: Vec<SortColumnDescription>,
    ) -> Result<Self> {
        let input_size = inputs.len();
        let mut sort_field_indices = Vec::with_capacity(sort_columns_descriptions.len());
        let sort_fields = sort_columns_descriptions
            .iter()
            .map(|d| {
                let data_type = match output_schema
                    .field_with_name(&d.column_name)?
                    .to_arrow()
                    .data_type()
                {
                    // The actual data type of `Data` and `Timestmap` will be `Int32` and `Int64`.
                    DataType::Date32 | DataType::Time32(_) => DataType::Int32,
                    DataType::Date64 | DataType::Time64(_) | DataType::Timestamp(_, _) => {
                        DataType::Int64
                    }
                    date_type => date_type.clone(),
                };
                sort_field_indices.push(output_schema.index_of(&d.column_name)?);
                Ok(SortField::new_with_options(data_type, SortOptions {
                    descending: !d.asc,
                    nulls_first: d.nulls_first,
                }))
            })
            .collect::<Result<Vec<_>>>()?;
        let row_converter = RowConverter::new(sort_fields);
        Ok(Self {
            inputs,
            output,
            output_schema,
            sort_field_indices,
            block_size,
            limit,
            blocks: vec![VecDeque::with_capacity(2); input_size],
            heap: BinaryHeap::with_capacity(input_size),
            in_progess_rows: vec![],
            cursor_finished: vec![true; input_size],
            input_finished: vec![false; input_size],
            row_converter,
            state: ProcessorState::Consume,
            aborting: Arc::new(AtomicBool::new(false)),
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
    fn drain_cursor(&mut self, mut cursor: Cursor) -> bool {
        let input_index = cursor.input_index;
        let block_index = self.blocks[input_index].len() - 1;
        while !cursor.is_finished() {
            self.in_progess_rows
                .push((input_index, block_index, cursor.advance()));
            if let Some(limit) = self.limit {
                if self.in_progess_rows.len() == limit {
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
        // Need to pop data to in_progess_rows.
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
                                self.in_progess_rows.push((
                                    input_index,
                                    block_index,
                                    cursor.advance(),
                                ));
                                if let Some(limit) = self.limit {
                                    if self.in_progess_rows.len() == limit {
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
                    if self.in_progess_rows.len() >= self.block_size {
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
                    debug_assert!(!self.in_progess_rows.is_empty());
                    self.state = ProcessorState::Output;
                    break;
                }
            }
        }
        if need_output {
            self.state = ProcessorState::Output;
        }
    }

    /// Drain `self.in_progess_rows` to build a output data block.
    fn build_block(&mut self) -> Result<DataBlock> {
        let num_rows = self.in_progess_rows.len();
        debug_assert!(num_rows > 0);

        let mut blocks_num_pre_sum = Vec::with_capacity(self.blocks.len());
        let mut len = 0;
        for block in self.blocks.iter() {
            blocks_num_pre_sum.push(len);
            len += block.len();
        }

        // Compute the indices of the output block.
        let first_row = &self.in_progess_rows[0];
        let mut index = blocks_num_pre_sum[first_row.0] + first_row.1;
        let mut start_row_index = first_row.2;
        let mut end_row_index = start_row_index + 1;
        let mut indices = Vec::new();
        for row in self.in_progess_rows.iter().skip(1) {
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
            .map(|(column_index, field)| {
                // Collect all rows for a ceterain column out of all preserved blocks.
                let candidate_cols = self
                    .blocks
                    .iter()
                    .flatten()
                    .map(|block| block.column(column_index).clone())
                    .collect::<Vec<_>>();
                DataBlock::take_column_by_slices_limit(
                    field.data_type(),
                    &candidate_cols,
                    &indices,
                    None,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        // Clear no need data.
        self.in_progess_rows.clear();
        // A cursor pointing to a new block is created onlyh if the previous block is finished.
        // This means that all blocks except the last one for each input port are drained into the output block.
        // Therefore, the previous blocks can be cleared.
        for blocks in self.blocks.iter_mut() {
            if blocks.len() > 1 {
                blocks.drain(0..(blocks.len() - 1));
            }
        }

        Ok(DataBlock::create(self.output_schema.clone(), columns))
    }
}

#[async_trait::async_trait]
impl Processor for MultiSortMergeProcessor {
    fn name(&self) -> String {
        "MultiSortMerge".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn interrupt(&self) {
        self.aborting.store(true, sync::atomic::Ordering::Release);
    }

    fn event(&mut self) -> Result<Event> {
        let aborting = self.aborting.load(sync::atomic::Ordering::Relaxed);
        if aborting {
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the server is shutting down or the query was killed.",
            ));
        }

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
                let all_finished = self.nums_active_inputs() == 0;
                if all_finished {
                    if !self.heap.is_empty() {
                        // The heap is not drained yet. Need to drain data into in_progress_rows.
                        self.state = ProcessorState::Preserve(vec![]);
                        return Ok(Event::Sync);
                    }
                    if !self.in_progess_rows.is_empty() {
                        // The in_progess_rows is not drained yet. Need to drain data into output.
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
                    let columns = self
                        .sort_field_indices
                        .iter()
                        .map(|i| {
                            let col = block.column(*i);
                            col.as_arrow_array(col.data_type())
                        })
                        .collect::<Vec<_>>();
                    let rows = self.row_converter.convert_columns(&columns)?;
                    if !block.is_empty() {
                        let cursor = Cursor::try_create(input_index, rows);
                        self.heap.push(Reverse(cursor));
                        self.cursor_finished[input_index] = false;
                        self.blocks[input_index].push_back(block);
                    }
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
    Consume,                           // Need to consume data from input.
    Preserve(Vec<(usize, DataBlock)>), // Need to preserve blocks in memory.
    Output,                            // Need to generate output block.
    Generated(DataBlock),              // Need to push output block to output port.
}
