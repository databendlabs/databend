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
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use common_arrow::arrow::array::ord::DynComparator;
use common_arrow::ArrayRef;
use common_datablocks::build_compare;
use common_datablocks::DataBlock;
use common_datablocks::SortColumnDescription;
use common_datavalues::ColumnRef;
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
        None => Err(ErrorCode::LogicalError("Cannot resize empty pipe.")),
        Some(pipe) if pipe.output_size() == 0 => {
            Err(ErrorCode::LogicalError("Cannot resize empty pipe."))
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
            );
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

    /// block_id is a global unique id. Not the sequence number of its input.
    pub block_id: usize,
    num_rows: usize,

    sort_columns: Vec<ColumnRef>,
    sort_columns_descriptions: Vec<SortColumnDescription>,

    /// A collection of comparators that compare rows in this cursors' block
    /// to the cursors in other blocks.
    ///
    /// Other blocks are uniquely identified by their input_index and block_index.
    comparators: RefCell<HashMap<usize, (usize, Vec<DynComparator>)>>,
}

impl Cursor {
    pub fn try_create(
        input_index: usize,
        block_index: usize,
        block: &DataBlock,
        sort_columns_descriptions: Vec<SortColumnDescription>,
    ) -> Result<Cursor> {
        let sort_columns = sort_columns_descriptions
            .iter()
            .map(|f| {
                let c = block.try_column_by_name(&f.column_name)?;
                Ok(c.clone())
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Cursor {
            input_index,
            block_id: block_index,
            row_index: 0,
            sort_columns_descriptions,
            sort_columns,
            num_rows: block.num_rows(),
            comparators: RefCell::new(HashMap::new()),
        })
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

    pub fn compare(&self, other: &Cursor) -> Result<Ordering> {
        if self.sort_columns.len() != other.sort_columns.len() {
            return Err(ErrorCode::LogicalError(format!(
                "Sort columns length not match: {} != {}",
                self.sort_columns.len(),
                other.sort_columns.len()
            )));
        }
        let zipped = self
            .sort_columns
            .iter()
            .map(|c| c.as_arrow_array(c.data_type()))
            .zip(
                other
                    .sort_columns
                    .iter()
                    .map(|c| c.as_arrow_array(c.data_type())),
            )
            .zip(self.sort_columns_descriptions.iter())
            .collect::<Vec<_>>();
        self.add_block_comparators(other, &zipped)?;
        let comparators = self.comparators.borrow();
        let comparators = &comparators
            .get(&other.input_index)
            .ok_or_else(|| {
                ErrorCode::LogicalError(format!(
                    "Cannot find comparators to compare {}.{} with {}.{}",
                    self.input_index, self.block_id, other.input_index, other.block_id
                ))
            })?
            .1;

        for (i, ((l, r), option)) in zipped.iter().enumerate() {
            match (l.is_valid(self.row_index), r.is_valid(other.row_index)) {
                (false, true) if option.nulls_first => return Ok(Ordering::Less),
                (false, true) => return Ok(Ordering::Greater),
                (true, false) if option.nulls_first => return Ok(Ordering::Greater),
                (true, false) => return Ok(Ordering::Less),
                (false, false) => {}
                (true, true) => match comparators[i](self.row_index, other.row_index) {
                    Ordering::Equal => {}
                    o if !option.asc => return Ok(o.reverse()),
                    o => return Ok(o),
                },
            }
        }

        // If all columns are equal, compare the input index.
        Ok(self.input_index.cmp(&other.input_index))
    }

    /// update comparators if there comes a new block.
    pub fn add_block_comparators(
        &self,
        other: &Cursor,
        zipped: &[((ArrayRef, ArrayRef), &SortColumnDescription)],
    ) -> Result<()> {
        let mut comparators = self.comparators.borrow_mut();
        if let Some(cmps) = comparators.get_mut(&other.input_index) {
            // comparator of this block is already exisits.
            if cmps.0 == other.block_id {
                return Ok(());
            }
            let mut compares = Vec::with_capacity(other.sort_columns.len());
            for ((l, r), _) in zipped.iter() {
                compares.push(build_compare(&**l, &**r)?)
            }
            cmps.1 = compares;
        } else {
            let mut compares = Vec::with_capacity(other.sort_columns.len());
            for ((l, r), _) in zipped.iter() {
                compares.push(build_compare(&**l, &**r)?)
            }
            comparators.insert(other.input_index, (other.block_id, compares));
        }
        Ok(())
    }
}

impl Ord for Cursor {
    fn cmp(&self, other: &Self) -> Ordering {
        self.compare(other).unwrap()
    }
}

impl PartialEq for Cursor {
    fn eq(&self, other: &Self) -> bool {
        other.compare(self).unwrap() == Ordering::Equal
    }
}

impl Eq for Cursor {}

impl PartialOrd for Cursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.compare(self).ok()
    }
}

/// TransformMultiSortMerge is a processor with multiple input ports;
pub struct MultiSortMergeProcessor {
    /// Data from inputs (every input is sorted)
    inputs: Vec<Arc<InputPort>>,
    output: Arc<OutputPort>,
    output_schema: DataSchemaRef,

    // Parameters
    block_size: usize,
    limit: Option<usize>,
    sort_columns_descriptions: Vec<SortColumnDescription>,

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
    heap: BinaryHeap<Cursor>,
    /// Assign every input block a unique id.
    cur_block_index: usize,
    /// If the input port is finished.
    input_finished: Vec<bool>,

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
    ) -> Self {
        let intput_size = inputs.len();
        Self {
            inputs,
            output,
            output_schema,
            block_size,
            limit,
            sort_columns_descriptions,
            blocks: vec![VecDeque::new(); intput_size],
            heap: BinaryHeap::with_capacity(intput_size),
            in_progess_rows: vec![],
            cursor_finished: vec![true; intput_size],
            cur_block_index: 0,
            input_finished: vec![false; intput_size],
            state: ProcessorState::Consume,
            aborting: Arc::new(AtomicBool::new(false)),
        }
    }

    fn get_data_blocks(&mut self) -> Result<Vec<(usize, DataBlock)>> {
        let mut data = Vec::new();
        for (i, input) in self.inputs.iter().enumerate() {
            if input.is_finished() {
                self.input_finished[i] = true;
                continue;
            }
            input.set_need_data();
            if self.cursor_finished[i] {
                if input.has_data() {
                    data.push((i, input.pull_data().unwrap()?));
                }
            }
        }
        Ok(data)
    }

    fn nums_active_inputs(&self) -> usize {
        self.input_finished
            .iter()
            .filter(|&&finished| !finished)
            .count()
    }

    fn drain_heap(&mut self) {
        let nums_active_inputs = self.nums_active_inputs();
        // Need to pop data to in_progess_rows.
        // Use `>=` because some of the input ports may be finished, but the data is still in the heap.
        while self.heap.len() >= nums_active_inputs {
            match self.heap.pop() {
                Some(mut cursor) => {
                    let input_index = cursor.input_index;
                    let row_index = cursor.advance();
                    if !cursor.is_finished() {
                        self.heap.push(cursor);
                    } else {
                        // We have read all rows of this block, need to read a new one.
                        self.cursor_finished[input_index] = true;
                    }
                    let block_index = self.blocks[input_index].len() - 1;
                    self.in_progess_rows
                        .push((input_index, block_index, row_index));
                    // Reach the block size, need to output.
                    if self.in_progess_rows.len() >= self.block_size {
                        self.state = ProcessorState::Output;
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
    }

    /// Drain `self.in_progess_rows` to build a output data block.
    fn build_block(&mut self) -> Result<DataBlock> {
        let num_rows = self.in_progess_rows.len();
        debug_assert!(num_rows > 0 && num_rows <= self.block_size);

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

        if matches!(self.state, ProcessorState::Generated(_)) {
            if let ProcessorState::Generated(data_block) =
                std::mem::replace(&mut self.state, ProcessorState::Consume)
            {
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
                    return Ok(Event::Finished);
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
            _ => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, ProcessorState::Consume) {
            ProcessorState::Preserve(blocks) => {
                for (input_index, block) in blocks.into_iter() {
                    if !block.is_empty() {
                        let cursor = Cursor::try_create(
                            input_index,
                            self.cur_block_index,
                            &block,
                            self.sort_columns_descriptions.clone(),
                        )?;
                        self.heap.push(cursor);
                        self.cursor_finished[input_index] = false;
                        self.blocks[input_index].push_back(block);
                        self.cur_block_index += 1;
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
            _ => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }
}

enum ProcessorState {
    Consume,                           // Need to consume data from input.
    Preserve(Vec<(usize, DataBlock)>), // Need to preserve blocks in memory.
    Output,                            // Need to generate output block.
    Generated(DataBlock),              // Need to push output block to output port.
}
