// Copyright 2023 Datafuse Labs.
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

// Some variables and functions are named and designed with reference to ClickHouse.
// - https://github.com/ClickHouse/ClickHouse/blob/master/src/Processors/Transforms/WindowTransform.h
// - https://github.com/ClickHouse/ClickHouse/blob/master/src/Processors/Transforms/WindowTransform.cpp

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use common_exception::Result;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::DataBlock;
use common_expression::Value;
use common_functions::aggregates::get_layout_offsets;
use common_functions::aggregates::AggregateFunction;
use common_functions::aggregates::StateAddr;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;

use super::frame::WindowFrame;
use super::WindowFrameBound;
use crate::pipelines::processors::transforms::group_by::Area;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
struct RowPtr {
    pub block: usize,
    pub row: usize,
}

impl RowPtr {
    #[inline(always)]
    pub fn new(block: usize, row: usize) -> Self {
        Self { block, row }
    }
}

#[derive(Clone)]
pub struct WindowBlock {
    pub block: DataBlock,
    pub builder: ColumnBuilder,
}

/// The input [`DataBlock`] of [`TransformWindow`] should be sorted by partition and order by columns.
///
/// Window function will not change the rows count of the original data.
pub struct TransformWindow {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    state: ProcessorState,
    input_is_finished: bool,

    function: Arc<dyn AggregateFunction>,
    arguments: Vec<usize>,

    place: StateAddr,

    partition_indices: Vec<usize>,

    /// A queue of data blocks that we need to process.
    /// If partition is ended, we may free the data block from front of the queue.
    blocks: VecDeque<WindowBlock>,
    /// A queue of data blocks that can be output.
    outputs: VecDeque<DataBlock>,

    /// monotonically increasing index of the current block in the queue.
    first_block: usize,

    // Partition: [`partition_start`, `partition_end`). `partition_end` is excluded.
    partition_start: RowPtr,
    partition_end: RowPtr,
    partition_ended: bool,

    // Frame: [`frame_start`, `frame_end`). `frame_end` is excluded.
    frame_kind: WindowFrame,
    frame_start: RowPtr,
    frame_end: RowPtr,
    frame_started: bool,
    frame_ended: bool,

    current_row: RowPtr,

    // Used for rank
    current_row_in_partition: usize,
}

impl TransformWindow {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        function: Arc<dyn AggregateFunction>,
        arguments: Vec<usize>,
        partition_indices: Vec<usize>,
        frame_kind: WindowFrame,
    ) -> Result<Self> {
        let mut arena = Area::create();
        let mut state_offset = Vec::with_capacity(1);
        let layout = get_layout_offsets(&[function.clone()], &mut state_offset)?;
        let state_place: StateAddr = arena.alloc_layout(layout).into();
        let state_place = state_place.next(state_offset[0]);

        Ok(Self {
            input,
            output,
            state: ProcessorState::Consume,
            function,
            arguments,
            partition_indices,
            place: state_place,
            blocks: VecDeque::new(),
            outputs: VecDeque::new(),
            first_block: 0,
            partition_start: RowPtr::default(),
            partition_end: RowPtr::default(),
            partition_ended: false,
            frame_kind,
            frame_start: RowPtr::default(),
            frame_end: RowPtr::default(),
            frame_started: false,
            frame_ended: false,
            current_row: RowPtr::default(),
            current_row_in_partition: 1,
            input_is_finished: false,
        })
    }

    #[inline(always)]
    fn blocks_end(&self) -> RowPtr {
        RowPtr::new(self.first_block + self.blocks.len(), 0)
    }

    #[inline(always)]
    fn block_rows(&self, index: RowPtr) -> usize {
        self.block_at(index).num_rows()
    }

    #[inline(always)]
    fn block_at(&self, index: RowPtr) -> &DataBlock {
        &self.blocks[index.block - self.first_block].block
    }

    #[inline(always)]
    fn builder_at(&mut self, index: RowPtr) -> &mut ColumnBuilder {
        &mut self.blocks[index.block - self.first_block].builder
    }

    #[inline(always)]
    fn column_at(&self, index: RowPtr, column_index: usize) -> &Column {
        self.block_at(index)
            .get_by_offset(column_index)
            .value
            .as_column()
            .unwrap()
    }

    fn current_row_sub_within_partition(&self, mut n: usize) -> RowPtr {
        let mut row = self.current_row;
        let start = &self.partition_start;
        // Use '>' to avoid overflow.
        while row.block > start.block {
            if row.row >= n {
                row.row -= n;
                return row;
            }
            n -= row.row;
            row.block -= 1;
            row.row = self.block_rows(row);
        }
        // row = RowPtr::new(partition_start.block, block.num_rows())
        row.row = row.row - (row.row - start.row).min(n);
        row
    }

    fn current_row_add_within_partition(&self, mut n: usize) -> RowPtr {
        let mut row = self.current_row;
        let end = &self.partition_end;
        while row.block < end.block {
            let rows = self.block_rows(row);
            if row.row + n < rows {
                row.row += n;
                return row;
            }
            n -= rows - row.row;
            row.block += 1;
            row.row = 0;
        }
        // row = RowPtr::new(partition_end.block, 0)
        row.row = end.row.min(row.row + n);
        row
    }

    #[inline(always)]
    fn aggregate_arguments(block: &DataBlock, arguments: &[usize]) -> Vec<Column> {
        arguments
            .iter()
            .map(|index| {
                block
                    .get_by_offset(*index)
                    .value
                    .as_column()
                    .cloned()
                    .unwrap()
            })
            .collect()
    }

    /// Advance the partition end to the next partition or the end of the data.
    fn advance_partition(&mut self) {
        if self.partition_ended {
            return;
        }

        let end = self.blocks_end();

        if self.input_is_finished {
            self.partition_ended = true;
            assert_eq!(self.partition_end, end);
            return;
        }

        if self.partition_end == end {
            return;
        }

        let partition_by_columns = self.partition_indices.len();
        if partition_by_columns == 0 {
            self.partition_end = end;
            return;
        }

        let block_rows = self.block_rows(self.partition_end);

        while self.partition_end.row < block_rows {
            let mut i = 0;
            while i < partition_by_columns {
                let start_column = self.column_at(self.partition_start, self.partition_indices[i]);
                let compare_column = self.column_at(self.partition_end, self.partition_indices[i]);

                if start_column.index(self.partition_start.row)
                    != compare_column.index(self.partition_end.row)
                {
                    break;
                }
                i += 1;
            }

            if i < partition_by_columns {
                self.partition_ended = true;
                return;
            }
            self.partition_end.row += 1;
        }

        assert_eq!(self.partition_end.row, block_rows);
        self.partition_end.block += 1;
        self.partition_end.row = 0;

        assert!(!self.partition_ended && self.partition_end == end);
    }

    fn advance_frame_start(&mut self) {
        if self.frame_started {
            return;
        }
        match &self.frame_kind.start_bound {
            WindowFrameBound::CurrentRow => {
                self.frame_started = true;
                self.frame_start = self.current_row;
            }
            WindowFrameBound::Preceding(Some(n)) => {
                self.frame_started = true;
                self.frame_start = self.current_row_sub_within_partition(*n);
            }
            WindowFrameBound::Preceding(_) => {
                self.frame_started = true;
                self.frame_start = self.partition_start;
            }
            WindowFrameBound::Following(Some(n)) => {
                self.frame_start = self.current_row_add_within_partition(*n);
                self.frame_started = self.partition_ended || self.frame_start < self.partition_end
            }
            WindowFrameBound::Following(_) => {
                unreachable!()
            }
        }
    }

    fn advance_frame_end(&mut self) {
        match &self.frame_kind.end_bound {
            WindowFrameBound::CurrentRow => {
                self.frame_ended = true;
                self.frame_end = self.current_row;
            }
            WindowFrameBound::Preceding(Some(n)) => {
                self.frame_ended = true;
                self.frame_end = self.current_row_sub_within_partition(*n);
            }
            WindowFrameBound::Preceding(_) => {
                unreachable!()
            }
            WindowFrameBound::Following(Some(n)) => {
                self.frame_end = self.current_row_add_within_partition(*n);
                self.frame_ended = self.partition_ended || self.frame_end < self.partition_end;
                // Frame end is excluded.
                self.frame_end = self.advance_row(self.frame_end).min(self.partition_end);
            }
            WindowFrameBound::Following(_) => {
                self.frame_ended = self.partition_ended;
                self.frame_end = self.partition_end;
            }
        }
    }

    // Advance the current row to the next row
    // if the current row is the last row of the current block, advance the current block and row = 0
    fn advance_row(&mut self, mut row: RowPtr) -> RowPtr {
        if row == self.blocks_end() {
            return row;
        }
        if row.row < self.block_rows(row) - 1 {
            row.row += 1;
        } else {
            row.block += 1;
            row.row = 0;
        }
        row
    }

    /// When adding a [`DataBlock`], we compute the aggregations to the end.
    ///
    /// For each row in the input block,
    /// compute the aggregation result of its window frame and add it intp result column buffer.
    ///
    /// If not reach the end bound of the window frame, hold the temporary aggregation value in `state_place`.
    ///
    /// Once collect all the results of one input [`DataBlock`], attach the corresponding result column to the input as output.
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        if let Some(data) = data {
            let num_rows = data.num_rows();
            self.blocks.push_back(WindowBlock {
                block: data.convert_to_full(),
                builder: ColumnBuilder::with_capacity(&self.function.return_type()?, num_rows),
            });
        }

        // Each loop will do:
        // 1. Try to advance the partition.
        // 2. Try to advance the frame (if the frame is not started or ended, break the loop and end the process).
        // 3. Compute the aggregation for current frame.
        // 4. If the partition is not ended, break the loop;
        //    else start next partition.
        loop {
            // 1.
            self.advance_partition();

            while self.current_row < self.partition_end {
                // 2.
                self.advance_frame_start();
                if !self.frame_started {
                    break;
                }
                self.advance_frame_end();
                if !self.frame_ended {
                    break;
                }

                // 3.
                self.apply_aggregate()?;

                self.current_row = self.advance_row(self.current_row);

                self.current_row_in_partition += 1;
                self.frame_started = false;
                self.frame_ended = false;
            }

            if self.input_is_finished {
                return Ok(());
            }

            // 4.
            if !self.partition_ended {
                break;
            }

            // start to new partition
            self.partition_start = self.partition_end;
            self.partition_end = self.advance_row(self.partition_end);
            self.partition_ended = false;

            // reset frames
            self.frame_start = self.partition_start;
            self.frame_end = self.partition_start;

            self.current_row_in_partition = 1;
        }

        Ok(())
    }

    fn check_outputs(&mut self) {
        while let Some(WindowBlock { block, builder }) = self.blocks.front() {
            if block.num_rows() == builder.len() {
                let WindowBlock { mut block, builder } = self.blocks.pop_front().unwrap();
                let new_column = builder.build();
                block.add_column(BlockEntry {
                    data_type: new_column.data_type(),
                    value: Value::Column(new_column),
                });
                self.outputs.push_back(block);
                self.first_block += 1;
            } else {
                break;
            }
        }
    }

    fn apply_aggregate(&mut self) -> Result<()> {
        let block_end = self.blocks_end();
        let row_start = self.frame_start;
        let row_end = self.frame_end;

        // TODO: for some case, current frame can continue to use the previous frame's state
        // Reset state
        self.function.init_state(self.place);

        for block in row_start.block..=row_end.block.min(block_end.block - 1) {
            let block_row_start = if block == row_start.block {
                row_start.row
            } else {
                0
            };
            let block_row_end = if block == row_end.block {
                row_end.row
            } else {
                self.block_rows(RowPtr { block, row: 0 })
            };

            let data = self.block_at(RowPtr { block, row: 0 });
            let columns = Self::aggregate_arguments(data, &self.arguments);

            for row in block_row_start..block_row_end {
                self.function.accumulate_row(self.place, &columns, row)?;
            }
        }

        let function = self.function.clone();
        let place = self.place;
        let builder = self.builder_at(self.current_row);
        function.merge_result(place, builder)?;

        Ok(())
    }
}

enum ProcessorState {
    Consume,
    AddBlock(Option<DataBlock>),
    Output,
}

#[async_trait::async_trait]
impl Processor for TransformWindow {
    fn name(&self) -> String {
        "Transform Window".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        let input_is_finished = self.input.is_finished();
        match self.state {
            ProcessorState::Consume => {
                let has_data = self.input.has_data();
                let data = self.input.pull_data().transpose()?;
                match (input_is_finished, has_data) {
                    (_, true) => {
                        self.state = ProcessorState::AddBlock(data);
                        Ok(Event::Sync)
                    }
                    (false, false) => Ok(Event::NeedData),
                    (true, _) => {
                        // input_is_finished should be set after adding block.
                        self.input_is_finished = true;
                        if !self.blocks.is_empty() {
                            self.state = ProcessorState::AddBlock(None);
                            Ok(Event::Sync)
                        } else {
                            self.output.finish();
                            Ok(Event::Finished)
                        }
                    }
                }
            }
            ProcessorState::Output => {
                let output = self.outputs.pop_front().unwrap();
                self.output.push_data(Ok(output));
                if self.outputs.is_empty() {
                    self.state = ProcessorState::Consume;
                }
                Ok(Event::NeedConsume)
            }
            _ => unreachable!(),
        }
    }

    fn process(&mut self) -> Result<()> {
        if let ProcessorState::AddBlock(data) =
            std::mem::replace(&mut self.state, ProcessorState::Consume)
        {
            self.add_block(data)?;
            self.check_outputs();
            self.state = if self.outputs.is_empty() {
                ProcessorState::Consume
            } else {
                ProcessorState::Output
            };
        } else {
            unreachable!()
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common_exception::Result;
    use common_expression::block_debug::assert_blocks_eq;
    use common_expression::types::DataType;
    use common_expression::types::Int32Type;
    use common_expression::types::NumberDataType;
    use common_expression::Column;
    use common_expression::ColumnBuilder;
    use common_expression::DataBlock;
    use common_expression::FromData;
    use common_functions::aggregates::AggregateFunctionFactory;
    use common_pipeline_core::processors::port::InputPort;
    use common_pipeline_core::processors::port::OutputPort;

    use super::TransformWindow;
    use super::WindowBlock;
    use crate::pipelines::processors::transforms::window::transform_window::RowPtr;
    use crate::pipelines::processors::transforms::WindowFrame;
    use crate::pipelines::processors::transforms::WindowFrameBound;

    fn get_transform_window(
        window_frame: WindowFrame,
        arg_type: DataType,
    ) -> Result<TransformWindow> {
        let function = AggregateFunctionFactory::instance().get("sum", vec![], vec![arg_type])?;
        TransformWindow::try_create(
            InputPort::create(),
            OutputPort::create(),
            function,
            vec![0],
            vec![0],
            window_frame,
        )
    }

    fn get_transform_window_with_data(
        window_frame: WindowFrame,
        column: Column,
    ) -> Result<TransformWindow> {
        let data_type = column.data_type();
        let num_rows = column.len();
        let mut transform = get_transform_window(window_frame, data_type.clone())?;
        transform.blocks.push_back(WindowBlock {
            block: DataBlock::new_from_columns(vec![column]),
            builder: ColumnBuilder::with_capacity(&data_type, num_rows),
        });
        Ok(transform)
    }

    #[test]
    fn test_partition_advance() -> Result<()> {
        {
            let mut transform = get_transform_window_with_data(
                WindowFrame {
                    start_bound: WindowFrameBound::CurrentRow,
                    end_bound: WindowFrameBound::CurrentRow,
                },
                Int32Type::from_data(vec![1, 1, 1]),
            )?;

            transform.advance_partition();

            assert!(!transform.partition_ended);
            assert_eq!(transform.partition_end, RowPtr::new(1, 0));
        }

        {
            let mut transform = get_transform_window_with_data(
                WindowFrame {
                    start_bound: WindowFrameBound::CurrentRow,
                    end_bound: WindowFrameBound::CurrentRow,
                },
                Int32Type::from_data(vec![1, 1, 2]),
            )?;

            transform.advance_partition();

            assert!(transform.partition_ended);
            assert_eq!(transform.partition_end, RowPtr::new(0, 2));
        }
        Ok(())
    }

    #[test]
    fn test_frame_advance() -> Result<()> {
        {
            let mut transform = get_transform_window_with_data(
                WindowFrame {
                    start_bound: WindowFrameBound::Following(Some(4)),
                    end_bound: WindowFrameBound::Following(Some(5)),
                },
                Int32Type::from_data(vec![1, 1, 1]),
            )?;

            transform.advance_partition();

            assert!(!transform.partition_ended);
            assert_eq!(transform.partition_end, RowPtr::new(1, 0));

            transform.advance_frame_start();
            assert!(!transform.frame_started)
        }

        {
            let mut transform = get_transform_window_with_data(
                WindowFrame {
                    start_bound: WindowFrameBound::Preceding(Some(2)),
                    end_bound: WindowFrameBound::Following(Some(5)),
                },
                Int32Type::from_data(vec![1, 1, 1]),
            )?;

            transform.advance_partition();
            transform.current_row = RowPtr::new(0, 1);

            assert!(!transform.partition_ended);
            assert_eq!(transform.partition_end, RowPtr::new(1, 0));

            transform.advance_frame_start();
            assert!(transform.frame_started);
            assert_eq!(transform.frame_start, RowPtr::new(0, 0));

            transform.advance_frame_end();
            assert!(!transform.frame_ended);
        }

        {
            let mut transform = get_transform_window_with_data(
                WindowFrame {
                    start_bound: WindowFrameBound::Preceding(Some(2)),
                    end_bound: WindowFrameBound::Following(Some(1)),
                },
                Int32Type::from_data(vec![1, 1, 1]),
            )?;

            transform.advance_partition();
            transform.current_row = RowPtr::new(0, 1);

            assert!(!transform.partition_ended);
            assert_eq!(transform.partition_end, RowPtr::new(1, 0));

            transform.advance_frame_start();
            assert!(transform.frame_started);
            assert_eq!(transform.frame_start, RowPtr::new(0, 0));

            transform.advance_frame_end();
            assert!(transform.frame_ended);
            assert_eq!(transform.frame_end, RowPtr::new(1, 0));
        }

        {
            let mut transform = get_transform_window_with_data(
                WindowFrame {
                    start_bound: WindowFrameBound::Preceding(None),
                    end_bound: WindowFrameBound::Following(None),
                },
                Int32Type::from_data(vec![1, 1, 1, 2]),
            )?;

            transform.advance_partition();
            transform.current_row = RowPtr::new(0, 1);

            assert!(transform.partition_ended);
            assert_eq!(transform.partition_end, RowPtr::new(0, 3));

            transform.advance_frame_start();
            assert!(transform.frame_started);
            assert_eq!(transform.frame_start, RowPtr::new(0, 0));

            transform.advance_frame_end();
            assert!(transform.frame_ended);
            assert_eq!(transform.frame_end, RowPtr::new(0, 3));
        }

        Ok(())
    }

    #[test]
    fn test_add_block() -> Result<()> {
        {
            let mut transform = get_transform_window(
                WindowFrame {
                    start_bound: WindowFrameBound::Preceding(None),
                    end_bound: WindowFrameBound::Following(None),
                },
                DataType::Number(NumberDataType::Int32),
            )?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 1, 1]),
            ])))?;

            transform.input_is_finished = true;

            transform.add_block(None)?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 3        |",
                    "| 1        | 3        |",
                    "| 1        | 3        |",
                    "+----------+----------+",
                ],
                &[output],
            );
        }

        {
            let mut transform = get_transform_window(
                WindowFrame {
                    start_bound: WindowFrameBound::Preceding(None),
                    end_bound: WindowFrameBound::Following(None),
                },
                DataType::Number(NumberDataType::Int32),
            )?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 1, 1, 2, 2, 3, 3, 3]),
            ])))?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![3, 4, 4]),
            ])))?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 3        |",
                    "| 1        | 3        |",
                    "| 1        | 3        |",
                    "| 2        | 4        |",
                    "| 2        | 4        |",
                    "| 3        | 12       |",
                    "| 3        | 12       |",
                    "| 3        | 12       |",
                    "+----------+----------+",
                ],
                &[output],
            );

            transform.input_is_finished = true;

            transform.add_block(None)?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 3        | 12       |",
                    "| 4        | 8        |",
                    "| 4        | 8        |",
                    "+----------+----------+",
                ],
                &[output],
            );
        }

        {
            let mut transform = get_transform_window(
                WindowFrame {
                    start_bound: WindowFrameBound::Preceding(None),
                    end_bound: WindowFrameBound::Following(None),
                },
                DataType::Number(NumberDataType::Int32),
            )?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 1, 1, 2, 2, 3, 3, 3]),
            ])))?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![5, 4, 4]),
            ])))?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 3        |",
                    "| 1        | 3        |",
                    "| 1        | 3        |",
                    "| 2        | 4        |",
                    "| 2        | 4        |",
                    "| 3        | 9        |",
                    "| 3        | 9        |",
                    "| 3        | 9        |",
                    "+----------+----------+",
                ],
                &[output],
            );

            transform.input_is_finished = true;

            transform.add_block(None)?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 5        | 5        |",
                    "| 4        | 8        |",
                    "| 4        | 8        |",
                    "+----------+----------+",
                ],
                &[output],
            );
        }

        {
            let mut transform = get_transform_window(
                WindowFrame {
                    start_bound: WindowFrameBound::Preceding(Some(1)),
                    end_bound: WindowFrameBound::Following(Some(1)),
                },
                DataType::Number(NumberDataType::Int32),
            )?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 1, 1, 2, 2, 3, 3, 3]),
            ])))?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![3, 4, 4]),
            ])))?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 2        |",
                    "| 1        | 3        |",
                    "| 1        | 2        |",
                    "| 2        | 4        |",
                    "| 2        | 4        |",
                    "| 3        | 6        |",
                    "| 3        | 9        |",
                    "| 3        | 9        |",
                    "+----------+----------+",
                ],
                &[output],
            );

            transform.input_is_finished = true;

            transform.add_block(None)?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 3        | 6        |",
                    "| 4        | 8        |",
                    "| 4        | 8        |",
                    "+----------+----------+",
                ],
                &[output],
            );
        }

        Ok(())
    }
}
