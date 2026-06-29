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

use std::cmp::Reverse;
use std::collections::VecDeque;

use databend_common_exception::Result;
use databend_common_expression::ChunkIndex;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataBlockVec;

use super::Rows;
use super::algorithm::*;

pub trait SortedStream {
    /// Returns the next block with the order column and if it is pending.
    ///
    /// If the block is [None] and it's not pending, it means the stream is finished.
    /// If the block is [None] but it's pending, it means the stream is not finished yet.
    fn next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)>;
}

#[async_trait::async_trait]
pub trait AsyncSortedStream {
    /// The async version of [`SortedStream::next`].
    async fn async_next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)>;
}

struct BufferState {
    buffer: DataBlockVec,
    stream_to_buffer: Vec<Option<usize>>,
    output_indices: ChunkIndex,
    detach: Vec<usize>,
    free: Vec<usize>,
}

impl BufferState {
    fn new(stream_count: usize) -> Self {
        Self {
            buffer: DataBlockVec::with_capacity(stream_count * 2),
            stream_to_buffer: vec![None; stream_count],
            output_indices: ChunkIndex::default(),
            detach: Vec::new(),
            free: Vec::new(),
        }
    }

    fn has_output(&self) -> bool {
        self.output_indices.num_rows() > 0
    }

    fn output_len(&self) -> usize {
        self.output_indices.num_rows()
    }

    fn attach_stream_block(&mut self, stream_index: usize, block: DataBlock) -> Result<()> {
        let index = if let Some(index) = self.free.pop() {
            self.buffer.replace(index, block);
            index
        } else {
            let index = self.buffer.block_rows().len();
            self.buffer.push(block)?;
            index
        };
        self.stream_to_buffer[stream_index] = Some(index);
        Ok(())
    }

    fn detach(&mut self, buffer_index: usize, stream_index: usize) {
        debug_assert_eq!(self.stream_to_buffer[stream_index], Some(buffer_index));
        self.stream_to_buffer[stream_index] = None;
        self.detach.push(buffer_index);
    }

    fn record_output_range(&mut self, buffer_index: usize, start: usize, count: usize) {
        self.output_indices
            .push_merge_range(buffer_index as _, start as _, count as _);
    }

    fn build_output(&mut self) -> DataBlock {
        let block = self.buffer.take(&self.output_indices);
        for i in self.detach.iter().copied() {
            self.buffer.replace_with_empty(i);
            self.free.push(i);
        }

        self.detach.clear();
        self.output_indices.clear();

        debug_assert_eq!(
            (0..self.buffer.block_rows().len())
                .filter(|buf| {
                    self.stream_to_buffer
                        .iter()
                        .flatten()
                        .all(|used| used != buf)
                        && !self.free.contains(buf)
                })
                .count(),
            0
        );

        block
    }
}

/// A merge sort operator to merge multiple sorted streams and output one sorted stream.
pub struct Merger<A, S>
where A: SortAlgorithm
{
    batch_rows: usize,
    limit: Option<usize>,
    row_by_row: bool,
    unsorted_streams: Vec<S>,

    pending_streams: VecDeque<usize>,
    sorted_cursors: A,
    buffers: BufferState,
}

impl<A, S> Merger<A, S>
where A: SortAlgorithm
{
    pub fn new(streams: Vec<S>, batch_rows: usize, limit: Option<usize>) -> Self {
        Self::create(streams, batch_rows, limit, false)
    }

    pub fn new_row_by_row(streams: Vec<S>, batch_rows: usize, limit: Option<usize>) -> Self {
        Self::create(streams, batch_rows, limit, true)
    }

    fn create(streams: Vec<S>, batch_rows: usize, limit: Option<usize>, row_by_row: bool) -> Self {
        // We only create a merger when there are at least two streams.
        debug_assert!(streams.len() > 1, "streams.len() = {}", streams.len());

        let sorted_cursors = A::with_capacity(streams.len());
        let pending_streams = (0..streams.len()).collect();
        let buffers = BufferState::new(streams.len());

        Self {
            unsorted_streams: streams,
            sorted_cursors,
            batch_rows,
            limit,
            row_by_row,
            pending_streams,
            buffers,
        }
    }

    #[inline(always)]
    pub fn is_finished(&self) -> bool {
        (self.sorted_cursors.is_empty() && !self.has_pending_stream() && !self.buffers.has_output())
            || self.limit == Some(0)
    }

    #[inline(always)]
    pub fn has_pending_stream(&self) -> bool {
        !self.pending_streams.is_empty()
    }

    /// To evaluate the current cursor, and update the top of the sorted_cursors if necessary.
    /// This method can only be called when iterating the sorted_cursors.
    ///
    /// Return `true` if the batch is full (need to output).
    #[inline(always)]
    fn evaluate_cursor(&mut self) -> bool {
        let cursor = if let Some(Reverse(cursor)) = self.sorted_cursors.peek() {
            cursor
        } else {
            return false;
        };

        let stream_index = cursor.input_index;
        let buffer_index = self.buffers.stream_to_buffer[stream_index]
            .expect("cursor must point to active stream buffer");
        let start = cursor.row_index;
        let count = self.evaluate_cursor_count(cursor);

        self.buffers.record_output_range(buffer_index, start, count);

        {
            // `peek_mut` adjusts the sorted cursor container when it is
            // dropped. Keep the guard scoped to this update so the next
            // iteration compares the advanced cursor with the other streams.
            let mut peek_mut = self.sorted_cursors.peek_mut();
            let cursor = &mut peek_mut.0;
            cursor.row_index += count;

            if cursor.is_finished() {
                // Pop the current `cursor`.
                A::pop_mut(peek_mut);
                self.buffers.detach(buffer_index, stream_index);
            }
        }

        if self.buffers.stream_to_buffer[stream_index].is_none() {
            self.pending_streams.push_back(stream_index);
        }

        let max_rows = self.limit.unwrap_or(self.batch_rows).min(self.batch_rows);
        debug_assert!(self.buffers.output_len() <= max_rows);
        self.buffers.output_len() != max_rows
    }

    #[inline(always)]
    fn evaluate_cursor_count(&self, cursor: &Cursor<A::Rows>) -> usize {
        debug_assert!(!cursor.is_finished());
        let start = cursor.row_index;
        let max_rows = self.limit.unwrap_or(self.batch_rows).min(self.batch_rows);
        let row_index_limit = cursor
            .num_rows()
            .min(start + max_rows - self.buffers.output_len());

        if self.row_by_row {
            return 1;
        }

        if self.sorted_cursors.len() == 1 || cursor.current() == cursor.last() {
            return row_index_limit - start;
        }

        if !A::SHOULD_PEEK_TOP2 {
            let mut p = cursor.cursor_mut();
            p.advance();
            let item = &cursor.current();
            while p.row_index < row_index_limit && p.current() == *item {
                p.advance();
            }
            return p.row_index - start;
        }

        let next_cursor = &self.sorted_cursors.peek_top2().0;
        if cursor.last() <= next_cursor.current() {
            // Short Path:
            // If the last row of current block is smaller than the next cursor,
            // we can drain the whole block.
            return row_index_limit - start;
        }

        let mut p = cursor.cursor_mut();
        p.advance();
        let item = &next_cursor.current();
        while p.row_index < row_index_limit && p.current() <= *item {
            // If the cursor is equals or smaller than the next cursor, continue advance.
            p.advance();
        }
        p.row_index - start
    }

    fn build_output(&mut self) -> Result<DataBlock> {
        let output_rows = self.buffers.output_len();
        self.limit = self.limit.map(|limit| limit - output_rows);
        let block = self.buffers.build_output();
        debug_assert!(block.num_rows() <= self.batch_rows);
        Ok(block)
    }

    pub fn streams(self) -> Vec<S> {
        self.unsorted_streams
    }
}

impl<A, S> Merger<A, S>
where
    A: SortAlgorithm,
    S: SortedStream + Send,
{
    #[inline]
    pub fn poll_pending_stream(&mut self) -> Result<()> {
        let mut continue_pendings = Vec::new();
        while let Some(i) = self.pending_streams.pop_front() {
            debug_assert!(self.buffers.stream_to_buffer[i].is_none());
            let (input, pending) = self.unsorted_streams[i].next()?;
            if pending {
                continue_pendings.push(i);
                continue;
            }
            if let Some((block, col)) = input {
                let rows = A::Rows::from_column(&col)?;
                self.buffers.attach_stream_block(i, block)?;
                let cursor = Cursor::new(i, rows);
                self.sorted_cursors.push(i, Reverse(cursor));
            }
        }
        self.sorted_cursors.rebuild();
        self.pending_streams.extend(continue_pendings);
        Ok(())
    }

    /// Returns the next sorted block and if it is pending.
    ///
    /// If the block is [None], it means the merger is finished or pending (has pending streams).
    pub fn next_block(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished() {
            return Ok(None);
        }

        if self.has_pending_stream() {
            self.poll_pending_stream()?;
            if self.has_pending_stream() {
                return Ok(None);
            }
        }

        // No pending streams now.
        if self.sorted_cursors.is_empty() {
            return if self.buffers.has_output() {
                Ok(Some(self.build_output()?))
            } else {
                Ok(None)
            };
        }

        while self.evaluate_cursor() {
            if self.has_pending_stream() {
                self.poll_pending_stream()?;
                if self.has_pending_stream() {
                    return Ok(None);
                }
            }
        }

        Ok(Some(self.build_output()?))
    }
}

impl<A, S> Merger<A, S>
where
    A: SortAlgorithm,
    S: AsyncSortedStream + Send,
{
    // This method can only be called when there is no data of the stream in the sorted_cursors.
    pub async fn async_poll_pending_stream(&mut self) -> Result<()> {
        let mut continue_pendings = Vec::new();
        while let Some(i) = self.pending_streams.pop_front() {
            debug_assert!(self.buffers.stream_to_buffer[i].is_none());
            let (input, pending) = self.unsorted_streams[i].async_next().await?;
            if pending {
                continue_pendings.push(i);
                continue;
            }
            if let Some((block, col)) = input {
                let rows = A::Rows::from_column(&col)?;
                self.buffers.attach_stream_block(i, block)?;
                let cursor = Cursor::new(i, rows);
                self.sorted_cursors.push(i, Reverse(cursor));
            }
        }
        self.sorted_cursors.rebuild();
        self.pending_streams.extend(continue_pendings);
        Ok(())
    }

    /// The async version of `next_block`.
    pub async fn async_next_block(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished() {
            return Ok(None);
        }

        if self.has_pending_stream() {
            self.async_poll_pending_stream().await?;
            if self.has_pending_stream() {
                return Ok(None);
            }
        }

        // No pending streams now.
        if self.sorted_cursors.is_empty() {
            return if self.buffers.has_output() {
                Ok(Some(self.build_output()?))
            } else {
                Ok(None)
            };
        }

        while self.evaluate_cursor() {
            if self.has_pending_stream() {
                self.async_poll_pending_stream().await?;
                if self.has_pending_stream() {
                    return Ok(None);
                }
            }
        }

        Ok(Some(self.build_output()?))
    }
}

pub type HeapMerger<R, S> = Merger<HeapSort<R>, S>;

pub type LoserTreeMerger<R, S> = Merger<LoserTreeSort<R>, S>;

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use databend_common_expression::Column;
    use databend_common_expression::DataBlock;
    use databend_common_expression::DataField;
    use databend_common_expression::DataSchemaRefExt;
    use databend_common_expression::FromData;
    use databend_common_expression::SortColumnDescription;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberDataType;

    use super::super::FixedRows;
    use super::super::convert_rows;
    use super::*;

    struct TestStream {
        blocks: VecDeque<(DataBlock, Column)>,
    }

    impl SortedStream for TestStream {
        fn next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
            Ok((self.blocks.pop_front(), false))
        }
    }

    fn make_compound_streams() -> Result<Vec<TestStream>> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("a", DataType::Number(NumberDataType::Int32)),
            DataField::new("b", DataType::Number(NumberDataType::Int32)),
        ]);
        let sort_desc: Arc<[SortColumnDescription]> = vec![
            SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
            },
            SortColumnDescription {
                offset: 1,
                asc: false,
                nulls_first: false,
            },
        ]
        .into();

        let make_stream = |b_values: Vec<i32>| -> Result<TestStream> {
            let rows = b_values.len();
            let block = DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1; rows]),
                Int32Type::from_data(b_values),
            ]);
            let order_col = convert_rows(schema.clone(), &sort_desc, block.clone(), true)?;
            Ok(TestStream {
                blocks: VecDeque::from([(block, order_col)]),
            })
        };

        Ok(vec![
            make_stream(vec![10, 1])?,
            make_stream(vec![9, 2])?,
            make_stream(vec![8, 3])?,
        ])
    }

    fn collect_b_values<A>(mut merger: Merger<A, TestStream>) -> Result<Vec<i32>>
    where A: SortAlgorithm {
        let mut outputs = Vec::new();
        while let Some(block) = merger.next_block()? {
            outputs.push(block.get_by_offset(1).to_column());
        }

        assert_eq!(outputs.len(), 1);
        let Some(databend_common_expression::Column::Number(
            databend_common_expression::types::NumberColumn::Int32(values),
        )) = outputs.pop()
        else {
            unreachable!("expected Int32 b column")
        };
        Ok(values.into_iter().collect())
    }

    #[test]
    fn test_row_by_row_heap_merge_keeps_compound_desc_order_for_overlaps() -> Result<()> {
        let streams = make_compound_streams()?;
        let merger = Merger::<HeapSort<FixedRows<2>>, _>::new_row_by_row(streams, 1024, Some(4));
        assert_eq!(collect_b_values(merger)?, vec![10, 9, 8, 3]);
        Ok(())
    }

    #[test]
    fn test_row_by_row_loser_tree_merge_keeps_compound_desc_order_for_overlaps() -> Result<()> {
        let streams = make_compound_streams()?;
        let merger =
            Merger::<LoserTreeSort<FixedRows<2>>, _>::new_row_by_row(streams, 1024, Some(4));
        assert_eq!(collect_b_values(merger)?, vec![10, 9, 8, 3]);
        Ok(())
    }
}
