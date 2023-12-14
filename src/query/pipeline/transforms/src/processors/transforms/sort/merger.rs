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
use std::collections::BinaryHeap;
use std::collections::VecDeque;
use std::sync::Arc;

use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::SortColumnDescription;

use super::utils::find_bigger_child_of_root;
use super::Cursor;
use super::Rows;
use crate::processors::sort::utils::get_ordered_rows;

#[async_trait::async_trait]
pub trait SortedStream {
    /// Returns the next block and if it is pending.
    ///
    /// If the block is [None] and it's not pending, it means the stream is finished.
    /// If the block is [None] but it's pending, it means the stream is not finished yet.
    fn next(&mut self) -> Result<(Option<DataBlock>, bool)> {
        Ok((None, false))
    }

    /// The async version of `next`.
    async fn async_next(&mut self) -> Result<(Option<DataBlock>, bool)> {
        self.next()
    }
}

/// A merge sort operator to merge multiple sorted streams and output one sorted stream.
pub struct HeapMerger<R, S>
where
    R: Rows,
    S: SortedStream,
{
    schema: DataSchemaRef,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    unsorted_streams: Vec<S>,
    heap: BinaryHeap<Reverse<Cursor<R>>>,
    buffer: Vec<DataBlock>,
    pending_stream: VecDeque<usize>,
    batch_size: usize,
    output_order_col: bool,

    temp_sorted_num_rows: usize,
    temp_output_indices: Vec<(usize, usize, usize)>,
    temp_sorted_blocks: Vec<DataBlock>,
}

impl<R, S> HeapMerger<R, S>
where
    R: Rows,
    S: SortedStream + Send,
{
    pub fn create(
        schema: DataSchemaRef,
        streams: Vec<S>,
        sort_desc: Arc<Vec<SortColumnDescription>>,
        batch_size: usize,
        output_order_col: bool,
    ) -> Self {
        // We only create a merger when there are at least two streams.
        debug_assert!(streams.len() > 1);
        debug_assert!(schema.num_fields() > 0);
        debug_assert_eq!(schema.fields.last().unwrap().name(), "_order_col");
        let heap = BinaryHeap::with_capacity(streams.len());
        let buffer = vec![DataBlock::empty_with_schema(schema.clone()); streams.len()];
        let pending_stream = (0..streams.len()).collect();

        Self {
            schema,
            unsorted_streams: streams,
            heap,
            buffer,
            batch_size,
            output_order_col,
            sort_desc,
            pending_stream,
            temp_sorted_num_rows: 0,
            temp_output_indices: vec![],
            temp_sorted_blocks: vec![],
        }
    }

    // This method can only be called when there is no data of the stream in the heap.
    async fn async_poll_pending_stream(&mut self) -> Result<()> {
        let mut continue_pendings = Vec::new();
        while let Some(i) = self.pending_stream.pop_front() {
            debug_assert!(self.buffer[i].is_empty());
            let (block, pending) = self.unsorted_streams[i].async_next().await?;
            if pending {
                continue_pendings.push(i);
                continue;
            }
            if let Some(mut block) = block {
                let rows = get_ordered_rows(&block, &self.sort_desc)?;
                if !self.output_order_col {
                    block.pop_columns(1);
                }
                let cursor = Cursor::new(i, rows);
                self.heap.push(Reverse(cursor));
                self.buffer[i] = block;
            }
        }
        self.pending_stream.extend(continue_pendings);
        Ok(())
    }

    #[inline]
    fn poll_pending_stream(&mut self) -> Result<()> {
        let mut continue_pendings = Vec::new();
        while let Some(i) = self.pending_stream.pop_front() {
            debug_assert!(self.buffer[i].is_empty());
            let (block, pending) = self.unsorted_streams[i].next()?;
            if pending {
                continue_pendings.push(i);
                continue;
            }
            if let Some(mut block) = block {
                let rows = get_ordered_rows(&block, &self.sort_desc)?;
                if !self.output_order_col {
                    block.pop_columns(1);
                }
                let cursor = Cursor::new(i, rows);
                self.heap.push(Reverse(cursor));
                self.buffer[i] = block;
            }
        }
        self.pending_stream.extend(continue_pendings);
        Ok(())
    }

    /// To evaluate the current cursor, and update the top of the heap if necessary.
    /// This method can only be called when iterating the heap.
    #[inline(always)]
    fn evaluate_cursor(&mut self, mut cursor: Cursor<R>) {
        if self.heap.len() == 1 {
            let start = cursor.row_index;
            let count =
                (cursor.num_rows() - start).min(self.batch_size - self.temp_sorted_num_rows);
            self.temp_sorted_num_rows += count;
            cursor.row_index += count;
            self.temp_output_indices
                .push((cursor.input_index, start, count));
        } else {
            let next_cursor = &find_bigger_child_of_root(&self.heap).0;
            if cursor.last().le(&next_cursor.current()) {
                // Short Path:
                // If the last row of current block is smaller than the next cursor,
                // we can drain the whole block.
                let start = cursor.row_index;
                let count =
                    (cursor.num_rows() - start).min(self.batch_size - self.temp_sorted_num_rows);
                self.temp_sorted_num_rows += count;
                cursor.row_index += count;
                self.temp_output_indices
                    .push((cursor.input_index, start, count));
            } else {
                // We copy current cursor for advancing,
                // and we will use this copied cursor to update the top of the heap at last
                // (let heap adjust itself without popping and pushing any element).
                let start = cursor.row_index;
                while !cursor.is_finished()
                    && cursor.le(next_cursor)
                    && self.temp_sorted_num_rows < self.batch_size
                {
                    // If the cursor is smaller than the next cursor, don't need to push the cursor back to the heap.
                    self.temp_sorted_num_rows += 1;
                    cursor.advance();
                }
                self.temp_output_indices.push((
                    cursor.input_index,
                    start,
                    cursor.row_index - start,
                ));
            }
        }

        if !cursor.is_finished() {
            // Update the top of the heap.
            // `self.heap.peek_mut` will return a `PeekMut` object which allows us to modify the top element of the heap.
            // The heap will adjust itself automatically when the `PeekMut` object is dropped (RAII).
            self.heap.peek_mut().unwrap().0 = cursor;
        } else {
            // Pop the current `cursor`.
            self.heap.pop();
            // We have read all rows of this block, need to release the old memory and read a new one.
            let temp_block = DataBlock::take_by_slices_limit_from_blocks(
                &self.buffer,
                &self.temp_output_indices,
                None,
            );
            self.buffer[cursor.input_index] = DataBlock::empty_with_schema(self.schema.clone());
            self.temp_sorted_blocks.push(temp_block);
            self.temp_output_indices.clear();
            self.pending_stream.push_back(cursor.input_index);
        }

        debug_assert!(self.temp_sorted_num_rows <= self.batch_size);
    }

    fn build_output(&mut self) -> Result<DataBlock> {
        if !self.temp_output_indices.is_empty() {
            let block = DataBlock::take_by_slices_limit_from_blocks(
                &self.buffer,
                &self.temp_output_indices,
                None,
            );
            self.temp_sorted_blocks.push(block);
        }
        let block = DataBlock::concat(&self.temp_sorted_blocks)?;

        debug_assert_eq!(block.num_rows(), self.temp_sorted_num_rows);
        debug_assert!(block.num_rows() <= self.batch_size);

        self.temp_sorted_blocks.clear();
        self.temp_output_indices.clear();
        self.temp_sorted_num_rows = 0;

        Ok(block)
    }

    /// Returns the next sorted block and if it is pending.
    ///
    /// If the block is [None] and it's not pending, it means the stream is finished.
    /// If the block is [None] but it's pending, it means the stream is not finished yet.
    pub fn next_block(&mut self) -> Result<(Option<DataBlock>, bool)> {
        if !self.pending_stream.is_empty() {
            self.poll_pending_stream()?;
            if !self.pending_stream.is_empty() {
                return Ok((None, true));
            }
        }
        if self.heap.is_empty() {
            if self.temp_sorted_num_rows > 0 {
                let block = self.build_output()?;
                return Ok((Some(block), false));
            }
            return Ok((None, false));
        }
        while let Some(Reverse(cursor)) = self.heap.peek() {
            self.evaluate_cursor(cursor.clone());
            if self.temp_sorted_num_rows == self.batch_size {
                break;
            }
            if !self.pending_stream.is_empty() {
                self.poll_pending_stream()?;
                if !self.pending_stream.is_empty() {
                    return Ok((None, true));
                }
            }
        }

        let block = self.build_output()?;
        Ok((Some(block), false))
    }

    /// The async version of `next_block`.
    pub async fn async_next_block(&mut self) -> Result<(Option<DataBlock>, bool)> {
        if !self.pending_stream.is_empty() {
            self.async_poll_pending_stream().await?;
            if !self.pending_stream.is_empty() {
                return Ok((None, true));
            }
        }
        if self.heap.is_empty() {
            if self.temp_sorted_num_rows > 0 {
                let block = self.build_output()?;
                return Ok((Some(block), false));
            }
            return Ok((None, false));
        }
        while let Some(Reverse(cursor)) = self.heap.peek() {
            self.evaluate_cursor(cursor.clone());
            if self.temp_sorted_num_rows == self.batch_size {
                break;
            }
            if !self.pending_stream.is_empty() {
                self.async_poll_pending_stream().await?;
                if !self.pending_stream.is_empty() {
                    return Ok((None, true));
                }
            }
        }

        let block = self.build_output()?;
        Ok((Some(block), false))
    }
}
