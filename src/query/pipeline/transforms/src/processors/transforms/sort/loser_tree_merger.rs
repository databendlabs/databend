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
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;

use super::loser_tree::LoserTree;
use super::Cursor;
use super::Rows;

pub trait SortedStream {
    /// Returns the next block with the order column and if it is pending.
    ///
    /// If the block is [None] and it's not pending, it means the stream is finished.
    /// If the block is [None] but it's pending, it means the stream is not finished yet.
    fn next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        Ok((None, false))
    }
}

pub struct LoserTreeMerger<R, S>
where
    R: Rows,
    S: SortedStream,
{
    schema: DataSchemaRef,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    unsorted_streams: Vec<S>,
    tree: LoserTree<Option<Reverse<Cursor<R>>>>,
    cursor_count: usize,
    buffer: Vec<DataBlock>,
    pending_streams: VecDeque<usize>,
    batch_size: usize,
    limit: Option<usize>,

    temp_sorted_num_rows: usize,
    temp_output_indices: Vec<(usize, usize, usize)>,
    temp_sorted_blocks: Vec<DataBlock>,
}

impl<R, S> LoserTreeMerger<R, S>
where
    R: Rows,
    S: SortedStream,
{
    pub fn create(
        schema: DataSchemaRef,
        streams: Vec<S>,
        sort_desc: Arc<Vec<SortColumnDescription>>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Self {
        // We only create a merger when there are at least two streams.
        debug_assert!(streams.len() > 1, "streams.len() = {}", streams.len());

        let buffer = vec![DataBlock::empty_with_schema(schema.clone()); streams.len()];
        let tree = LoserTree::from(vec![None; streams.len()]);
        let pending_stream = (0..streams.len()).collect();

        Self {
            schema,
            unsorted_streams: streams,
            tree,
            cursor_count: 0,
            buffer,
            batch_size,
            limit,
            sort_desc,
            pending_streams: pending_stream,
            temp_sorted_num_rows: 0,
            temp_output_indices: vec![],
            temp_sorted_blocks: vec![],
        }
    }

    #[inline(always)]
    pub fn is_finished(&self) -> bool {
        (self.cursor_count == 0 && !self.has_pending_stream() && self.temp_sorted_num_rows == 0)
            || self.limit == Some(0)
    }

    #[inline(always)]
    pub fn has_pending_stream(&self) -> bool {
        !self.pending_streams.is_empty()
    }

    #[inline]
    pub fn poll_pending_stream(&mut self) -> Result<()> {
        let mut continue_pendings = Vec::new();
        while let Some(i) = self.pending_streams.pop_front() {
            debug_assert!(self.buffer[i].is_empty());
            let (input, pending) = self.unsorted_streams[i].next()?;
            if pending {
                continue_pendings.push(i);
                continue;
            }
            if let Some((block, col)) = input {
                let rows = R::from_column(&col, &self.sort_desc)?;
                let cursor = Cursor::new(i, rows);
                self.tree.update(i, Some(Reverse(cursor)));
                self.cursor_count += 1;
                self.buffer[i] = block;
            }
        }
        self.pending_streams.extend(continue_pendings);
        Ok(())
    }

    /// To evaluate the current cursor, and update the top of the heap if necessary.
    /// This method can only be called when iterating the heap.
    ///
    /// Return `true` if the batch is full (need to output).
    #[inline(always)]
    fn evaluate_cursor(&mut self, mut cursor: Cursor<R>) -> bool {
        let max_rows = self.limit.unwrap_or(self.batch_size).min(self.batch_size);
        if self.cursor_count == 1 {
            let start = cursor.row_index;
            let count = (cursor.num_rows() - start).min(max_rows - self.temp_sorted_num_rows);
            self.temp_sorted_num_rows += count;
            cursor.row_index += count;
            self.temp_output_indices
                .push((cursor.input_index, start, count));
        } else {
            let next_cursor = &self.tree.peek_top2().as_ref().unwrap().0;
            if cursor.last().le(&next_cursor.current()) {
                // Short Path:
                // If the last row of current block is smaller than the next cursor,
                // we can drain the whole block.
                let start = cursor.row_index;
                let count = (cursor.num_rows() - start).min(max_rows - self.temp_sorted_num_rows);
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
                    && self.temp_sorted_num_rows < max_rows
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
            *self.tree.peek_mut() = Some(Reverse(cursor));
        } else {
            *self.tree.peek_mut() = None;
            self.cursor_count -= 1;
            // We have read all rows of this block, need to release the old memory and read a new one.
            let temp_block = DataBlock::take_by_slices_limit_from_blocks(
                &self.buffer,
                &self.temp_output_indices,
                None,
            );
            self.buffer[cursor.input_index] = DataBlock::empty_with_schema(self.schema.clone());
            self.temp_sorted_blocks.push(temp_block);
            self.temp_output_indices.clear();
            self.pending_streams.push_back(cursor.input_index);
        }

        debug_assert!(self.temp_sorted_num_rows <= max_rows);
        self.temp_sorted_num_rows == max_rows
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

        self.limit = self.limit.map(|limit| limit - self.temp_sorted_num_rows);
        self.temp_sorted_blocks.clear();
        self.temp_output_indices.clear();
        self.temp_sorted_num_rows = 0;

        Ok(block)
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
        if self.cursor_count == 0 {
            return if self.temp_sorted_num_rows > 0 {
                Ok(Some(self.build_output()?))
            } else {
                Ok(None)
            };
        }

        while let Some(Reverse(cursor)) = self.tree.peek() {
            if self.evaluate_cursor(cursor.clone()) {
                break;
            }
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
