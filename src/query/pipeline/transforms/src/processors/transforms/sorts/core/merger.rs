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
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;

use super::algorithm::*;
use super::Rows;

#[async_trait::async_trait]
pub trait SortedStream {
    /// Returns the next block with the order column and if it is pending.
    ///
    /// If the block is [None] and it's not pending, it means the stream is finished.
    /// If the block is [None] but it's pending, it means the stream is not finished yet.
    fn next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        Ok((None, false))
    }

    /// The async version of `next`.
    async fn async_next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        self.next()
    }
}

/// A merge sort operator to merge multiple sorted streams and output one sorted stream.
pub struct Merger<A, S>
where
    A: SortAlgorithm,
    S: SortedStream,
{
    schema: DataSchemaRef,
    unsorted_streams: Vec<S>,
    sorted_cursors: A,
    buffer: Vec<DataBlock>,
    pending_streams: VecDeque<usize>,
    batch_rows: usize,
    limit: Option<usize>,

    temp_sorted_num_rows: usize,
    temp_output_indices: Vec<(usize, usize, usize)>,
    temp_sorted_blocks: Vec<DataBlock>,
}

impl<A, S> Merger<A, S>
where
    A: SortAlgorithm,
    S: SortedStream + Send,
{
    pub fn create(
        schema: DataSchemaRef,
        streams: Vec<S>,
        batch_rows: usize,
        limit: Option<usize>,
    ) -> Self {
        // We only create a merger when there are at least two streams.
        debug_assert!(streams.len() > 1, "streams.len() = {}", streams.len());

        let sorted_cursors = A::with_capacity(streams.len());
        let buffer = vec![DataBlock::empty_with_schema(schema.clone()); streams.len()];
        let pending_streams = (0..streams.len()).collect();

        Self {
            schema,
            unsorted_streams: streams,
            sorted_cursors,
            buffer,
            batch_rows,
            limit,
            pending_streams,
            temp_sorted_num_rows: 0,
            temp_output_indices: vec![],
            temp_sorted_blocks: vec![],
        }
    }

    #[inline(always)]
    pub fn is_finished(&self) -> bool {
        (self.sorted_cursors.is_empty()
            && !self.has_pending_stream()
            && self.temp_sorted_num_rows == 0)
            || self.limit == Some(0)
    }

    #[inline(always)]
    pub fn has_pending_stream(&self) -> bool {
        !self.pending_streams.is_empty()
    }

    // This method can only be called when there is no data of the stream in the sorted_cursors.
    pub async fn async_poll_pending_stream(&mut self) -> Result<()> {
        let mut continue_pendings = Vec::new();
        while let Some(i) = self.pending_streams.pop_front() {
            debug_assert!(self.buffer[i].is_empty());
            let (input, pending) = self.unsorted_streams[i].async_next().await?;
            if pending {
                continue_pendings.push(i);
                continue;
            }
            if let Some((block, col)) = input {
                let rows = A::Rows::from_column(&col)?;
                let cursor = Cursor::new(i, rows);
                self.sorted_cursors.push(i, Reverse(cursor));
                self.buffer[i] = block;
            }
        }
        self.sorted_cursors.rebuild();
        self.pending_streams.extend(continue_pendings);
        Ok(())
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
                let rows = A::Rows::from_column(&col)?;
                let cursor = Cursor::new(i, rows);
                self.sorted_cursors.push(i, Reverse(cursor));
                self.buffer[i] = block;
            }
        }
        self.sorted_cursors.rebuild();
        self.pending_streams.extend(continue_pendings);
        Ok(())
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

        let input_index = cursor.input_index;
        let start = cursor.row_index;
        let count = self.evaluate_cursor_count(cursor);

        self.temp_sorted_num_rows += count;
        self.push_output_indices((input_index, start, count));

        // `self.sorted_cursors.peek_mut` will return a `PeekMut` object which allows us to modify the top element of the sorted_cursors.
        // The sorted_cursors will adjust itself automatically when the `PeekMut` object is dropped (RAII).
        let mut peek_mut = self.sorted_cursors.peek_mut();
        let cursor = &mut peek_mut.0;
        cursor.row_index += count;

        if cursor.is_finished() {
            // Pop the current `cursor`.
            A::pop_mut(peek_mut);
            // We have read all rows of this block, need to release the old memory and read a new one.
            let temp_block = DataBlock::take_by_slices_limit_from_blocks(
                &self.buffer,
                &self.temp_output_indices,
                None,
            );
            self.buffer[input_index] = DataBlock::empty_with_schema(self.schema.clone());
            self.temp_sorted_blocks.push(temp_block);
            self.temp_output_indices.clear();
            self.pending_streams.push_back(input_index);
        }

        let max_rows = self.limit.unwrap_or(self.batch_rows).min(self.batch_rows);
        debug_assert!(self.temp_sorted_num_rows <= max_rows);
        self.temp_sorted_num_rows != max_rows
    }

    #[inline(always)]
    fn evaluate_cursor_count(&self, cursor: &Cursor<A::Rows>) -> usize {
        debug_assert!(!cursor.is_finished());
        let start = cursor.row_index;
        let max_rows = self.limit.unwrap_or(self.batch_rows).min(self.batch_rows);
        let row_index_limit = cursor
            .num_rows()
            .min(start + max_rows - self.temp_sorted_num_rows);

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

    fn push_output_indices(&mut self, (input, start, count): (usize, usize, usize)) {
        match self.temp_output_indices.last_mut() {
            Some((pre_input, pre_start, pre_count)) if input == *pre_input => {
                debug_assert_eq!(*pre_start + *pre_count, start);
                *pre_count += count
            }
            _ => self.temp_output_indices.push((input, start, count)),
        }
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
        debug_assert!(block.num_rows() <= self.batch_rows);

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
        if self.sorted_cursors.is_empty() {
            return if self.temp_sorted_num_rows > 0 {
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
            return if self.temp_sorted_num_rows > 0 {
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

    pub fn streams(self) -> Vec<S> {
        self.unsorted_streams
    }
}

pub type HeapMerger<R, S> = Merger<HeapSort<R>, S>;

pub type LoserTreeMerger<R, S> = Merger<LoserTreeSort<R>, S>;
