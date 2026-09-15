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

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::future::Future;

use databend_common_exception::Result;
use databend_common_expression::ChunkIndex;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataBlockVec;
use databend_common_expression::types::DataType;
use futures::future::Either;

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

/// Owns both the sort cursors and the rows their items borrow from.
///
/// `cursors` must be declared before `rows` so it is dropped first.
struct CursorStorage<A: SortAlgorithm> {
    cursors: A,
    rows: Box<[Option<A::Rows>]>,
}

impl<A: SortAlgorithm> CursorStorage<A> {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            cursors: A::with_capacity(capacity),
            rows: (0..capacity)
                .map(|_| None)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.cursors.is_empty()
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.cursors.len()
    }

    #[inline(always)]
    fn peek(&self) -> Option<&Cursor<A::Rows>> {
        self.cursors.peek()
    }

    #[inline(always)]
    fn peek_top2(&self) -> &Cursor<A::Rows> {
        self.cursors.peek_top2()
    }

    fn rows(&self, stream_index: usize) -> &A::Rows {
        self.rows[stream_index]
            .as_ref()
            .expect("cursor must have originating rows")
    }

    fn item_cmp(
        &self,
        stream_index: usize,
        row_index: usize,
        item: <A::Rows as Rows>::Item<'static>,
    ) -> Ordering {
        // Safety: the item is used only while its originating Rows is borrowed.
        unsafe { self.rows(stream_index).row_stable(row_index).cmp(&item) }
    }

    fn push(&mut self, stream_index: usize, rows: A::Rows) {
        debug_assert!(self.rows[stream_index].is_none());
        self.rows[stream_index] = Some(rows);

        let rows = self.rows[stream_index].as_ref().unwrap();
        let num_rows = rows.len();
        debug_assert!(num_rows > 0);
        // Safety: Rows guarantees its items survive moving the Rows wrapper.
        // CursorStorage keeps the originating Rows alive until this cursor is
        // removed, and its field order drops all cursors before any rows.
        let (current, last) = unsafe { (rows.row_stable(0), rows.row_stable(num_rows - 1)) };
        let cursor = Cursor::new(stream_index, num_rows, current, last);
        self.cursors.push(stream_index, cursor);
    }

    fn rebuild(&mut self) {
        self.cursors.rebuild();
    }

    /// Advances the top cursor and returns its stream index when it is exhausted.
    fn advance_top(&mut self, count: usize) -> Option<usize> {
        let cursor = self.cursors.peek().unwrap();
        let stream_index = cursor.input_index;
        let row_index = cursor.row_index + count;
        let num_rows = self.rows(stream_index).len();
        debug_assert!(row_index <= num_rows);
        // Safety: the originating Rows remains in this storage until after the
        // cursor has been removed below.
        let current = (row_index < num_rows)
            .then(|| unsafe { self.rows(stream_index).row_stable(row_index) });

        let mut peek_mut = self.cursors.peek_mut();
        let cursor = &mut peek_mut;
        cursor.advance(count, current);

        if !cursor.is_finished() {
            return None;
        }

        A::pop_mut(peek_mut);
        self.rows[stream_index] = None;
        Some(stream_index)
    }
}

/// A merge sort operator to merge multiple sorted streams and output one sorted stream.
pub struct Merger<A, S>
where A: SortAlgorithm
{
    batch_rows: usize,
    limit: Option<usize>,
    unsorted_streams: Vec<S>,

    pending_streams: VecDeque<usize>,
    cursor_storage: CursorStorage<A>,
    buffers: BufferState,
}

impl<A, S> Merger<A, S>
where A: SortAlgorithm
{
    pub fn new(streams: Vec<S>, batch_rows: usize, limit: Option<usize>) -> Self {
        // We only create a merger when there are at least two streams.
        debug_assert!(streams.len() > 1, "streams.len() = {}", streams.len());

        let cursor_storage = CursorStorage::with_capacity(streams.len());
        let pending_streams = (0..streams.len()).collect();
        let buffers = BufferState::new(streams.len());

        Self {
            unsorted_streams: streams,
            cursor_storage,
            batch_rows,
            limit,
            pending_streams,
            buffers,
        }
    }

    #[inline(always)]
    pub fn is_finished(&self) -> bool {
        (self.cursor_storage.is_empty() && !self.has_pending_stream() && !self.buffers.has_output())
            || self.limit == Some(0)
    }

    #[inline(always)]
    pub fn has_pending_stream(&self) -> bool {
        !self.pending_streams.is_empty()
    }

    /// To evaluate the current cursor, and update the top of the cursor storage if necessary.
    /// This method can only be called when iterating the cursor storage.
    ///
    /// Return `true` if the batch is full (need to output).
    #[inline(always)]
    fn evaluate_cursor(&mut self) -> bool {
        let cursor = if let Some(cursor) = self.cursor_storage.peek() {
            *cursor
        } else {
            return false;
        };

        let stream_index = cursor.input_index;
        let buffer_index = self.buffers.stream_to_buffer[stream_index]
            .expect("cursor must point to active stream buffer");
        let start = cursor.row_index;
        let count = self.evaluate_cursor_count(&cursor);

        self.buffers.record_output_range(buffer_index, start, count);

        if let Some(stream_index) = self.cursor_storage.advance_top(count) {
            self.buffers.detach(buffer_index, stream_index);
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

        if self.cursor_storage.len() == 1 || cursor.current() == cursor.last() {
            return row_index_limit - start;
        }

        if !A::SHOULD_PEEK_TOP2 {
            let mut row_index = start + 1;
            let item = cursor.current();
            while row_index < row_index_limit
                && self
                    .cursor_storage
                    .item_cmp(cursor.input_index, row_index, item)
                    == Ordering::Equal
            {
                row_index += 1;
            }
            return row_index - start;
        }

        let next_cursor = self.cursor_storage.peek_top2();
        if cursor.last() <= next_cursor.current() {
            // Short Path:
            // If the last row of current block is smaller than the next cursor,
            // we can drain the whole block.
            return row_index_limit - start;
        }

        let mut row_index = start + 1;
        let item = next_cursor.current();
        while row_index < row_index_limit
            && self
                .cursor_storage
                .item_cmp(cursor.input_index, row_index, item)
                != Ordering::Greater
        {
            // If the cursor is equals or smaller than the next cursor, continue advance.
            row_index += 1;
        }
        row_index - start
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
                self.cursor_storage.push(i, rows);
            }
        }
        self.pending_streams.extend(continue_pendings);
        // `rebuild` is the mutation/read barrier: no cursor is observed while
        // an input can still contribute an unresolved next block.
        if self.pending_streams.is_empty() {
            self.cursor_storage.rebuild();
        }
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
        if self.cursor_storage.is_empty() {
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
    // This method can only be called when there is no data of the stream in the cursor storage.
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
                self.cursor_storage.push(i, rows);
            }
        }
        self.pending_streams.extend(continue_pendings);
        // Keep the same read barrier as the synchronous path.
        if self.pending_streams.is_empty() {
            self.cursor_storage.rebuild();
        }
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
        if self.cursor_storage.is_empty() {
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

pub type LoserTreeMerger<R, S> = Merger<LoserTreeTop2Sort<R>, S>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeAlgorithm {
    Heap,
    LoserTree,
    LoserTreeTop2,
}

/// Runtime selection around fully monomorphized mergers.
///
/// Dispatch happens once per public merger operation. The cursor evaluation and
/// loser-tree replay loops remain inside a concrete [`Merger<A, S>`], so they do
/// not pay an enum branch per row or per replay.
pub enum SelectedMerger<R: Rows, S> {
    Heap(Merger<HeapSort<R>, S>),
    LoserTree(Merger<LoserTreeSort<R>, S>),
    LoserTreeTop2(Merger<LoserTreeTop2Sort<R>, S>),
}

impl<R: Rows, S> SelectedMerger<R, S> {
    pub fn new(
        algorithm: MergeAlgorithm,
        streams: Vec<S>,
        batch_rows: usize,
        limit: Option<usize>,
    ) -> Self {
        match algorithm {
            MergeAlgorithm::Heap => Self::Heap(Merger::new(streams, batch_rows, limit)),
            MergeAlgorithm::LoserTree => Self::LoserTree(Merger::new(streams, batch_rows, limit)),
            MergeAlgorithm::LoserTreeTop2 => {
                Self::LoserTreeTop2(Merger::new(streams, batch_rows, limit))
            }
        }
    }

    pub fn new_auto(
        streams: Vec<S>,
        batch_rows: usize,
        limit: Option<usize>,
        enable_loser_tree: bool,
    ) -> Self {
        let algorithm = if !enable_loser_tree {
            MergeAlgorithm::Heap
        } else if streams.len() >= 16 {
            MergeAlgorithm::LoserTree
        } else if std::matches!(
            R::data_type(),
            DataType::Boolean
                | DataType::Number(_)
                | DataType::Decimal(_)
                | DataType::Timestamp
                | DataType::TimestampTz
                | DataType::Date
                | DataType::Interval
                | DataType::Opaque(_)
        ) {
            MergeAlgorithm::Heap
        } else {
            MergeAlgorithm::LoserTreeTop2
        };
        Self::new(algorithm, streams, batch_rows, limit)
    }

    pub fn is_finished(&self) -> bool {
        match self {
            Self::Heap(merger) => merger.is_finished(),
            Self::LoserTreeTop2(merger) => merger.is_finished(),
            Self::LoserTree(merger) => merger.is_finished(),
        }
    }

    pub fn has_pending_stream(&self) -> bool {
        match self {
            Self::Heap(merger) => merger.has_pending_stream(),
            Self::LoserTreeTop2(merger) => merger.has_pending_stream(),
            Self::LoserTree(merger) => merger.has_pending_stream(),
        }
    }

    pub fn streams(self) -> Vec<S> {
        match self {
            Self::Heap(merger) => merger.streams(),
            Self::LoserTreeTop2(merger) => merger.streams(),
            Self::LoserTree(merger) => merger.streams(),
        }
    }
}

impl<R, S> SelectedMerger<R, S>
where
    R: Rows,
    S: SortedStream + Send,
{
    pub fn poll_pending_stream(&mut self) -> Result<()> {
        match self {
            Self::Heap(merger) => merger.poll_pending_stream(),
            Self::LoserTreeTop2(merger) => merger.poll_pending_stream(),
            Self::LoserTree(merger) => merger.poll_pending_stream(),
        }
    }

    pub fn next_block(&mut self) -> Result<Option<DataBlock>> {
        match self {
            Self::Heap(merger) => merger.next_block(),
            Self::LoserTreeTop2(merger) => merger.next_block(),
            Self::LoserTree(merger) => merger.next_block(),
        }
    }
}

impl<R, S> SelectedMerger<R, S>
where
    R: Rows,
    S: AsyncSortedStream + Send,
{
    pub fn async_poll_pending_stream(&mut self) -> impl Future<Output = Result<()>> + Send + '_ {
        match self {
            Self::Heap(merger) => Either::Left(Either::Left(merger.async_poll_pending_stream())),
            Self::LoserTreeTop2(merger) => {
                Either::Left(Either::Right(merger.async_poll_pending_stream()))
            }
            Self::LoserTree(merger) => Either::Right(merger.async_poll_pending_stream()),
        }
    }

    pub fn async_next_block(
        &mut self,
    ) -> impl Future<Output = Result<Option<DataBlock>>> + Send + '_ {
        match self {
            Self::Heap(merger) => Either::Left(Either::Left(merger.async_next_block())),
            Self::LoserTreeTop2(merger) => Either::Left(Either::Right(merger.async_next_block())),
            Self::LoserTree(merger) => Either::Right(merger.async_next_block()),
        }
    }
}
