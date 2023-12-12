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
use std::intrinsics::unlikely;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_base::runtime::GLOBAL_MEM_STAT;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::row::RowConverter as CommonConverter;
use common_expression::BlockMetaInfo;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::SortColumnDescription;

use super::sort::utils::find_bigger_child_of_root;
use super::sort::CommonRows;
use super::sort::Cursor;
use super::sort::DateConverter;
use super::sort::DateRows;
use super::sort::Rows;
use super::sort::StringConverter;
use super::sort::StringRows;
use super::sort::TimestampConverter;
use super::sort::TimestampRows;
use super::transform_sort_merge_base::MergeSort;
use super::transform_sort_merge_base::Status;
use super::transform_sort_merge_base::TransformSortMergeBase;
use super::AccumulatingTransform;
use crate::processors::sort::SortSpillMeta;
use crate::processors::sort::SortSpillMetaWithParams;

/// A spilled block file is at most 8MB.
const SPILL_BATCH_BYTES_SIZE: usize = 8 * 1024 * 1024;

/// Merge sort blocks without limit.
///
/// For merge sort with limit, see [`super::transform_sort_merge_limit`]
pub struct TransformSortMerge<R: Rows> {
    block_size: usize,
    heap: BinaryHeap<Reverse<Cursor<R>>>,
    buffer: Vec<DataBlock>,

    aborting: Arc<AtomicBool>,
    // The following fields are used for spilling.
    may_spill: bool,
    max_memory_usage: usize,
    spilling_bytes_threshold: usize,
    /// Record current memory usage.
    num_bytes: usize,
    num_rows: usize,

    // The following two fields will be passed to the spill processor.
    // If these two fields are not zero, it means we need to spill.
    /// The number of rows of each spilled block.
    spill_batch_size: usize,
    /// The number of spilled blocks in each merge of the spill processor.
    spill_num_merge: usize,
}

impl<R: Rows> TransformSortMerge<R> {
    pub fn create(
        block_size: usize,
        max_memory_usage: usize,
        spilling_bytes_threshold: usize,
    ) -> Self {
        let may_spill = max_memory_usage != 0 && spilling_bytes_threshold != 0;
        TransformSortMerge {
            block_size,
            heap: BinaryHeap::new(),
            buffer: vec![],
            aborting: Arc::new(AtomicBool::new(false)),
            may_spill,
            max_memory_usage,
            spilling_bytes_threshold,
            num_bytes: 0,
            num_rows: 0,
            spill_batch_size: 0,
            spill_num_merge: 0,
        }
    }
}

impl<R: Rows> MergeSort<R> for TransformSortMerge<R> {
    const NAME: &'static str = "TransformSortMerge";

    fn add_block(&mut self, block: DataBlock, init_cursor: Cursor<R>) -> Result<Status> {
        if unlikely(self.aborting.load(Ordering::Relaxed)) {
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the server is shutting down or the query was killed.",
            ));
        }

        if unlikely(block.is_empty()) {
            return Ok(Status::Continue);
        }

        self.num_bytes += block.memory_size();
        self.num_rows += block.num_rows();
        self.buffer.push(block);
        self.heap.push(Reverse(init_cursor));

        if self.may_spill
            && (self.num_bytes >= self.spilling_bytes_threshold
                || GLOBAL_MEM_STAT.get_memory_usage() as usize >= self.max_memory_usage)
        {
            let blocks = self.prepare_spill()?;
            return Ok(Status::Spill(blocks));
        }

        Ok(Status::Continue)
    }

    fn on_finish(&mut self) -> Result<Vec<DataBlock>> {
        if self.spill_num_merge > 0 {
            debug_assert!(self.spill_batch_size > 0);
            // Make the last block as a big memory block.
            self.drain_heap(usize::MAX)
        } else {
            self.drain_heap(self.block_size)
        }
    }

    fn interrupt(&self) {
        self.aborting.store(true, Ordering::Release);
    }
}

impl<R: Rows> TransformSortMerge<R> {
    fn prepare_spill(&mut self) -> Result<Vec<DataBlock>> {
        let mut spill_meta = Box::new(SortSpillMeta {}) as Box<dyn BlockMetaInfo>;
        if self.spill_batch_size == 0 {
            debug_assert_eq!(self.spill_num_merge, 0);
            // We use the first memory calculation to estimate the batch size and the number of merge.
            self.spill_num_merge = self.num_bytes.div_ceil(SPILL_BATCH_BYTES_SIZE).max(2);
            self.spill_batch_size = self.num_rows.div_ceil(self.spill_num_merge);
            // The first block to spill will contain the parameters of spilling.
            // Later blocks just contain a empty struct `SortSpillMeta` to save memory.
            spill_meta = Box::new(SortSpillMetaWithParams {
                batch_size: self.spill_batch_size,
                num_merge: self.spill_num_merge,
            }) as Box<dyn BlockMetaInfo>;
        } else {
            debug_assert!(self.spill_num_merge > 0);
        }

        let mut blocks = self.drain_heap(self.spill_batch_size)?;
        if let Some(b) = blocks.first_mut() {
            b.replace_meta(spill_meta);
        }
        for b in blocks.iter_mut().skip(1) {
            b.replace_meta(Box::new(SortSpillMeta {}));
        }

        debug_assert!(self.heap.is_empty());
        self.num_rows = 0;
        self.num_bytes = 0;
        self.buffer.clear();

        Ok(blocks)
    }

    fn drain_heap(&mut self, batch_size: usize) -> Result<Vec<DataBlock>> {
        // TODO: the codes is highly duplicated with the codes in `transform_sort_spill.rs`,
        // need to refactor and merge them later.
        if self.num_rows == 0 {
            return Ok(vec![]);
        }

        let output_block_num = self.num_rows.div_ceil(batch_size);
        let mut output_blocks = Vec::with_capacity(output_block_num);
        let mut output_indices = Vec::with_capacity(output_block_num);

        // 1. Drain the heap
        let mut temp_num_rows = 0;
        let mut temp_indices = Vec::new();
        while let Some(Reverse(cursor)) = self.heap.peek() {
            if unlikely(self.aborting.load(Ordering::Relaxed)) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }

            let mut cursor = cursor.clone();
            if self.heap.len() == 1 {
                let start = cursor.row_index;
                let count = (cursor.num_rows() - start).min(batch_size - temp_num_rows);
                temp_num_rows += count;
                cursor.row_index += count;
                temp_indices.push((cursor.input_index, start, count));
            } else {
                let next_cursor = &find_bigger_child_of_root(&self.heap).0;
                if cursor.last().le(&next_cursor.current()) {
                    // Short Path:
                    // If the last row of current block is smaller than the next cursor,
                    // we can drain the whole block.
                    let start = cursor.row_index;
                    let count = (cursor.num_rows() - start).min(batch_size - temp_num_rows);
                    temp_num_rows += count;
                    cursor.row_index += count;
                    temp_indices.push((cursor.input_index, start, count));
                } else {
                    // We copy current cursor for advancing,
                    // and we will use this copied cursor to update the top of the heap at last
                    // (let heap adjust itself without popping and pushing any element).
                    let start = cursor.row_index;
                    while !cursor.is_finished()
                        && cursor.le(next_cursor)
                        && temp_num_rows < batch_size
                    {
                        // If the cursor is smaller than the next cursor, don't need to push the cursor back to the heap.
                        temp_num_rows += 1;
                        cursor.advance();
                    }
                    temp_indices.push((cursor.input_index, start, cursor.row_index - start));
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
            }

            if temp_num_rows == batch_size {
                output_indices.push(temp_indices.clone());
                temp_indices.clear();
                temp_num_rows = 0;
            }
        }

        if !temp_indices.is_empty() {
            output_indices.push(temp_indices);
        }

        // 2. Build final blocks from `output_indices`.
        for indices in output_indices {
            if unlikely(self.aborting.load(Ordering::Relaxed)) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }

            let block = DataBlock::take_by_slices_limit_from_blocks(&self.buffer, &indices, None);
            output_blocks.push(block);
        }

        Ok(output_blocks)
    }
}

pub(super) type MergeSortDateImpl = TransformSortMerge<DateRows>;
pub(super) type MergeSortDate = TransformSortMergeBase<MergeSortDateImpl, DateRows, DateConverter>;

pub(super) type MergeSortTimestampImpl = TransformSortMerge<TimestampRows>;
pub(super) type MergeSortTimestamp =
    TransformSortMergeBase<MergeSortTimestampImpl, TimestampRows, TimestampConverter>;

pub(super) type MergeSortStringImpl = TransformSortMerge<StringRows>;
pub(super) type MergeSortString =
    TransformSortMergeBase<MergeSortStringImpl, StringRows, StringConverter>;

pub(super) type MergeSortCommonImpl = TransformSortMerge<CommonRows>;
pub(super) type MergeSortCommon =
    TransformSortMergeBase<MergeSortCommonImpl, CommonRows, CommonConverter>;

pub fn sort_merge(
    data_schema: DataSchemaRef,
    block_size: usize,
    sort_desc: Vec<SortColumnDescription>,
    data_blocks: Vec<DataBlock>,
) -> Result<Vec<DataBlock>> {
    let mut processor = MergeSortCommon::try_create(
        data_schema,
        Arc::new(sort_desc),
        false,
        false,
        MergeSortCommonImpl::create(block_size, 0, 0),
    )?;
    for block in data_blocks {
        processor.transform(block)?;
    }
    processor.on_finish(true)
}
