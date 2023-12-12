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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::row::RowConverter as CommonConverter;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::SortColumnDescription;

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

/// Merge sort blocks without limit.
///
/// For merge sort with limit, see [`super::transform_sort_merge_limit`]
pub struct TransformSortMerge<R: Rows> {
    block_size: usize,
    heap: BinaryHeap<Reverse<Cursor<R>>>,
    buffer: Vec<DataBlock>,

    aborting: Arc<AtomicBool>,
    // The following fields are used for spilling.
    // may_spill: bool,
    // max_memory_usage: usize,
    // spilling_bytes_threshold: usize,
}

impl<R: Rows> TransformSortMerge<R> {
    pub fn create(
        block_size: usize,
        _max_memory_usage: usize,
        _spilling_bytes_threshold: usize,
    ) -> Self {
        // let may_spill = max_memory_usage != 0 && spilling_bytes_threshold != 0;
        TransformSortMerge {
            block_size,
            heap: BinaryHeap::new(),
            buffer: vec![],
            aborting: Arc::new(AtomicBool::new(false)),
            // may_spill,
            // max_memory_usage,
            // spilling_bytes_threshold,
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

        self.buffer.push(block);
        self.heap.push(Reverse(init_cursor));
        Ok(Status::Continue)
    }

    fn on_finish(&mut self) -> Result<Vec<DataBlock>> {
        let output_size = self.buffer.iter().map(|b| b.num_rows()).sum::<usize>();
        if output_size == 0 {
            return Ok(vec![]);
        }

        let output_block_num = output_size.div_ceil(self.block_size);
        let mut output_blocks = Vec::with_capacity(output_block_num);
        let mut output_indices = Vec::with_capacity(output_size);

        // 1. Drain the heap
        while let Some(Reverse(mut cursor)) = self.heap.pop() {
            if unlikely(self.aborting.load(Ordering::Relaxed)) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }

            let block_idx = cursor.input_index;
            if self.heap.is_empty() {
                // If there is no other block in the heap, we can drain the whole block.
                while !cursor.is_finished() {
                    output_indices.push((block_idx, cursor.advance()));
                }
            } else {
                let next_cursor = &self.heap.peek().unwrap().0;
                // If the last row of current block is smaller than the next cursor,
                // we can drain the whole block.
                if cursor.last().le(&next_cursor.current()) {
                    while !cursor.is_finished() {
                        output_indices.push((block_idx, cursor.advance()));
                    }
                } else {
                    while !cursor.is_finished() && cursor.le(next_cursor) {
                        // If the cursor is smaller than the next cursor, don't need to push the cursor back to the heap.
                        output_indices.push((block_idx, cursor.advance()));
                    }
                    if !cursor.is_finished() {
                        self.heap.push(Reverse(cursor));
                    }
                }
            }
        }

        // 2. Build final blocks from `output_indices`.
        for i in 0..output_block_num {
            if unlikely(self.aborting.load(Ordering::Relaxed)) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }

            let start = i * self.block_size;
            let end = (start + self.block_size).min(output_indices.len());
            // Convert indices to merge slice.
            let mut merge_slices = Vec::with_capacity(output_indices.len());
            let (block_idx, row_idx) = output_indices[start];
            merge_slices.push((block_idx, row_idx, 1));
            for (block_idx, row_idx) in output_indices.iter().take(end).skip(start + 1) {
                if *block_idx == merge_slices.last().unwrap().0 {
                    // If the block index is the same as the last one, we can merge them.
                    merge_slices.last_mut().unwrap().2 += 1;
                } else {
                    merge_slices.push((*block_idx, *row_idx, 1));
                }
            }
            let block =
                DataBlock::take_by_slices_limit_from_blocks(&self.buffer, &merge_slices, None);
            output_blocks.push(block);
        }

        Ok(output_blocks)
    }

    fn interrupt(&self) {
        self.aborting.store(true, Ordering::Release);
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
