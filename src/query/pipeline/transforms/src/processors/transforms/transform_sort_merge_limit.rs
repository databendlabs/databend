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
use std::cmp::Reverse;
use std::collections::HashMap;
use std::intrinsics::unlikely;

use databend_common_base::containers::FixedHeap;
use databend_common_exception::Result;
use databend_common_expression::row::RowConverter as CommonConverter;
use databend_common_expression::DataBlock;

use super::sort::CommonRows;
use super::sort::Cursor;
use super::sort::CursorOrder;
use super::sort::DateConverter;
use super::sort::DateRows;
use super::sort::Rows;
use super::sort::StringConverter;
use super::sort::StringRows;
use super::sort::TimestampConverter;
use super::sort::TimestampRows;
use super::transform_sort_merge_base::MergeSort;
use super::transform_sort_merge_base::TransformSortMergeBase;

/// This is a specific version of [`super::transform_sort_merge::TransformSortMerge`] which sort blocks with limit.
pub struct TransformSortMergeLimit<R: Rows> {
    heap: FixedHeap<Reverse<Cursor<R, LocalCursorOrder>>>,
    buffer: HashMap<usize, DataBlock>,

    /// Record current memory usage.
    num_bytes: usize,
    num_rows: usize,

    block_size: usize,
}

impl<R: Rows> MergeSort<R> for TransformSortMergeLimit<R> {
    const NAME: &'static str = "TransformSortMergeLimit";

    fn add_block(&mut self, block: DataBlock, init_rows: R, input_index: usize) -> Result<()> {
        if unlikely(self.heap.cap() == 0 || block.is_empty()) {
            // limit is 0 or block is empty.
            return Ok(());
        }

        let mut cursor = Cursor::new(input_index, init_rows);
        self.num_bytes += block.memory_size();
        self.num_rows += block.num_rows();
        let cur_index = input_index;
        self.buffer.insert(cur_index, block);

        while !cursor.is_finished() {
            if let Some(Reverse(evict)) = self.heap.push(Reverse(cursor.clone())) {
                if evict.row_index == 0 {
                    // Evict the first row of the block,
                    // which means the block must not appear in the Top-N result.
                    if let Some(block) = self.buffer.remove(&evict.input_index) {
                        self.num_bytes -= block.memory_size();
                        self.num_rows -= block.num_rows();
                    }
                }

                if evict.input_index == cur_index {
                    // The Top-N heap is full, and later rows in current block cannot be put into the heap.
                    break;
                }
            }
            cursor.advance();
        }

        Ok(())
    }

    fn on_finish(&mut self, all_in_one_block: bool) -> Result<Vec<DataBlock>> {
        if all_in_one_block {
            Ok(self.drain_heap(self.num_rows))
        } else {
            Ok(self.drain_heap(self.block_size))
        }
    }

    #[inline(always)]
    fn num_bytes(&self) -> usize {
        self.num_bytes
    }

    #[inline(always)]
    fn num_rows(&self) -> usize {
        self.num_rows
    }

    fn prepare_spill(&mut self, spill_batch_size: usize) -> Result<Vec<DataBlock>> {
        // TBD: if it's better to add the blocks back to the heap.
        // Reason: the output `blocks` is a result of Top-N,
        // so the memory usage will be less than the original buffered data.
        // If the reduced memory usage does not reach the spilling threshold,
        // we can avoid one spilling.
        let blocks = self.drain_heap(spill_batch_size);

        debug_assert!(self.buffer.is_empty());

        Ok(blocks)
    }
}

#[derive(Clone, Copy)]
struct LocalCursorOrder;

impl<R: Rows> CursorOrder<R> for LocalCursorOrder {
    fn eq(a: &Cursor<R, Self>, b: &Cursor<R, Self>) -> bool {
        (a.input_index == b.input_index && a.row_index == b.row_index) || a.current() == b.current()
    }

    fn cmp(a: &Cursor<R, Self>, b: &Cursor<R, Self>) -> Ordering {
        if a.input_index == b.input_index {
            return a.row_index.cmp(&b.row_index);
        }
        a.current()
            .cmp(&b.current())
            .then_with(|| a.input_index.cmp(&b.input_index))
    }
}

impl<R: Rows> TransformSortMergeLimit<R> {
    pub fn create(block_size: usize, limit: usize) -> Self {
        debug_assert!(limit <= 10000, "Too large sort merge limit: {}", limit);
        TransformSortMergeLimit {
            heap: FixedHeap::new(limit),
            buffer: HashMap::with_capacity(limit),
            block_size,
            num_bytes: 0,
            num_rows: 0,
        }
    }

    fn drain_heap(&mut self, batch_size: usize) -> Vec<DataBlock> {
        if self.heap.is_empty() {
            return vec![];
        }

        let output_size = self.heap.len();
        let block_indices = self.buffer.keys().cloned().collect::<Vec<_>>();
        let blocks = self.buffer.values().cloned().collect::<Vec<_>>();
        let mut output_indices = Vec::with_capacity(output_size);
        while let Some(Reverse(cursor)) = self.heap.pop() {
            let block_index = block_indices
                .iter()
                .position(|i| *i == cursor.input_index)
                .unwrap();

            output_indices.push((block_index, cursor.row_index));
        }

        let output_block_num = output_size.div_ceil(batch_size);
        let mut output_blocks = Vec::with_capacity(output_block_num);

        for i in 0..output_block_num {
            let start = i * batch_size;
            let end = (start + batch_size).min(output_indices.len());
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
            let block = DataBlock::take_by_slices_limit_from_blocks(&blocks, &merge_slices, None);
            output_blocks.push(block);
        }

        self.buffer.clear();
        self.num_bytes = 0;
        self.num_rows = 0;

        output_blocks
    }
}

pub(super) type MergeSortLimitDateImpl = TransformSortMergeLimit<DateRows>;
pub(super) type MergeSortLimitDate =
    TransformSortMergeBase<MergeSortLimitDateImpl, DateRows, DateConverter>;

pub(super) type MergeSortLimitTimestampImpl = TransformSortMergeLimit<TimestampRows>;
pub(super) type MergeSortLimitTimestamp =
    TransformSortMergeBase<MergeSortLimitTimestampImpl, TimestampRows, TimestampConverter>;

pub(super) type MergeSortLimitStringImpl = TransformSortMergeLimit<StringRows>;
pub(super) type MergeSortLimitString =
    TransformSortMergeBase<MergeSortLimitStringImpl, StringRows, StringConverter>;

pub(super) type MergeSortLimitCommonImpl = TransformSortMergeLimit<CommonRows>;
pub(super) type MergeSortLimitCommon =
    TransformSortMergeBase<MergeSortLimitCommonImpl, CommonRows, CommonConverter>;
