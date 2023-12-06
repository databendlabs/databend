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
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::with_number_mapped_type;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::SortColumnDescription;
use common_pipeline_core::processors::InputPort;
use common_pipeline_core::processors::OutputPort;
use common_pipeline_core::processors::Processor;

use super::sort::CommonRows;
use super::sort::Cursor;
use super::sort::DateConverter;
use super::sort::DateRows;
use super::sort::Rows;
use super::sort::SimpleRowConverter;
use super::sort::SimpleRows;
use super::sort::StringConverter;
use super::sort::StringRows;
use super::sort::TimestampConverter;
use super::sort::TimestampRows;
use super::transform_sort_merge_base::MergeSort;
use super::transform_sort_merge_base::Status;
use super::transform_sort_merge_base::TransformSortMergeBase;
use super::AccumulatingTransform;
use super::AccumulatingTransformer;

/// Merge sort blocks without limit.
///
/// For merge sort with limit, see [`super::transform_sort_merge_limit`]
pub struct TransformSortMerge<R: Rows> {
    block_size: usize,
    heap: BinaryHeap<Reverse<Cursor<R>>>,
    buffer: Vec<DataBlock>,

    aborting: Arc<AtomicBool>,
}

impl<R: Rows> TransformSortMerge<R> {
    pub fn create(block_size: usize) -> Self {
        TransformSortMerge {
            block_size,
            heap: BinaryHeap::new(),
            buffer: vec![],
            aborting: Arc::new(AtomicBool::new(false)),
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

type MergeSortDateImpl = TransformSortMerge<DateRows>;
type MergeSortDate = TransformSortMergeBase<MergeSortDateImpl, DateRows, DateConverter>;

type MergeSortTimestampImpl = TransformSortMerge<TimestampRows>;
type MergeSortTimestamp =
    TransformSortMergeBase<MergeSortTimestampImpl, TimestampRows, TimestampConverter>;

type MergeSortStringImpl = TransformSortMerge<StringRows>;
type MergeSortString = TransformSortMergeBase<MergeSortStringImpl, StringRows, StringConverter>;

type MergeSortCommonImpl = TransformSortMerge<CommonRows>;
type MergeSortCommon = TransformSortMergeBase<MergeSortCommonImpl, CommonRows, CommonConverter>;

pub fn try_create_transform_sort_merge(
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    schema: DataSchemaRef,
    block_size: usize,
    sort_desc: Vec<SortColumnDescription>,
    order_col_generated: bool,
    output_order_col: bool,
) -> Result<Box<dyn Processor>> {
    let processor = if sort_desc.len() == 1 {
        let sort_type = schema.field(sort_desc[0].offset).data_type();
        match sort_type {
            DataType::Number(num_ty) => with_number_mapped_type!(|NUM_TYPE| match num_ty {
                NumberDataType::NUM_TYPE => AccumulatingTransformer::create(
                    input,
                    output,
                    TransformSortMergeBase::<
                        TransformSortMerge<SimpleRows<NumberType<NUM_TYPE>>>,
                        SimpleRows<NumberType<NUM_TYPE>>,
                        SimpleRowConverter<NumberType<NUM_TYPE>>,
                    >::try_create(
                        schema,
                        sort_desc,
                        order_col_generated,
                        output_order_col,
                        TransformSortMerge::create(block_size),
                    )?,
                ),
            }),
            DataType::Date => AccumulatingTransformer::create(
                input,
                output,
                MergeSortDate::try_create(
                    schema,
                    sort_desc,
                    order_col_generated,
                    output_order_col,
                    MergeSortDateImpl::create(block_size),
                )?,
            ),
            DataType::Timestamp => AccumulatingTransformer::create(
                input,
                output,
                MergeSortTimestamp::try_create(
                    schema,
                    sort_desc,
                    order_col_generated,
                    output_order_col,
                    MergeSortTimestampImpl::create(block_size),
                )?,
            ),
            DataType::String => AccumulatingTransformer::create(
                input,
                output,
                MergeSortString::try_create(
                    schema,
                    sort_desc,
                    order_col_generated,
                    output_order_col,
                    MergeSortStringImpl::create(block_size),
                )?,
            ),
            _ => AccumulatingTransformer::create(
                input,
                output,
                MergeSortCommon::try_create(
                    schema,
                    sort_desc,
                    order_col_generated,
                    output_order_col,
                    MergeSortCommonImpl::create(block_size),
                )?,
            ),
        }
    } else {
        AccumulatingTransformer::create(
            input,
            output,
            MergeSortCommon::try_create(
                schema,
                sort_desc,
                order_col_generated,
                output_order_col,
                MergeSortCommonImpl::create(block_size),
            )?,
        )
    };

    Ok(processor)
}

pub fn sort_merge(
    data_schema: DataSchemaRef,
    block_size: usize,
    sort_desc: Vec<SortColumnDescription>,
    data_blocks: Vec<DataBlock>,
) -> Result<Vec<DataBlock>> {
    let mut processor = MergeSortCommon::try_create(
        data_schema,
        sort_desc,
        false,
        false,
        MergeSortCommonImpl::create(block_size),
    )?;
    for block in data_blocks {
        processor.transform(block)?;
    }
    processor.on_finish(true)
}
