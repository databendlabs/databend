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
use std::collections::HashMap;
use std::intrinsics::unlikely;
use std::sync::Arc;

use common_base::containers::FixedHeap;
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
use super::AccumulatingTransformer;

/// This is a specific version of [`super::transform_sort_merge::TransformSortMerge`] which sort blocks with limit.
pub struct TransformSortMergeLimit<R: Rows> {
    heap: FixedHeap<Reverse<Cursor<R>>>,
    buffer: HashMap<usize, DataBlock>,

    block_size: usize,
}

impl<R: Rows> MergeSort<R> for TransformSortMergeLimit<R> {
    const NAME: &'static str = "TransformSortMergeLimit";

    fn add_block(&mut self, block: DataBlock, mut cursor: Cursor<R>) -> Result<Status> {
        if unlikely(self.heap.cap() == 0 || block.is_empty()) {
            // limit is 0 or block is empty.
            return Ok(Status::Continue);
        }

        let cur_index = cursor.input_index;
        self.buffer.insert(cur_index, block);

        while !cursor.is_finished() {
            if let Some(Reverse(evict)) = self.heap.push(Reverse(cursor.clone())) {
                if evict.row_index == 0 {
                    // Evict the first row of the block,
                    // which means the block must not appear in the Top-N result.
                    self.buffer.remove(&evict.input_index);
                }

                if evict.input_index == cur_index {
                    // The Top-N heap is full, and later rows in current block cannot be put into the heap.
                    break;
                }
            }
            cursor.advance();
        }

        Ok(Status::Continue)
    }

    fn on_finish(&mut self) -> Result<Vec<DataBlock>> {
        if self.heap.is_empty() {
            return Ok(vec![]);
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

        let output_block_num = output_size.div_ceil(self.block_size);
        let mut output_blocks = Vec::with_capacity(output_block_num);

        for i in 0..output_block_num {
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
            let block = DataBlock::take_by_slices_limit_from_blocks(&blocks, &merge_slices, None);
            output_blocks.push(block);
        }

        Ok(output_blocks)
    }
}

impl<R: Rows> TransformSortMergeLimit<R> {
    pub fn create(block_size: usize, limit: usize) -> Self {
        TransformSortMergeLimit {
            heap: FixedHeap::new(limit),
            buffer: HashMap::with_capacity(limit),
            block_size,
        }
    }
}

type MergeSortDateImpl = TransformSortMergeLimit<DateRows>;
type MergeSortDate = TransformSortMergeBase<MergeSortDateImpl, DateRows, DateConverter>;

type MergeSortTimestampImpl = TransformSortMergeLimit<TimestampRows>;
type MergeSortTimestamp =
    TransformSortMergeBase<MergeSortTimestampImpl, TimestampRows, TimestampConverter>;

type MergeSortStringImpl = TransformSortMergeLimit<StringRows>;
type MergeSortString = TransformSortMergeBase<MergeSortStringImpl, StringRows, StringConverter>;

type MergeSortCommonImpl = TransformSortMergeLimit<CommonRows>;
type MergeSortCommon = TransformSortMergeBase<MergeSortCommonImpl, CommonRows, CommonConverter>;

#[allow(clippy::too_many_arguments)]
pub fn try_create_transform_sort_merge_limit(
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    schema: DataSchemaRef,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    block_size: usize,
    limit: usize,
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
                        TransformSortMergeLimit<SimpleRows<NumberType<NUM_TYPE>>>,
                        SimpleRows<NumberType<NUM_TYPE>>,
                        SimpleRowConverter<NumberType<NUM_TYPE>>,
                    >::try_create(
                        schema,
                        sort_desc,
                        order_col_generated,
                        output_order_col,
                        TransformSortMergeLimit::create(block_size, limit),
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
                    MergeSortDateImpl::create(block_size, limit),
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
                    MergeSortTimestampImpl::create(block_size, limit),
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
                    MergeSortStringImpl::create(block_size, limit),
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
                    MergeSortCommonImpl::create(block_size, limit),
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
                MergeSortCommonImpl::create(block_size, limit),
            )?,
        )
    };

    Ok(processor)
}
