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
use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_arrow::arrow::compute::sort::row::RowConverter as ArrowRowConverter;
use common_arrow::arrow::compute::sort::row::Rows as ArrowRows;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::DateType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::types::TimestampType;
use common_expression::with_number_mapped_type;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::SortColumnDescription;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::Processor;

use super::sort::Cursor;
use super::sort::RowConverter;
use super::sort::Rows;
use super::sort::SimpleRowConverter;
use super::sort::SimpleRows;
use super::Compactor;
use super::TransformCompact;

pub struct SortMergeCompactor<R, Converter> {
    block_size: usize,
    limit: Option<usize>,
    sort_columns_descriptions: Vec<SortColumnDescription>,
    aborting: Arc<AtomicBool>,
    schema: DataSchemaRef,

    _c: PhantomData<Converter>,
    _r: PhantomData<R>,
}

impl<R, Converter> SortMergeCompactor<R, Converter>
where
    R: Rows,
    Converter: RowConverter<R>,
{
    pub fn try_create(
        schema: DataSchemaRef,
        block_size: usize,
        limit: Option<usize>,
        sort_desc: Vec<SortColumnDescription>,
    ) -> Result<Self> {
        Ok(SortMergeCompactor {
            schema,
            block_size,
            limit,
            sort_columns_descriptions: sort_desc,
            aborting: Arc::new(AtomicBool::new(false)),
            _c: PhantomData,
            _r: PhantomData,
        })
    }
}

impl<R, Converter> Compactor for SortMergeCompactor<R, Converter>
where
    R: Rows,
    Converter: RowConverter<R>,
{
    fn name() -> &'static str {
        "SortMergeTransform"
    }

    fn interrupt(&self) {
        self.aborting.store(true, Ordering::Release);
    }

    fn compact_final(&self, blocks: &[DataBlock]) -> Result<Vec<DataBlock>> {
        if blocks.is_empty() {
            return Ok(vec![]);
        }

        let all_rows = blocks.iter().map(|b| b.num_rows()).sum::<usize>();
        let output_size = self.limit.unwrap_or(all_rows).min(all_rows);

        if output_size == 0 {
            return Ok(vec![]);
        }

        let mut row_converter =
            Converter::create(self.sort_columns_descriptions.clone(), self.schema.clone())?;

        let output_block_num = output_size.div_ceil(self.block_size);
        let mut output_blocks = Vec::with_capacity(output_block_num);
        let mut output_indices = Vec::with_capacity(output_size);
        let mut heap: BinaryHeap<Reverse<Cursor<R>>> = BinaryHeap::with_capacity(blocks.len());

        // 1. Put all blocks into a min-heap.
        for (i, block) in blocks.iter().enumerate() {
            if block.is_empty() {
                continue;
            }
            let columns = self
                .sort_columns_descriptions
                .iter()
                .map(|i| block.get_by_offset(i.offset).clone())
                .collect::<Vec<_>>();
            let rows = row_converter.convert(&columns, block.num_rows())?;
            let cursor = Cursor::try_create(i, rows);
            heap.push(Reverse(cursor));
        }

        // 2. Drain the heap till the limit is reached.
        let mut row = 0;
        while row < output_size {
            match heap.pop() {
                Some(Reverse(mut cursor)) => {
                    let block_idx = cursor.input_index;
                    if heap.is_empty() {
                        // If there is no other block in the heap, we can drain the whole block.
                        while output_size > 0 && !cursor.is_finished() {
                            output_indices.push((block_idx, cursor.advance()));
                            row += 1;
                        }
                    } else {
                        let next_cursor = &heap.peek().unwrap().0;
                        // If the last row of current block is smaller than the next cursor,
                        // we can drain the whole block.
                        if cursor.last().le(&next_cursor.current()) {
                            while output_size > 0 && !cursor.is_finished() {
                                output_indices.push((block_idx, cursor.advance()));
                                row += 1;
                            }
                        } else {
                            while output_size > 0 && !cursor.is_finished() && cursor.le(next_cursor)
                            {
                                // If the cursor is smaller than the next cursor, don't need to push the cursor back to the heap.
                                output_indices.push((block_idx, cursor.advance()));
                                row += 1;
                            }
                            if output_size > 0 && !cursor.is_finished() {
                                heap.push(Reverse(cursor));
                            }
                        }
                    }
                }
                None => {
                    unreachable!("heap is empty, but we haven't reached the limit yet");
                }
            }
            if unlikely(row % self.block_size == 0 && self.aborting.load(Ordering::Relaxed)) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }
        }

        // 3. Build final blocks from `output_indices`.
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
            let block = DataBlock::take_by_slices_limit_from_blocks(blocks, &merge_slices, None);
            output_blocks.push(block);
        }

        Ok(output_blocks)
    }
}

type SimpleDateCompactor = SortMergeCompactor<SimpleRows<DateType>, SimpleRowConverter<DateType>>;
type SimpleDateSort = TransformCompact<SimpleDateCompactor>;

type SimpleTimestampCompactor =
    SortMergeCompactor<SimpleRows<TimestampType>, SimpleRowConverter<TimestampType>>;
type SimpleTimestampSort = TransformCompact<SimpleTimestampCompactor>;

type SimpleStringCompactor =
    SortMergeCompactor<SimpleRows<StringType>, SimpleRowConverter<StringType>>;
type SimpleStringSort = TransformCompact<SimpleStringCompactor>;

type CommonCompactor = SortMergeCompactor<ArrowRows, ArrowRowConverter>;
type CommonSort = TransformCompact<CommonCompactor>;

pub fn try_create_transform_sort_merge(
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_schema: DataSchemaRef,
    block_size: usize,
    limit: Option<usize>,
    sort_columns_descriptions: Vec<SortColumnDescription>,
) -> Result<Box<dyn Processor>> {
    if sort_columns_descriptions.len() == 1 {
        let sort_type = output_schema
            .field(sort_columns_descriptions[0].offset)
            .data_type();
        match sort_type {
            DataType::Number(num_ty) => with_number_mapped_type!(|NUM_TYPE| match num_ty {
                NumberDataType::NUM_TYPE => TransformCompact::<
                    SortMergeCompactor<
                        SimpleRows<NumberType<NUM_TYPE>>,
                        SimpleRowConverter<NumberType<NUM_TYPE>>,
                    >,
                >::try_create(
                    input,
                    output,
                    SortMergeCompactor::<
                        SimpleRows<NumberType<NUM_TYPE>>,
                        SimpleRowConverter<NumberType<NUM_TYPE>>,
                    >::try_create(
                        output_schema, block_size, limit, sort_columns_descriptions,
                    )?
                ),
            }),
            DataType::Date => SimpleDateSort::try_create(
                input,
                output,
                SimpleDateCompactor::try_create(
                    output_schema,
                    block_size,
                    limit,
                    sort_columns_descriptions,
                )?,
            ),
            DataType::Timestamp => SimpleTimestampSort::try_create(
                input,
                output,
                SimpleTimestampCompactor::try_create(
                    output_schema,
                    block_size,
                    limit,
                    sort_columns_descriptions,
                )?,
            ),
            DataType::String => SimpleStringSort::try_create(
                input,
                output,
                SimpleStringCompactor::try_create(
                    output_schema,
                    block_size,
                    limit,
                    sort_columns_descriptions,
                )?,
            ),
            _ => CommonSort::try_create(
                input,
                output,
                CommonCompactor::try_create(
                    output_schema,
                    block_size,
                    limit,
                    sort_columns_descriptions,
                )?,
            ),
        }
    } else {
        CommonSort::try_create(
            input,
            output,
            CommonCompactor::try_create(
                output_schema,
                block_size,
                limit,
                sort_columns_descriptions,
            )?,
        )
    }
}
