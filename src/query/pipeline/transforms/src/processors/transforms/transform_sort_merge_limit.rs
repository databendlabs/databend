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
use std::sync::Arc;

use common_arrow::arrow::compute::sort::row::RowConverter as ArrowRowConverter;
use common_arrow::arrow::compute::sort::row::Rows as ArrowRows;
use common_base::containers::FixedHeap;
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
use super::AsyncAccumulatingTransform;
use super::AsyncAccumulatingTransformer;

pub struct TransformSortMergeLimit<R: Rows, Converter> {
    row_converter: Converter,
    order_by_cols: Vec<usize>,
    heap: FixedHeap<Reverse<Cursor<Arc<R>>>>,

    buffer: HashMap<usize, DataBlock>,
    cur_index: usize,
}

impl<R, Converter> TransformSortMergeLimit<R, Converter>
where
    R: Rows,
    Converter: RowConverter<R>,
{
    pub fn try_create(
        schema: DataSchemaRef,
        sort_desc: Vec<SortColumnDescription>,
        limit: usize,
    ) -> Result<Self> {
        let order_by_cols = sort_desc.iter().map(|i| i.offset).collect::<Vec<_>>();
        let row_converter = Converter::create(sort_desc, schema)?;
        Ok(TransformSortMergeLimit {
            row_converter,
            order_by_cols,
            heap: FixedHeap::new(limit),
            buffer: HashMap::with_capacity(limit),
            cur_index: 0,
        })
    }
}

#[async_trait::async_trait]
impl<R, Converter> AsyncAccumulatingTransform for TransformSortMergeLimit<R, Converter>
where
    R: Rows + Send + Sync,
    Converter: RowConverter<R> + Send + Sync,
{
    const NAME: &'static str = "TransformSortMergeLimit";

    async fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>> {
        if self.heap.cap() == 0 {
            return Ok(None);
        }

        let order_by_cols = self
            .order_by_cols
            .iter()
            .map(|i| data.get_by_offset(*i).clone())
            .collect::<Vec<_>>();
        let rows = Arc::new(
            self.row_converter
                .convert(&order_by_cols, data.num_rows())?,
        );
        let mut cursor = Cursor::new(self.cur_index, rows);
        self.buffer.insert(self.cur_index, data);

        while !cursor.is_finished() {
            if let Some(Reverse(evict)) = self.heap.push(Reverse(cursor.clone())) {
                if evict.row_index == 0 {
                    // Evict the first row of the block,
                    // which means the block must not appear in the Top-N result.
                    self.buffer.remove(&evict.input_index);
                }

                if evict.input_index == self.cur_index {
                    // The Top-N heap is full, and later rows in current block cannot be put into the heap.
                    break;
                }
            }
            cursor.advance();
        }

        self.cur_index += 1;
        Ok(None)
    }

    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        if self.heap.is_empty() {
            return Ok(None);
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

        let (block_idx, row_idx) = output_indices[0];
        let mut merge_slices = Vec::with_capacity(output_indices.len());
        merge_slices.push((block_idx, row_idx, 1));
        for (block_idx, row_idx) in output_indices.iter().skip(1) {
            if *block_idx == merge_slices.last().unwrap().0 {
                // If the block index is the same as the last one, we can merge them.
                merge_slices.last_mut().unwrap().2 += 1;
            } else {
                merge_slices.push((*block_idx, *row_idx, 1));
            }
        }

        let block = DataBlock::take_by_slices_limit_from_blocks(&blocks, &merge_slices, None);

        Ok(Some(block))
    }
}

type SimpleDateTransform =
    TransformSortMergeLimit<SimpleRows<DateType>, SimpleRowConverter<DateType>>;
type SimpleDateSort = AsyncAccumulatingTransformer<SimpleDateTransform>;

type SimpleTimestampTransform =
    TransformSortMergeLimit<SimpleRows<TimestampType>, SimpleRowConverter<TimestampType>>;
type SimpleTimestampSort = AsyncAccumulatingTransformer<SimpleTimestampTransform>;

type SimpleStringTransform =
    TransformSortMergeLimit<SimpleRows<StringType>, SimpleRowConverter<StringType>>;
type SimpleStringSort = AsyncAccumulatingTransformer<SimpleStringTransform>;

type CommonTransform = TransformSortMergeLimit<ArrowRows, ArrowRowConverter>;
type CommonSort = AsyncAccumulatingTransformer<CommonTransform>;

pub fn try_create_transform_sort_merge_limit(
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    input_schema: DataSchemaRef,
    sort_desc: Vec<SortColumnDescription>,
    limit: usize,
) -> Result<Box<dyn Processor>> {
    Ok(if sort_desc.len() == 1 {
        let sort_type = input_schema.field(sort_desc[0].offset).data_type();
        match sort_type {
            DataType::Number(num_ty) => with_number_mapped_type!(|NUM_TYPE| match num_ty {
                NumberDataType::NUM_TYPE => AsyncAccumulatingTransformer::<
                    TransformSortMergeLimit<
                        SimpleRows<NumberType<NUM_TYPE>>,
                        SimpleRowConverter<NumberType<NUM_TYPE>>,
                    >,
                >::create(
                    input,
                    output,
                    TransformSortMergeLimit::<
                        SimpleRows<NumberType<NUM_TYPE>>,
                        SimpleRowConverter<NumberType<NUM_TYPE>>,
                    >::try_create(input_schema, sort_desc, limit)?
                ),
            }),
            DataType::Date => SimpleDateSort::create(
                input,
                output,
                SimpleDateTransform::try_create(input_schema, sort_desc, limit)?,
            ),
            DataType::Timestamp => SimpleTimestampSort::create(
                input,
                output,
                SimpleTimestampTransform::try_create(input_schema, sort_desc, limit)?,
            ),
            DataType::String => SimpleStringSort::create(
                input,
                output,
                SimpleStringTransform::try_create(input_schema, sort_desc, limit)?,
            ),
            _ => CommonSort::create(
                input,
                output,
                CommonTransform::try_create(input_schema, sort_desc, limit)?,
            ),
        }
    } else {
        CommonSort::create(
            input,
            output,
            CommonTransform::try_create(input_schema, sort_desc, limit)?,
        )
    })
}
