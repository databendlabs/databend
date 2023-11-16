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

use common_base::containers::FixedHeap;
use common_exception::Result;
use common_expression::row::RowConverter as CommonRowConverter;
use common_expression::types::string::StringColumn;
use common_expression::types::DataType;
use common_expression::types::DateType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::types::TimestampType;
use common_expression::with_number_mapped_type;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::SortColumnDescription;
use common_expression::Value;
use common_pipeline_core::processors::InputPort;
use common_pipeline_core::processors::OutputPort;
use common_pipeline_core::processors::Processor;

use super::sort::Cursor;
use super::sort::RowConverter;
use super::sort::Rows;
use super::sort::SimpleRowConverter;
use super::sort::SimpleRows;
use super::AccumulatingTransform;
use super::AccumulatingTransformer;

pub struct TransformSortMergeLimit<R: Rows, Converter> {
    row_converter: Converter,
    order_by_cols: Vec<usize>,
    heap: FixedHeap<Reverse<Cursor<Arc<R>>>>,

    buffer: HashMap<usize, DataBlock>,
    cur_index: usize,

    block_size: usize,

    gen_order_col: bool,
}

impl<R, Converter> TransformSortMergeLimit<R, Converter>
where
    R: Rows,
    Converter: RowConverter<R>,
{
    pub fn try_create(
        schema: DataSchemaRef,
        sort_desc: Vec<SortColumnDescription>,
        block_size: usize,
        limit: usize,
        gen_order_col: bool,
    ) -> Result<Self> {
        let order_by_cols = sort_desc.iter().map(|i| i.offset).collect::<Vec<_>>();
        let row_converter = Converter::create(sort_desc, schema)?;
        Ok(TransformSortMergeLimit {
            row_converter,
            order_by_cols,
            heap: FixedHeap::new(limit),
            buffer: HashMap::with_capacity(limit),
            block_size,
            cur_index: 0,
            gen_order_col,
        })
    }
}

impl<R, Converter> AccumulatingTransform for TransformSortMergeLimit<R, Converter>
where
    R: Rows + Send + Sync,
    Converter: RowConverter<R> + Send + Sync,
{
    const NAME: &'static str = "TransformSortMergeLimit";

    fn transform(&mut self, mut data: DataBlock) -> Result<Vec<DataBlock>> {
        if self.heap.cap() == 0 {
            // limit is 0
            return Ok(vec![]);
        }

        if data.is_empty() {
            return Ok(vec![]);
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

        if self.gen_order_col {
            let order_col = rows.to_column();
            data.add_column(BlockEntry {
                data_type: order_col.data_type(),
                value: Value::Column(order_col),
            });
        }

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
        Ok(vec![])
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
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

type SimpleDateTransform =
    TransformSortMergeLimit<SimpleRows<DateType>, SimpleRowConverter<DateType>>;
type SimpleDateSort = AccumulatingTransformer<SimpleDateTransform>;

type SimpleTimestampTransform =
    TransformSortMergeLimit<SimpleRows<TimestampType>, SimpleRowConverter<TimestampType>>;
type SimpleTimestampSort = AccumulatingTransformer<SimpleTimestampTransform>;

type SimpleStringTransform =
    TransformSortMergeLimit<SimpleRows<StringType>, SimpleRowConverter<StringType>>;
type SimpleStringSort = AccumulatingTransformer<SimpleStringTransform>;

type CommonTransform = TransformSortMergeLimit<StringColumn, CommonRowConverter>;
type CommonSort = AccumulatingTransformer<CommonTransform>;

pub fn try_create_transform_sort_merge_limit(
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    input_schema: DataSchemaRef,
    sort_desc: Vec<SortColumnDescription>,
    block_size: usize,
    limit: usize,
    gen_order_col: bool,
) -> Result<Box<dyn Processor>> {
    Ok(if sort_desc.len() == 1 {
        let sort_type = input_schema.field(sort_desc[0].offset).data_type();
        match sort_type {
            DataType::Number(num_ty) => with_number_mapped_type!(|NUM_TYPE| match num_ty {
                NumberDataType::NUM_TYPE => AccumulatingTransformer::<
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
                    >::try_create(
                        input_schema, sort_desc, block_size, limit, gen_order_col
                    )?
                ),
            }),
            DataType::Date => SimpleDateSort::create(
                input,
                output,
                SimpleDateTransform::try_create(
                    input_schema,
                    sort_desc,
                    block_size,
                    limit,
                    gen_order_col,
                )?,
            ),
            DataType::Timestamp => SimpleTimestampSort::create(
                input,
                output,
                SimpleTimestampTransform::try_create(
                    input_schema,
                    sort_desc,
                    block_size,
                    limit,
                    gen_order_col,
                )?,
            ),
            DataType::String => SimpleStringSort::create(
                input,
                output,
                SimpleStringTransform::try_create(
                    input_schema,
                    sort_desc,
                    block_size,
                    limit,
                    gen_order_col,
                )?,
            ),
            _ => CommonSort::create(
                input,
                output,
                CommonTransform::try_create(
                    input_schema,
                    sort_desc,
                    block_size,
                    limit,
                    gen_order_col,
                )?,
            ),
        }
    } else {
        CommonSort::create(
            input,
            output,
            CommonTransform::try_create(input_schema, sort_desc, block_size, limit, gen_order_col)?,
        )
    })
}
