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

use std::marker::PhantomData;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::SortColumnDescription;
use common_expression::Value;

use super::sort::Cursor;
use super::sort::RowConverter;
use super::sort::Rows;
use super::AccumulatingTransform;

pub enum Status {
    /// Continue to add blocks.
    Continue,
    // TODO(spill): transfer these blocks to spilling transform.
    // Need to spill blocks.
    // This status is not used currently.
    // Spill,
}

pub trait MergeSort<R: Rows> {
    const NAME: &'static str;

    /// Add a block to the merge sort processor.
    /// `block` is the input data block.
    /// `init_cursor` is the initial sorting cursor of this `block`.
    fn add_block(&mut self, block: DataBlock, init_cursor: Cursor<R>) -> Result<Status>;

    fn on_finish(&mut self) -> Result<Vec<DataBlock>>;

    fn interrupt(&self) {}
}

/// The base struct for merging sorted blocks from a single thread.
pub struct TransformSortMergeBase<M, R, Converter> {
    inner: M,

    row_converter: Converter,
    sort_desc: Vec<SortColumnDescription>,
    /// If the next transform of current transform is [`super::transform_multi_sort_merge::MultiSortMergeProcessor`],
    /// we can generate and output the order column to avoid the extra converting in the next transform.
    output_order_col: bool,
    /// If this transform is after an Exchange transform,
    /// it means it will compact the data from cluster nodes.
    /// And the order column is already generated in each cluster node,
    /// so we don't need to generate the order column again.
    order_col_generated: bool,

    /// The index for the next input block.
    next_index: usize,

    _r: PhantomData<R>,
}

impl<M, R, Converter> TransformSortMergeBase<M, R, Converter>
where
    M: MergeSort<R>,
    R: Rows,
    Converter: RowConverter<R>,
{
    pub fn try_create(
        schema: DataSchemaRef,
        sort_desc: Vec<SortColumnDescription>,
        order_col_generated: bool,
        output_order_col: bool,
        inner: M,
    ) -> Result<Self> {
        debug_assert!(if order_col_generated {
            // If the order column is already generated,
            // it means this transform is after a exchange source and it's the last transform for sorting.
            // We should remove the order column.
            !output_order_col
        } else {
            true
        });

        let row_converter = Converter::create(&sort_desc, schema)?;

        Ok(Self {
            inner,
            row_converter,
            sort_desc,
            output_order_col,
            order_col_generated,
            next_index: 0,
            _r: PhantomData,
        })
    }
}

impl<M, R, Converter> AccumulatingTransform for TransformSortMergeBase<M, R, Converter>
where
    M: MergeSort<R> + Send + Sync,
    R: Rows + Send + Sync,
    Converter: RowConverter<R> + Send + Sync,
{
    const NAME: &'static str = M::NAME;

    fn transform(&mut self, mut block: DataBlock) -> Result<Vec<DataBlock>> {
        let rows = if self.order_col_generated {
            let order_col = block
                .columns()
                .last()
                .unwrap()
                .value
                .as_column()
                .unwrap()
                .clone();
            let rows = R::from_column(order_col, &self.sort_desc)
                .ok_or_else(|| ErrorCode::BadDataValueType("Order column type mismatched."))?;
            // Need to remove order column.
            block.pop_columns(1);
            rows
        } else {
            let order_by_cols = self
                .sort_desc
                .iter()
                .map(|d| block.get_by_offset(d.offset).clone())
                .collect::<Vec<_>>();
            let rows = self
                .row_converter
                .convert(&order_by_cols, block.num_rows())?;
            if self.output_order_col {
                let order_col = rows.to_column();
                block.add_column(BlockEntry {
                    data_type: order_col.data_type(),
                    value: Value::Column(order_col),
                });
            }
            rows
        };

        let cursor = Cursor::new(self.next_index, rows);
        self.next_index += 1;

        match self.inner.add_block(block, cursor)? {
            Status::Continue => Ok(vec![]),
        }
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        self.inner.on_finish()
    }
}
