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
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::Value;

use super::sort::RowConverter;
use super::sort::Rows;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
pub struct SortSpillParams {
    /// The number of rows of each spilled block.
    pub batch_rows: usize,
    /// The number of spilled blocks in each merge of the spill processor.
    pub num_merge: usize,
}

pub trait MergeSort<R: Rows> {
    const NAME: &'static str;

    /// Add a block to the merge sort processor.
    /// `block` is the input data block.
    /// `init_rows` is the initial sorting rows of this `block`.
    fn add_block(&mut self, block: DataBlock, init_rows: R) -> Result<()>;

    /// Return buffered data size.
    fn num_bytes(&self) -> usize;

    /// Return buffered rows.
    fn num_rows(&self) -> usize;

    /// Prepare the blocks to spill.
    fn prepare_spill(&mut self, spill_batch_rows: usize) -> Result<Vec<DataBlock>>;

    /// Finish the merge sorter and output the remain data.
    ///
    /// If `all_in_one_block`, the return value is a single block.
    fn on_finish(&mut self, all_in_one_block: bool) -> Result<Vec<DataBlock>>;

    fn interrupt(&self) {}
}

/// The base struct for merging sorted blocks from a single thread.
pub struct TransformSortMergeBase<M, R, Converter> {
    inner: M,

    row_converter: Converter,
    sort_desc: Arc<[SortColumnDescription]>,
    /// If the next transform of current transform is [`super::transform_multi_sort_merge::MultiSortMergeProcessor`],
    /// we can generate and output the order column to avoid the extra converting in the next transform.
    output_order_col: bool,
    /// If this transform is after an Exchange transform,
    /// it means it will compact the data from cluster nodes.
    /// And the order column is already generated in each cluster node,
    /// so we don't need to generate the order column again.
    order_col_generated: bool,

    _r: PhantomData<R>,
}

impl<M, R, C> TransformSortMergeBase<M, R, C>
where
    M: MergeSort<R>,
    R: Rows,
    C: RowConverter<R>,
{
    pub fn try_create(
        schema: DataSchemaRef,
        sort_desc: Arc<[SortColumnDescription]>,
        order_col_generated: bool,
        output_order_col: bool,
        inner: M,
    ) -> Result<Self> {
        let row_converter = C::create(&sort_desc, schema)?;

        Ok(Self {
            inner,
            row_converter,
            sort_desc,
            output_order_col,
            order_col_generated,
            _r: PhantomData,
        })
    }

    pub fn transform(&mut self, mut block: DataBlock) -> Result<()> {
        let rows = if self.order_col_generated {
            let rows = R::from_column(block.get_last_column())?;
            if !self.output_order_col {
                // The next processor could be a sort spill processor which need order column.
                // And the order column will be removed in that processor.
                block.pop_columns(1);
            }
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

        self.inner.add_block(block, rows)?;

        Ok(())
    }

    pub fn on_finish(&mut self) -> Result<Vec<DataBlock>> {
        self.inner.on_finish(false)
    }
}
