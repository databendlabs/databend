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

use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_exception::Result;
use databend_common_expression::row::RowConverter as CommonConverter;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::Value;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use match_template::match_template;

use super::sort::utils::ORDER_COL_NAME;
use super::sort::CommonRows;
use super::sort::RowConverter;
use super::sort::Rows;
use super::sort::SimpleRowConverter;
use super::sort::SimpleRowsAsc;
use super::sort::SimpleRowsDesc;
use super::sort::SortSpillMeta;
use super::sort::SortSpillMetaWithParams;
use super::AccumulatingTransform;
use super::AccumulatingTransformer;
use super::TransformSortMerge;
use super::TransformSortMergeLimit;

/// The memory will be doubled during merging.
const MERGE_RATIO: usize = 2;

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
    fn add_block(&mut self, block: DataBlock, init_rows: R, input_index: usize) -> Result<()>;

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
    sort_desc: Arc<Vec<SortColumnDescription>>,
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

    // The following fields are used for spilling.
    may_spill: bool,
    max_memory_usage: usize,
    spilling_bytes_threshold: usize,
    spilling_batch_bytes: usize,

    // The spill_params will be passed to the spill processor.
    // If spill_params is Some, it means we need to spill.
    spill_params: Option<SortSpillParams>,

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
        sort_desc: Arc<Vec<SortColumnDescription>>,
        order_col_generated: bool,
        output_order_col: bool,
        max_memory_usage: usize,
        spilling_bytes_threshold: usize,
        spilling_batch_bytes: usize,
        inner: M,
    ) -> Result<Self> {
        let may_spill = max_memory_usage != 0 && spilling_bytes_threshold != 0;
        let row_converter = Converter::create(&sort_desc, schema)?;

        Ok(Self {
            inner,
            row_converter,
            sort_desc,
            output_order_col,
            order_col_generated,
            next_index: 0,
            max_memory_usage,
            spilling_bytes_threshold,
            spilling_batch_bytes,
            spill_params: None,
            may_spill,
            _r: PhantomData,
        })
    }

    fn prepare_spill(&mut self) -> Result<Vec<DataBlock>> {
        let mut spill_params = if self.spill_params.is_none() {
            // We use the first memory calculation to estimate the batch size and the number of merge.
            let num_merge = self
                .inner
                .num_bytes()
                .div_ceil(self.spilling_batch_bytes)
                .max(2);
            let batch_rows = self.inner.num_rows().div_ceil(num_merge);
            // The first block to spill will contain the parameters of spilling.
            // Later blocks just contain a empty struct `SortSpillMeta` to save memory.
            let params = SortSpillParams {
                batch_rows,
                num_merge,
            };
            self.spill_params = Some(params);
            Some(params)
        } else {
            None
        };

        let mut blocks = self
            .inner
            .prepare_spill(self.spill_params.unwrap().batch_rows)?;

        // Fill the spill meta.
        for b in blocks.iter_mut() {
            b.replace_meta(match spill_params.take() {
                Some(params) => Box::new(SortSpillMetaWithParams(params)),
                None => Box::new(SortSpillMeta {}),
            });
        }

        debug_assert_eq!(self.inner.num_bytes(), 0);
        debug_assert_eq!(self.inner.num_rows(), 0);
        // Re-count the block index.
        self.next_index = 0;

        Ok(blocks)
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
            let rows = R::from_column(block.get_last_column(), &self.sort_desc)?;
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

        self.inner.add_block(block, rows, self.next_index)?;
        self.next_index += 1;
        let blocks = if self.may_spill
            && (self.inner.num_bytes() * MERGE_RATIO >= self.spilling_bytes_threshold
                || GLOBAL_MEM_STAT.get_memory_usage() as usize >= self.max_memory_usage)
        {
            self.prepare_spill()?
        } else {
            vec![]
        };

        Ok(blocks)
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        // If the processor has started to spill blocks,
        // gather the final few data in one block.
        self.inner.on_finish(self.spill_params.is_some())
    }
}

pub struct TransformSortMergeBuilder {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    schema: DataSchemaRef,
    block_size: usize,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    order_col_generated: bool,
    output_order_col: bool,
    max_memory_usage: usize,
    spilling_bytes_threshold_per_core: usize,
    spilling_batch_bytes: usize,
    enable_loser_tree: bool,
    limit: Option<usize>,
}

impl TransformSortMergeBuilder {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        sort_desc: Arc<Vec<SortColumnDescription>>,
        block_size: usize,
    ) -> Self {
        Self {
            input,
            output,
            block_size,
            schema,
            sort_desc,
            order_col_generated: false,
            output_order_col: false,
            max_memory_usage: 0,
            spilling_bytes_threshold_per_core: 0,
            spilling_batch_bytes: 8 * 1024 * 1024,
            enable_loser_tree: false,
            limit: None,
        }
    }

    pub fn with_order_col_generated(mut self, order_col_generated: bool) -> Self {
        self.order_col_generated = order_col_generated;
        self
    }

    pub fn with_output_order_col(mut self, output_order_col: bool) -> Self {
        self.output_order_col = output_order_col;
        self
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_max_memory_usage(mut self, max_memory_usage: usize) -> Self {
        self.max_memory_usage = max_memory_usage;
        self
    }

    pub fn with_spilling_bytes_threshold_per_core(
        mut self,
        spilling_bytes_threshold_per_core: usize,
    ) -> Self {
        self.spilling_bytes_threshold_per_core = spilling_bytes_threshold_per_core;
        self
    }

    pub fn with_spilling_batch_bytes(mut self, spilling_batch_bytes: usize) -> Self {
        if spilling_batch_bytes > 0 {
            self.spilling_batch_bytes = spilling_batch_bytes;
        }
        self
    }

    pub fn with_enable_loser_tree(mut self, enable_loser_tree: bool) -> Self {
        self.enable_loser_tree = enable_loser_tree;
        self
    }

    pub fn build(self) -> Result<Box<dyn Processor>> {
        debug_assert!(if self.output_order_col {
            self.schema.has_field(ORDER_COL_NAME)
        } else {
            !self.schema.has_field(ORDER_COL_NAME)
        });

        if self.limit.is_some() {
            self.build_sort_limit()
        } else {
            self.build_sort()
        }
    }

    fn build_sort(self) -> Result<Box<dyn Processor>> {
        if self.sort_desc.len() == 1 {
            let sort_type = self.schema.field(self.sort_desc[0].offset).data_type();
            let asc = self.sort_desc[0].asc;

            match_template! {
                T = [ Date => DateType, Timestamp => TimestampType, String => StringType ],
                match sort_type {
                    DataType::T => self.build_sort_rows_simple::<T>(asc),
                    DataType::Number(num_ty) => with_number_mapped_type!(|NUM_TYPE| match num_ty {
                        NumberDataType::NUM_TYPE => {
                            self.build_sort_rows_simple::<NumberType<NUM_TYPE>>(asc)
                        }
                    }),
                    _ => self.build_sort_rows::<CommonRows, CommonConverter>(),
                }
            }
        } else {
            self.build_sort_rows::<CommonRows, CommonConverter>()
        }
    }

    fn build_sort_rows_simple<T>(self, asc: bool) -> Result<Box<dyn Processor>>
    where
        T: ArgType + Send + Sync,
        T::Column: Send + Sync,
        for<'a> T::ScalarRef<'a>: Ord,
    {
        if asc {
            self.build_sort_rows::<SimpleRowsAsc<T>, SimpleRowConverter<T>>()
        } else {
            self.build_sort_rows::<SimpleRowsDesc<T>, SimpleRowConverter<T>>()
        }
    }

    fn build_sort_rows<R, C>(self) -> Result<Box<dyn Processor>>
    where
        R: Rows + Send + Sync + 'static,
        C: RowConverter<R> + Send + Sync + 'static,
    {
        Ok(AccumulatingTransformer::create(
            self.input,
            self.output,
            TransformSortMergeBase::<TransformSortMerge<R>, R, C>::try_create(
                self.schema.clone(),
                self.sort_desc.clone(),
                self.order_col_generated,
                self.output_order_col,
                self.max_memory_usage,
                self.spilling_bytes_threshold_per_core,
                self.spilling_batch_bytes,
                TransformSortMerge::create(
                    self.schema,
                    self.sort_desc,
                    self.block_size,
                    self.enable_loser_tree,
                ),
            )?,
        ))
    }

    fn build_sort_limit(self) -> Result<Box<dyn Processor>> {
        if self.sort_desc.len() == 1 {
            let sort_type = self.schema.field(self.sort_desc[0].offset).data_type();
            let asc = self.sort_desc[0].asc;

            match_template! {
                T = [ Date => DateType, Timestamp => TimestampType, String => StringType ],
                match sort_type {
                    DataType::T => self.build_sort_limit_simple::<T>(asc),
                    DataType::Number(num_ty) => with_number_mapped_type!(|NUM_TYPE| match num_ty {
                        NumberDataType::NUM_TYPE => {
                            self.build_sort_limit_simple::<NumberType<NUM_TYPE>>(asc)
                        }
                    }),
                    _ => self.build_sort_limit_rows::<CommonRows, CommonConverter>(),
                }
            }
        } else {
            self.build_sort_limit_rows::<CommonRows, CommonConverter>()
        }
    }

    fn build_sort_limit_simple<T>(self, asc: bool) -> Result<Box<dyn Processor>>
    where
        T: ArgType + Send + Sync,
        T::Column: Send + Sync,
        for<'a> T::ScalarRef<'a>: Ord,
    {
        if asc {
            self.build_sort_limit_rows::<SimpleRowsAsc<T>, SimpleRowConverter<T>>()
        } else {
            self.build_sort_limit_rows::<SimpleRowsDesc<T>, SimpleRowConverter<T>>()
        }
    }

    fn build_sort_limit_rows<R, C>(self) -> Result<Box<dyn Processor>>
    where
        R: Rows + Send + Sync + 'static,
        C: RowConverter<R> + Send + Sync + 'static,
    {
        Ok(AccumulatingTransformer::create(
            self.input,
            self.output,
            TransformSortMergeBase::<TransformSortMergeLimit<R>, R, C>::try_create(
                self.schema,
                self.sort_desc,
                self.order_col_generated,
                self.output_order_col,
                self.max_memory_usage,
                self.spilling_bytes_threshold_per_core,
                self.spilling_batch_bytes,
                TransformSortMergeLimit::create(self.block_size, self.limit.unwrap()),
            )?,
        ))
    }
}
