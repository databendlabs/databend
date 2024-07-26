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
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::Value;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;

use super::sort::RowConverter;
use super::sort::Rows;
use super::sort::SimpleRowConverter;
use super::sort::SimpleRows;
use super::sort::SortSpillMeta;
use super::AccumulatingTransform;
use super::AccumulatingTransformer;
use super::MergeSortCommon;
use super::MergeSortCommonImpl;
use super::MergeSortDate;
use super::MergeSortDateImpl;
use super::MergeSortLimitCommon;
use super::MergeSortLimitCommonImpl;
use super::MergeSortLimitDate;
use super::MergeSortLimitDateImpl;
use super::MergeSortLimitString;
use super::MergeSortLimitStringImpl;
use super::MergeSortLimitTimestamp;
use super::MergeSortLimitTimestampImpl;
use super::MergeSortString;
use super::MergeSortStringImpl;
use super::MergeSortTimestamp;
use super::MergeSortTimestampImpl;
use super::TransformSortMerge;
use super::TransformSortMergeLimit;
use crate::processors::sort::utils::ORDER_COL_NAME;
use crate::processors::sort::SortSpillMetaWithParams;

/// The memory will be doubled during merging.
const MERGE_RATIO: usize = 2;

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
    // The following two fields will be passed to the spill processor.
    // If these two fields are not zero, it means we need to spill.
    /// The number of rows of each spilled block.
    spill_batch_rows: usize,
    /// The number of spilled blocks in each merge of the spill processor.
    spill_num_merge: usize,

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

            spill_batch_rows: 0,
            spill_num_merge: 0,
            may_spill,
            _r: PhantomData,
        })
    }

    fn prepare_spill(&mut self) -> Result<Vec<DataBlock>> {
        let mut spill_meta = Box::new(SortSpillMeta {}) as Box<dyn BlockMetaInfo>;
        if self.spill_batch_rows == 0 {
            debug_assert_eq!(self.spill_num_merge, 0);
            // We use the first memory calculation to estimate the batch size and the number of merge.
            self.spill_num_merge = self
                .inner
                .num_bytes()
                .div_ceil(self.spilling_batch_bytes)
                .max(2);
            self.spill_batch_rows = self.inner.num_rows().div_ceil(self.spill_num_merge);
            // The first block to spill will contain the parameters of spilling.
            // Later blocks just contain a empty struct `SortSpillMeta` to save memory.
            spill_meta = Box::new(SortSpillMetaWithParams {
                batch_rows: self.spill_batch_rows,
                num_merge: self.spill_num_merge,
            }) as Box<dyn BlockMetaInfo>;
        } else {
            debug_assert!(self.spill_num_merge > 0);
        }

        let mut blocks = self.inner.prepare_spill(self.spill_batch_rows)?;

        // Fill the spill meta.
        if let Some(b) = blocks.first_mut() {
            b.replace_meta(spill_meta);
        }
        for b in blocks.iter_mut().skip(1) {
            b.replace_meta(Box::new(SortSpillMeta {}));
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
        self.inner.on_finish(self.spill_num_merge > 0)
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
            self.build_sort_merge_limit()
        } else {
            self.build_sort_merge()
        }
    }

    fn build_sort_merge(self) -> Result<Box<dyn Processor>> {
        let Self {
            input,
            output,
            schema,
            block_size,
            sort_desc,
            order_col_generated,
            output_order_col,
            max_memory_usage,
            spilling_bytes_threshold_per_core,
            spilling_batch_bytes,
            enable_loser_tree,
            ..
        } = self;

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
                            schema.clone(),
                            sort_desc.clone(),
                            order_col_generated,
                            output_order_col,
                            max_memory_usage,
                            spilling_bytes_threshold_per_core,
                            spilling_batch_bytes,
                            TransformSortMerge::create(
                                schema,
                                sort_desc,
                                block_size,
                                enable_loser_tree
                            ),
                        )?,
                    ),
                }),
                DataType::Date => AccumulatingTransformer::create(
                    input,
                    output,
                    MergeSortDate::try_create(
                        schema.clone(),
                        sort_desc.clone(),
                        order_col_generated,
                        output_order_col,
                        max_memory_usage,
                        spilling_bytes_threshold_per_core,
                        spilling_batch_bytes,
                        MergeSortDateImpl::create(schema, sort_desc, block_size, enable_loser_tree),
                    )?,
                ),
                DataType::Timestamp => AccumulatingTransformer::create(
                    input,
                    output,
                    MergeSortTimestamp::try_create(
                        schema.clone(),
                        sort_desc.clone(),
                        order_col_generated,
                        output_order_col,
                        max_memory_usage,
                        spilling_bytes_threshold_per_core,
                        spilling_batch_bytes,
                        MergeSortTimestampImpl::create(
                            schema,
                            sort_desc,
                            block_size,
                            enable_loser_tree,
                        ),
                    )?,
                ),
                DataType::String => AccumulatingTransformer::create(
                    input,
                    output,
                    MergeSortString::try_create(
                        schema.clone(),
                        sort_desc.clone(),
                        order_col_generated,
                        output_order_col,
                        max_memory_usage,
                        spilling_bytes_threshold_per_core,
                        spilling_batch_bytes,
                        MergeSortStringImpl::create(
                            schema,
                            sort_desc,
                            block_size,
                            enable_loser_tree,
                        ),
                    )?,
                ),
                _ => AccumulatingTransformer::create(
                    input,
                    output,
                    MergeSortCommon::try_create(
                        schema.clone(),
                        sort_desc.clone(),
                        order_col_generated,
                        output_order_col,
                        max_memory_usage,
                        spilling_bytes_threshold_per_core,
                        spilling_batch_bytes,
                        MergeSortCommonImpl::create(
                            schema,
                            sort_desc,
                            block_size,
                            enable_loser_tree,
                        ),
                    )?,
                ),
            }
        } else {
            AccumulatingTransformer::create(
                input,
                output,
                MergeSortCommon::try_create(
                    schema.clone(),
                    sort_desc.clone(),
                    order_col_generated,
                    output_order_col,
                    max_memory_usage,
                    spilling_bytes_threshold_per_core,
                    spilling_batch_bytes,
                    MergeSortCommonImpl::create(schema, sort_desc, block_size, enable_loser_tree),
                )?,
            )
        };

        Ok(processor)
    }

    fn build_sort_merge_limit(self) -> Result<Box<dyn Processor>> {
        let Self {
            input,
            output,
            schema,
            block_size,
            sort_desc,
            order_col_generated,
            output_order_col,
            limit,
            spilling_bytes_threshold_per_core,
            spilling_batch_bytes,
            max_memory_usage,
            ..
        } = self;
        let limit = limit.unwrap();

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
                            max_memory_usage,
                            spilling_bytes_threshold_per_core,
                            spilling_batch_bytes,
                            TransformSortMergeLimit::create(block_size, limit),
                        )?,
                    ),
                }),
                DataType::Date => AccumulatingTransformer::create(
                    input,
                    output,
                    MergeSortLimitDate::try_create(
                        schema,
                        sort_desc,
                        order_col_generated,
                        output_order_col,
                        max_memory_usage,
                        spilling_bytes_threshold_per_core,
                        spilling_batch_bytes,
                        MergeSortLimitDateImpl::create(block_size, limit),
                    )?,
                ),
                DataType::Timestamp => AccumulatingTransformer::create(
                    input,
                    output,
                    MergeSortLimitTimestamp::try_create(
                        schema,
                        sort_desc,
                        order_col_generated,
                        output_order_col,
                        max_memory_usage,
                        spilling_bytes_threshold_per_core,
                        spilling_batch_bytes,
                        MergeSortLimitTimestampImpl::create(block_size, limit),
                    )?,
                ),
                DataType::String => AccumulatingTransformer::create(
                    input,
                    output,
                    MergeSortLimitString::try_create(
                        schema,
                        sort_desc,
                        order_col_generated,
                        output_order_col,
                        max_memory_usage,
                        spilling_bytes_threshold_per_core,
                        spilling_batch_bytes,
                        MergeSortLimitStringImpl::create(block_size, limit),
                    )?,
                ),
                _ => AccumulatingTransformer::create(
                    input,
                    output,
                    MergeSortLimitCommon::try_create(
                        schema,
                        sort_desc,
                        order_col_generated,
                        output_order_col,
                        max_memory_usage,
                        spilling_bytes_threshold_per_core,
                        spilling_batch_bytes,
                        MergeSortLimitCommonImpl::create(block_size, limit),
                    )?,
                ),
            }
        } else {
            AccumulatingTransformer::create(
                input,
                output,
                MergeSortLimitCommon::try_create(
                    schema,
                    sort_desc,
                    order_col_generated,
                    output_order_col,
                    max_memory_usage,
                    spilling_bytes_threshold_per_core,
                    spilling_batch_bytes,
                    MergeSortLimitCommonImpl::create(block_size, limit),
                )?,
            )
        };

        Ok(processor)
    }
}
