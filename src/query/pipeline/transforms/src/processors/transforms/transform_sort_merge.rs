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

use std::intrinsics::unlikely;
use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::row::RowConverter as CommonConverter;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;

use super::sort::algorithm::HeapSort;
use super::sort::algorithm::LoserTreeSort;
use super::sort::algorithm::SortAlgorithm;
use super::sort::CommonRows;
use super::sort::Rows;
use super::sort::SortedStream;
use super::transform_sort_merge_base::MergeSort;
use super::transform_sort_merge_base::TransformSortMergeBase;
use super::AccumulatingTransform;
use crate::processors::sort::Merger;
use crate::MemorySettings;

/// Merge sort blocks without limit.
///
/// For merge sort with limit, see [`super::transform_sort_merge_limit`]
pub struct TransformSortMerge<R: Rows> {
    schema: DataSchemaRef,
    enable_loser_tree: bool,
    limit: Option<usize>,
    block_size: usize,
    buffer: Vec<Option<(DataBlock, Column)>>,

    aborting: Arc<AtomicBool>,

    /// Record current memory usage.
    num_bytes: usize,
    num_rows: usize,

    _r: PhantomData<R>,
}

impl<R: Rows> TransformSortMerge<R> {
    pub fn create(
        schema: DataSchemaRef,
        _sort_desc: Arc<Vec<SortColumnDescription>>,
        block_size: usize,
        enable_loser_tree: bool,
        limit: Option<usize>,
    ) -> Self {
        TransformSortMerge {
            schema,
            enable_loser_tree,
            limit,
            block_size,
            buffer: vec![],
            aborting: Arc::new(AtomicBool::new(false)),
            num_bytes: 0,
            num_rows: 0,
            _r: PhantomData,
        }
    }
}

impl<R: Rows> MergeSort<R> for TransformSortMerge<R> {
    const NAME: &'static str = "TransformSortMerge";

    fn add_block(&mut self, block: DataBlock, init_rows: R, _input_index: usize) -> Result<()> {
        if unlikely(self.aborting.load(Ordering::Relaxed)) {
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the server is shutting down or the query was killed.",
            ));
        }

        if unlikely(block.is_empty()) {
            return Ok(());
        }

        self.num_bytes += block.memory_size();
        self.num_rows += block.num_rows();
        self.buffer.push(Some((block, init_rows.to_column())));

        Ok(())
    }

    fn on_finish(&mut self, all_in_one_block: bool) -> Result<Vec<DataBlock>> {
        if all_in_one_block {
            self.merge_sort(self.num_rows)
        } else {
            self.merge_sort(self.block_size)
        }
    }

    fn interrupt(&self) {
        self.aborting.store(true, Ordering::Release);
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
        let blocks = self.merge_sort(spill_batch_size)?;

        self.num_rows = 0;
        self.num_bytes = 0;

        debug_assert!(self.buffer.is_empty());

        Ok(blocks)
    }
}

impl<R: Rows> TransformSortMerge<R> {
    fn merge_sort(&mut self, batch_size: usize) -> Result<Vec<DataBlock>> {
        if self.buffer.is_empty() {
            return Ok(vec![]);
        }

        let size_hint = self.num_rows.div_ceil(batch_size);

        if self.buffer.len() == 1 {
            // If there is only one block, we don't need to merge.
            let (block, _) = self.buffer.pop().unwrap().unwrap();
            let num_rows = block.num_rows();
            if size_hint == 1 {
                return Ok(vec![block]);
            }
            let mut result = Vec::with_capacity(size_hint);
            for i in 0..size_hint {
                let start = i * batch_size;
                let end = ((i + 1) * batch_size).min(num_rows);
                let block = block.slice(start..end);
                result.push(block);
            }
            return Ok(result);
        }

        if self.enable_loser_tree {
            self.merge_sort_algo::<LoserTreeSort<R>>(batch_size, size_hint)
        } else {
            self.merge_sort_algo::<HeapSort<R>>(batch_size, size_hint)
        }
    }

    fn merge_sort_algo<A: SortAlgorithm>(
        &mut self,
        batch_size: usize,
        size_hint: usize,
    ) -> Result<Vec<DataBlock>> {
        let streams = self.buffer.drain(..).collect::<Vec<BlockStream>>();
        let mut result = Vec::with_capacity(size_hint);

        let mut merger =
            Merger::<A, _>::create(self.schema.clone(), streams, batch_size, self.limit);

        while let Some(block) = merger.next_block()? {
            if unlikely(self.aborting.load(Ordering::Relaxed)) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }
            result.push(block);
        }

        debug_assert!(merger.is_finished());

        Ok(result)
    }
}

type BlockStream = Option<(DataBlock, Column)>;

impl SortedStream for BlockStream {
    fn next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        Ok((self.take(), false))
    }
}

pub(super) type MergeSortCommonImpl = TransformSortMerge<CommonRows>;
pub(super) type MergeSortCommon =
    TransformSortMergeBase<MergeSortCommonImpl, CommonRows, CommonConverter>;

pub fn sort_merge(
    schema: DataSchemaRef,
    block_size: usize,
    sort_desc: Vec<SortColumnDescription>,
    data_blocks: Vec<DataBlock>,
    sort_spilling_batch_bytes: usize,
    enable_loser_tree: bool,
    have_order_col: bool,
) -> Result<Vec<DataBlock>> {
    let sort_desc = Arc::new(sort_desc);
    let mut memory_settings = MemorySettings::disable_spill();
    memory_settings.spill_unit_size = sort_spilling_batch_bytes;

    let mut processor = MergeSortCommon::try_create(
        schema.clone(),
        sort_desc.clone(),
        have_order_col,
        false,
        memory_settings,
        MergeSortCommonImpl::create(schema, sort_desc, block_size, enable_loser_tree, None),
    )?;
    for block in data_blocks {
        processor.transform(block)?;
    }

    processor.on_finish(true)
}
