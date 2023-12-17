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

use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::row::RowConverter as CommonConverter;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;

use super::sort::CommonRows;
use super::sort::Cursor;
use super::sort::DateConverter;
use super::sort::DateRows;
use super::sort::HeapMerger;
use super::sort::Rows;
use super::sort::SortedStream;
use super::sort::StringConverter;
use super::sort::StringRows;
use super::sort::TimestampConverter;
use super::sort::TimestampRows;
use super::transform_sort_merge_base::MergeSort;
use super::transform_sort_merge_base::Status;
use super::transform_sort_merge_base::TransformSortMergeBase;
use super::AccumulatingTransform;
use crate::processors::sort::SortSpillMeta;
use crate::processors::sort::SortSpillMetaWithParams;

/// A spilled block file is at most 8MB.
const SPILL_BATCH_BYTES_SIZE: usize = 8 * 1024 * 1024;

/// Merge sort blocks without limit.
///
/// For merge sort with limit, see [`super::transform_sort_merge_limit`]
pub struct TransformSortMerge<R: Rows> {
    schema: DataSchemaRef,
    sort_desc: Arc<Vec<SortColumnDescription>>,

    block_size: usize,
    buffer: Vec<Option<(DataBlock, Column)>>,

    aborting: Arc<AtomicBool>,
    // The following fields are used for spilling.
    may_spill: bool,
    max_memory_usage: usize,
    spilling_bytes_threshold: usize,
    /// Record current memory usage.
    num_bytes: usize,
    num_rows: usize,

    // The following two fields will be passed to the spill processor.
    // If these two fields are not zero, it means we need to spill.
    /// The number of rows of each spilled block.
    spill_batch_size: usize,
    /// The number of spilled blocks in each merge of the spill processor.
    spill_num_merge: usize,

    _r: PhantomData<R>,
}

impl<R: Rows> TransformSortMerge<R> {
    pub fn create(
        schema: DataSchemaRef,
        sort_desc: Arc<Vec<SortColumnDescription>>,
        block_size: usize,
        max_memory_usage: usize,
        spilling_bytes_threshold: usize,
    ) -> Self {
        let may_spill = max_memory_usage != 0 && spilling_bytes_threshold != 0;
        TransformSortMerge {
            schema,
            sort_desc,
            block_size,
            buffer: vec![],
            aborting: Arc::new(AtomicBool::new(false)),
            may_spill,
            max_memory_usage,
            spilling_bytes_threshold,
            num_bytes: 0,
            num_rows: 0,
            spill_batch_size: 0,
            spill_num_merge: 0,
            _r: PhantomData,
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

        self.num_bytes += block.memory_size();
        self.num_rows += block.num_rows();
        self.buffer.push(Some((block, init_cursor.to_column())));

        if self.may_spill
            && (self.num_bytes >= self.spilling_bytes_threshold
                || GLOBAL_MEM_STAT.get_memory_usage() as usize >= self.max_memory_usage)
        {
            let blocks = self.prepare_spill()?;
            return Ok(Status::Spill(blocks));
        }

        Ok(Status::Continue)
    }

    fn on_finish(&mut self) -> Result<Vec<DataBlock>> {
        if self.spill_num_merge > 0 {
            debug_assert!(self.spill_batch_size > 0);
            // Make the last block as a big memory block.
            self.merge_sort(usize::MAX)
        } else {
            self.merge_sort(self.block_size)
        }
    }

    fn interrupt(&self) {
        self.aborting.store(true, Ordering::Release);
    }
}

impl<R: Rows> TransformSortMerge<R> {
    fn prepare_spill(&mut self) -> Result<Vec<DataBlock>> {
        let mut spill_meta = Box::new(SortSpillMeta {}) as Box<dyn BlockMetaInfo>;
        if self.spill_batch_size == 0 {
            debug_assert_eq!(self.spill_num_merge, 0);
            // We use the first memory calculation to estimate the batch size and the number of merge.
            self.spill_num_merge = self.num_bytes.div_ceil(SPILL_BATCH_BYTES_SIZE).max(2);
            self.spill_batch_size = self.num_rows.div_ceil(self.spill_num_merge);
            // The first block to spill will contain the parameters of spilling.
            // Later blocks just contain a empty struct `SortSpillMeta` to save memory.
            spill_meta = Box::new(SortSpillMetaWithParams {
                batch_size: self.spill_batch_size,
                num_merge: self.spill_num_merge,
            }) as Box<dyn BlockMetaInfo>;
        } else {
            debug_assert!(self.spill_num_merge > 0);
        }

        let mut blocks = self.merge_sort(self.spill_batch_size)?;
        if let Some(b) = blocks.first_mut() {
            b.replace_meta(spill_meta);
        }
        for b in blocks.iter_mut().skip(1) {
            b.replace_meta(Box::new(SortSpillMeta {}));
        }

        self.num_rows = 0;
        self.num_bytes = 0;
        self.buffer.clear();

        Ok(blocks)
    }

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

        let streams = self.buffer.drain(..).collect::<Vec<_>>();
        let mut result = Vec::with_capacity(size_hint);
        let mut merger = HeapMerger::<R, BlockStream>::create(
            self.schema.clone(),
            streams,
            self.sort_desc.clone(),
            batch_size,
            None,
        );

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

pub(super) type MergeSortDateImpl = TransformSortMerge<DateRows>;
pub(super) type MergeSortDate = TransformSortMergeBase<MergeSortDateImpl, DateRows, DateConverter>;

pub(super) type MergeSortTimestampImpl = TransformSortMerge<TimestampRows>;
pub(super) type MergeSortTimestamp =
    TransformSortMergeBase<MergeSortTimestampImpl, TimestampRows, TimestampConverter>;

pub(super) type MergeSortStringImpl = TransformSortMerge<StringRows>;
pub(super) type MergeSortString =
    TransformSortMergeBase<MergeSortStringImpl, StringRows, StringConverter>;

pub(super) type MergeSortCommonImpl = TransformSortMerge<CommonRows>;
pub(super) type MergeSortCommon =
    TransformSortMergeBase<MergeSortCommonImpl, CommonRows, CommonConverter>;

pub fn sort_merge(
    schema: DataSchemaRef,
    block_size: usize,
    sort_desc: Vec<SortColumnDescription>,
    data_blocks: Vec<DataBlock>,
) -> Result<Vec<DataBlock>> {
    let sort_desc = Arc::new(sort_desc);
    let mut processor = MergeSortCommon::try_create(
        schema.clone(),
        sort_desc.clone(),
        false,
        false,
        MergeSortCommonImpl::create(schema, sort_desc, block_size, 0, 0),
    )?;
    for block in data_blocks {
        processor.transform(block)?;
    }
    processor.on_finish(true)
}
