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

use std::hint::unlikely;
use std::marker::PhantomData;
use std::sync::Arc;

use bytesize::ByteSize;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline::core::check_interrupt;

use super::core::Merger;
use super::core::RowConverter;
use super::core::Rows;
use super::core::RowsTypeVisitor;
use super::core::SortKeyDescription;
use super::core::SortedStream;
use super::core::algorithm::HeapSort;
use super::core::algorithm::LoserTreeSort;
use super::core::algorithm::SortAlgorithm;
use super::core::select_row_type;
use super::sort_merge_base::MergeSort;

/// Merge sort blocks without limit.
///
/// For merge sort with limit, see [`super::transform_sort_merge_limit`]
pub struct TransformSortMerge<R: Rows> {
    enable_loser_tree: bool,
    limit: Option<usize>,
    block_size: usize,
    buffer: Vec<Option<(DataBlock, Column)>>,

    /// Record current memory usage.
    num_bytes: ByteSize,
    num_rows: usize,

    _r: PhantomData<R>,
}

impl<R: Rows> TransformSortMerge<R> {
    pub fn create(block_size: usize, enable_loser_tree: bool, limit: Option<usize>) -> Self {
        TransformSortMerge {
            enable_loser_tree,
            limit,
            block_size,
            buffer: vec![],
            num_bytes: ByteSize(0),
            num_rows: 0,
            _r: PhantomData,
        }
    }
}

impl<R: Rows> MergeSort<R> for TransformSortMerge<R> {
    const NAME: &'static str = "TransformSortMerge";

    fn add_block(&mut self, block: DataBlock, init_rows: R) -> Result<()> {
        check_interrupt()?;
        if unlikely(block.is_empty()) {
            return Ok(());
        }

        self.num_bytes += block.memory_size() as u64;
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

    #[inline(always)]
    fn num_bytes(&self) -> ByteSize {
        self.num_bytes
    }

    #[inline(always)]
    fn num_rows(&self) -> usize {
        self.num_rows
    }

    fn prepare_spill(&mut self, spill_batch_size: usize) -> Result<Vec<DataBlock>> {
        let blocks = self.merge_sort(spill_batch_size)?;

        self.num_rows = 0;
        self.num_bytes = ByteSize(0);

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
        let streams = std::mem::take(&mut self.buffer);
        let mut result = Vec::with_capacity(size_hint);

        let mut merger = Merger::<A, _>::new(streams, batch_size, self.limit);

        while let Some(block) = merger.next_block()? {
            check_interrupt()?;
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

/// Sorted blocks pulled one at a time: a merge output, or a sorted run.
pub type SortedBlockIterator = Box<dyn Iterator<Item = Result<DataBlock>> + Send>;

/// Merge sorted blocks into blocks of at most `block_size` rows, pulled one at
/// a time by the caller. The merger owns the input blocks and drops each one
/// after its last row is emitted, so the merged output does not have to be
/// held next to the whole input.
pub fn sort_merge(
    schema: DataSchemaRef,
    block_size: usize,
    sort_desc: Vec<SortColumnDescription>,
    data_blocks: Vec<DataBlock>,
    enable_fixed_rows: bool,
    enable_loser_tree: bool,
) -> Result<SortedBlockIterator> {
    let block_size = block_size.max(1);
    let sources = data_blocks
        .into_iter()
        .filter(|block| !block.is_empty())
        .map(|block| {
            let rows = block.num_rows();
            Box::new(
                (0..rows)
                    .step_by(block_size)
                    .map(move |start| Ok(block.slice(start..(start + block_size).min(rows)))),
            ) as SortedBlockIterator
        })
        .collect();
    sort_merge_sources(
        schema,
        block_size,
        sort_desc,
        sources,
        enable_fixed_rows,
        enable_loser_tree,
    )
}

/// Merges sources that each emit sorted blocks in order, such as runs read
/// back from spill. Only the current block of every source is held.
pub fn sort_merge_sources(
    schema: DataSchemaRef,
    block_size: usize,
    sort_desc: Vec<SortColumnDescription>,
    mut sources: Vec<SortedBlockIterator>,
    enable_fixed_rows: bool,
    enable_loser_tree: bool,
) -> Result<SortedBlockIterator> {
    match sources.len() {
        0 => return Ok(Box::new(std::iter::empty())),
        1 => return Ok(sources.pop().unwrap()),
        _ => {}
    }
    let sort_desc: Arc<[_]> = sort_desc.into();
    let key_desc = SortKeyDescription::new(sort_desc, schema, enable_fixed_rows)?;
    let mut visitor = IncrementalMergeVisitor {
        key_desc,
        block_size,
        enable_loser_tree,
        sources,
    };
    select_row_type(&mut visitor, enable_fixed_rows)
}

fn merged_blocks<A, S>(mut merger: Merger<A, S>) -> SortedBlockIterator
where
    A: SortAlgorithm + 'static,
    S: SortedStream + Send + 'static,
{
    Box::new(std::iter::from_fn(move || {
        check_interrupt()
            .and_then(|_| merger.next_block())
            .transpose()
    }))
}

/// Encodes the sort keys of each block as it is pulled from its source.
struct SourceStream<R: Rows> {
    source: SortedBlockIterator,
    converter: R::Converter,
}

impl<R: Rows> SortedStream for SourceStream<R> {
    fn next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        while let Some(block) = self.source.next().transpose()? {
            if !block.is_empty() {
                let rows = self.converter.convert(&block)?;
                return Ok((Some((block, rows.to_column())), false));
            }
        }
        Ok((None, false))
    }
}

struct IncrementalMergeVisitor {
    key_desc: SortKeyDescription,
    block_size: usize,
    enable_loser_tree: bool,
    sources: Vec<SortedBlockIterator>,
}

impl RowsTypeVisitor for IncrementalMergeVisitor {
    type Result = Result<SortedBlockIterator>;

    fn sort_key_desc(&self) -> SortKeyDescription {
        self.key_desc.clone()
    }

    fn visit_type<R>(&mut self) -> Self::Result
    where
        R: Rows + 'static,
        R::Converter: Send + 'static,
    {
        let streams = std::mem::take(&mut self.sources)
            .into_iter()
            .map(|source| {
                Ok(SourceStream::<R> {
                    source,
                    converter: <R as Rows>::Converter::new(self.key_desc.clone())?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(if self.enable_loser_tree {
            merged_blocks(Merger::<LoserTreeSort<R>, _>::new(
                streams,
                self.block_size,
                None,
            ))
        } else {
            merged_blocks(Merger::<HeapSort<R>, _>::new(
                streams,
                self.block_size,
                None,
            ))
        })
    }
}
