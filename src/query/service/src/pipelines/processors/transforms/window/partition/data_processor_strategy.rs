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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_transforms::sorts::SortedBlockIterator;
use databend_common_pipeline_transforms::sorts::sort_merge;
use databend_common_pipeline_transforms::sorts::sort_merge_sources;
use databend_common_settings::Settings;

/// Sorts the data of one window partition: runs are sorted when they are
/// spilled, and a restored partition merges its runs. Output is pulled one
/// block at a time, so only the current block of every run is held.
pub trait DataProcessorStrategy: Send + Sync + 'static {
    const NAME: &'static str;
    /// Sort blocks into one run, emitted as sorted pages. `spilled_bytes` is
    /// how much of the partition has been spilled before this run.
    fn sort_run(
        &self,
        data_blocks: Vec<DataBlock>,
        spilled_bytes: usize,
    ) -> Result<SortedBlockIterator>;
    /// Merge the sorted runs of one partition.
    fn merge_runs(&self, runs: Vec<SortedBlockIterator>) -> Result<SortedBlockIterator>;
}

pub struct CompactStrategy {
    max_bytes_per_block: usize,
    max_rows_per_block: usize,
}

impl CompactStrategy {
    pub fn new(max_rows_per_block: usize, max_bytes_per_block: usize) -> Self {
        Self {
            max_bytes_per_block,
            max_rows_per_block,
        }
    }

    fn concat_blocks(blocks: Vec<DataBlock>) -> Result<DataBlock> {
        DataBlock::concat(&blocks)
    }

    fn check_large_enough(&self, rows: usize, bytes: usize) -> bool {
        rows >= self.max_rows_per_block || bytes >= self.max_bytes_per_block
    }
}

impl DataProcessorStrategy for CompactStrategy {
    const NAME: &'static str = "Compact";

    fn sort_run(
        &self,
        data_blocks: Vec<DataBlock>,
        _spilled_bytes: usize,
    ) -> Result<SortedBlockIterator> {
        let blocks_num = data_blocks.len();
        if blocks_num < 2 {
            return Ok(Box::new(data_blocks.into_iter().map(Ok)));
        }

        let mut accumulated_rows = 0;
        let mut accumulated_bytes = 0;
        let mut pending_blocks = Vec::with_capacity(blocks_num);
        let mut staged_blocks = Vec::with_capacity(blocks_num);
        let mut result = Vec::with_capacity(blocks_num);
        for block in data_blocks {
            accumulated_rows += block.num_rows();
            accumulated_bytes += block.estimate_block_size(block.num_columns());
            pending_blocks.push(block);
            if !self.check_large_enough(accumulated_rows, accumulated_bytes) {
                continue;
            }
            if !staged_blocks.is_empty() {
                result.push(Self::concat_blocks(std::mem::take(&mut staged_blocks))?);
            }
            std::mem::swap(&mut staged_blocks, &mut pending_blocks);
            accumulated_rows = 0;
            accumulated_bytes = 0;
        }

        staged_blocks.append(&mut pending_blocks);
        if !staged_blocks.is_empty() {
            result.push(Self::concat_blocks(std::mem::take(&mut staged_blocks))?);
        }

        Ok(Box::new(result.into_iter().map(Ok)))
    }

    fn merge_runs(&self, runs: Vec<SortedBlockIterator>) -> Result<SortedBlockIterator> {
        // Compaction keeps no order across runs: emit them one after another.
        Ok(Box::new(runs.into_iter().flatten()))
    }
}

/// A partition that spills at most this much has only a few runs. They are
/// kept in pages of PAGE_BYTES, so the partition is read back in a few large
/// reads instead of waiting on many small ones.
const SMALL_PARTITION_BYTES: usize = 64 * 1024 * 1024;
/// Runs of a larger partition are split into about this many pages, so
/// restoring it holds about 1/PAGES_PER_RUN of each run instead of the whole
/// run. It also bounds the pages, and so the reads waited on, per run.
const PAGES_PER_RUN: usize = 16;
/// Lower bound of a sorted page. It stays above the spill reader's range merge
/// size (512 KiB), so pages of small runs do not turn into many tiny reads.
const PAGE_MIN_BYTES: usize = 1024 * 1024;
/// Upper bound of a sorted page, so a page of wide rows stays small when it is
/// read back next to the current page of every other run.
const PAGE_BYTES: usize = 8 * 1024 * 1024;

pub struct SortStrategy {
    sort_desc: Vec<SortColumnDescription>,
    schema: DataSchemaRef,
    max_block_size: usize,
    enable_fixed_rows: bool,
    enable_loser_tree: bool,
}

impl SortStrategy {
    pub fn try_create(
        settings: &Settings,
        sort_desc: Vec<SortColumnDescription>,
        schema: DataSchemaRef,
    ) -> Result<Self> {
        Ok(Self {
            sort_desc,
            schema,
            max_block_size: settings.get_max_block_size()? as usize,
            enable_fixed_rows: settings.get_enable_fixed_rows_sort()?,
            enable_loser_tree: settings.get_enable_loser_tree_merge_sort()?,
        })
    }
}

impl DataProcessorStrategy for SortStrategy {
    const NAME: &'static str = "Sort";

    /// Each page of a spilled run is one row group, and restoring a partition
    /// holds the current page of every run plus about two prefetched ones. So
    /// the page size decides restore memory, and the page count decides how
    /// many reads each run waits on:
    ///
    /// - A partition that has spilled at most SMALL_PARTITION_BYTES, this run
    ///   included, has only a few runs, as each spill is at least 10 MiB. Its
    ///   pages stay at PAGE_BYTES: it is small enough to read back whole, and
    ///   too few runs are merged to hide the latency of many small reads.
    /// - Runs of a larger partition are split into about PAGES_PER_RUN pages,
    ///   within [PAGE_MIN_BYTES, PAGE_BYTES], so each run holds about
    ///   1/PAGES_PER_RUN of itself. Such a partition has many runs, so a read
    ///   of one run overlaps with merging the others.
    ///
    /// A page also holds at most max_block_size rows.
    fn sort_run(
        &self,
        data_blocks: Vec<DataBlock>,
        spilled_bytes: usize,
    ) -> Result<SortedBlockIterator> {
        // Size pages as they are written: constant columns are expanded to
        // full columns there, though memory_size() counts them once.
        let (rows, bytes) = data_blocks.iter().fold((0, 0), |(rows, bytes), block| {
            let block_bytes = block.estimate_block_size(block.num_columns());
            (rows + block.num_rows(), bytes + block_bytes)
        });
        let page_bytes = if spilled_bytes + bytes <= SMALL_PARTITION_BYTES {
            PAGE_BYTES
        } else {
            (bytes / PAGES_PER_RUN).clamp(PAGE_MIN_BYTES, PAGE_BYTES)
        };
        // Turn the page size into rows by the average row width of this run.
        let page_rows = match bytes.checked_div(rows) {
            Some(row_bytes) if row_bytes > 0 => (page_bytes / row_bytes).max(1),
            _ => self.max_block_size,
        }
        .min(self.max_block_size);
        let data_blocks = data_blocks
            .into_iter()
            .map(|data_block| DataBlock::sort(&data_block, &self.sort_desc, None))
            .collect::<Result<Vec<_>>>()?;

        sort_merge(
            self.schema.clone(),
            page_rows,
            self.sort_desc.clone(),
            data_blocks,
            self.enable_fixed_rows,
            self.enable_loser_tree,
        )
    }

    fn merge_runs(&self, runs: Vec<SortedBlockIterator>) -> Result<SortedBlockIterator> {
        sort_merge_sources(
            self.schema.clone(),
            self.max_block_size,
            self.sort_desc.clone(),
            runs,
            self.enable_fixed_rows,
            self.enable_loser_tree,
        )
    }
}
