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

use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use databend_common_io::constants::DEFAULT_BLOCK_COMPRESSED_SIZE;
use databend_common_io::constants::DEFAULT_BLOCK_PER_SEGMENT;
use databend_common_io::constants::DEFAULT_BLOCK_ROW_COUNT;

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct BlockThresholds {
    pub max_rows_per_block: usize,
    pub min_rows_per_block: usize,

    pub max_bytes_per_block: usize,
    pub min_bytes_per_block: usize,

    pub max_compressed_per_block: usize,
    pub min_compressed_per_block: usize,

    pub block_per_segment: usize,
}

impl Default for BlockThresholds {
    fn default() -> BlockThresholds {
        BlockThresholds {
            max_rows_per_block: DEFAULT_BLOCK_ROW_COUNT,
            min_rows_per_block: (DEFAULT_BLOCK_ROW_COUNT * 4).div_ceil(5),
            max_bytes_per_block: DEFAULT_BLOCK_BUFFER_SIZE * 2,
            min_bytes_per_block: (DEFAULT_BLOCK_BUFFER_SIZE * 4).div_ceil(5),
            max_compressed_per_block: DEFAULT_BLOCK_COMPRESSED_SIZE,
            min_compressed_per_block: (DEFAULT_BLOCK_COMPRESSED_SIZE * 4).div_ceil(5),
            block_per_segment: DEFAULT_BLOCK_PER_SEGMENT,
        }
    }
}

impl BlockThresholds {
    pub fn new(
        max_rows_per_block: usize,
        bytes_per_block: usize,
        max_compressed_per_block: usize,
        block_per_segment: usize,
    ) -> Self {
        BlockThresholds {
            max_rows_per_block,
            min_rows_per_block: (max_rows_per_block * 4).div_ceil(5),
            max_bytes_per_block: bytes_per_block * 2,
            min_bytes_per_block: (bytes_per_block * 4).div_ceil(5),
            max_compressed_per_block,
            min_compressed_per_block: (max_compressed_per_block * 4).div_ceil(5),
            block_per_segment,
        }
    }

    #[inline]
    pub fn check_perfect_block(
        &self,
        row_count: usize,
        block_size: usize,
        file_size: usize,
    ) -> bool {
        row_count >= self.min_rows_per_block
            || block_size >= self.min_bytes_per_block
            || file_size >= self.min_compressed_per_block
    }

    #[inline]
    pub fn check_perfect_segment(
        &self,
        total_blocks: usize,
        total_rows: usize,
        total_bytes: usize,
        total_compressed: usize,
    ) -> bool {
        total_blocks >= self.block_per_segment
            && (total_rows >= self.min_rows_per_block * self.block_per_segment
                || total_bytes >= self.min_bytes_per_block * self.block_per_segment
                || total_compressed >= self.min_compressed_per_block * self.block_per_segment)
    }

    #[inline]
    pub fn check_large_enough(&self, row_count: usize, block_size: usize) -> bool {
        row_count >= self.min_rows_per_block || block_size >= self.min_bytes_per_block
    }

    #[inline]
    pub fn check_for_compact(&self, row_count: usize, block_size: usize) -> bool {
        row_count < 2 * self.min_rows_per_block && block_size < 2 * self.min_bytes_per_block
    }

    #[inline]
    pub fn check_too_small(&self, row_count: usize, block_size: usize, file_size: usize) -> bool {
        row_count < self.min_rows_per_block / 2
            && block_size < self.min_bytes_per_block / 2
            && file_size < self.min_compressed_per_block / 2
    }

    #[inline]
    pub fn calc_rows_for_compact(&self, total_bytes: usize, total_rows: usize) -> usize {
        if self.check_for_compact(total_rows, total_bytes) {
            return total_rows;
        }

        let block_num_by_rows = std::cmp::max(total_rows / self.min_rows_per_block, 1);
        let block_num_by_size = total_bytes / self.min_bytes_per_block;
        if block_num_by_rows >= block_num_by_size {
            return self.max_rows_per_block;
        }
        total_rows.div_ceil(block_num_by_size)
    }

    /// Calculates the optimal number of rows per block based on total data size and row count.
    ///
    /// # Parameters
    /// - `total_bytes`: The total size of the data in bytes.
    /// - `total_rows`: The total number of rows in the data.
    /// - `total_compressed`: The total compressed size of the data in bytes.
    ///
    /// # Returns
    /// - The calculated number of rows per block that satisfies the thresholds.
    #[inline]
    pub fn calc_rows_for_recluster(
        &self,
        total_rows: usize,
        total_bytes: usize,
        total_compressed: usize,
    ) -> usize {
        // Check if the data is compact enough to skip further calculations.
        if self.check_for_compact(total_rows, total_bytes)
            && total_compressed < 2 * self.min_compressed_per_block
        {
            return total_rows;
        }

        let block_num_by_rows = std::cmp::max(total_rows / self.min_rows_per_block, 1);
        let block_num_by_compressed = total_compressed.div_ceil(self.max_compressed_per_block);
        // If row-based block count exceeds compressed-based block count, use max rows per block.
        if block_num_by_rows >= block_num_by_compressed {
            return self.max_rows_per_block;
        }

        let bytes_per_block = total_bytes.div_ceil(block_num_by_compressed);
        // Adjust the number of blocks based on block size thresholds.
        let max_bytes_per_block = self.max_bytes_per_block.min(400 * 1024 * 1024);
        let min_bytes_per_block = (self.min_bytes_per_block / 2).min(50 * 1024 * 1024);
        let block_nums = if bytes_per_block > max_bytes_per_block {
            // Case 1: If the block size is too bigger.
            total_bytes.div_ceil(max_bytes_per_block)
        } else if bytes_per_block < min_bytes_per_block {
            // Case 2: If the block size is too smaller.
            total_bytes / min_bytes_per_block
        } else {
            // Case 3: Otherwise, use the compressed-based block count.
            block_num_by_compressed
        };
        total_rows.div_ceil(block_nums.max(1)).max(1)
    }
}
