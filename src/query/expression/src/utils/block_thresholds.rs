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
use databend_common_io::constants::DEFAULT_BLOCK_MAX_ROWS;
use databend_common_io::constants::DEFAULT_BLOCK_MIN_ROWS;
use databend_common_io::constants::DEFAULT_BLOCK_PER_SEGMENT;

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct BlockThresholds {
    pub max_rows_per_block: usize,
    pub min_rows_per_block: usize,
    pub max_bytes_per_block: usize,
    pub max_bytes_per_file: usize,
    pub block_per_segment: usize,
}

impl Default for BlockThresholds {
    fn default() -> BlockThresholds {
        BlockThresholds {
            max_rows_per_block: DEFAULT_BLOCK_MAX_ROWS,
            min_rows_per_block: DEFAULT_BLOCK_MIN_ROWS,
            max_bytes_per_block: DEFAULT_BLOCK_BUFFER_SIZE,
            max_bytes_per_file: DEFAULT_BLOCK_COMPRESSED_SIZE,
            block_per_segment: DEFAULT_BLOCK_PER_SEGMENT,
        }
    }
}

impl BlockThresholds {
    pub fn new(
        max_rows_per_block: usize,
        min_rows_per_block: usize,
        max_bytes_per_block: usize,
        max_bytes_per_file: usize,
        block_per_segment: usize,
    ) -> Self {
        BlockThresholds {
            max_rows_per_block,
            min_rows_per_block,
            max_bytes_per_block,
            max_bytes_per_file,
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
            || block_size >= self.max_bytes_per_block
            || file_size >= self.max_bytes_per_file
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
                || total_bytes >= self.max_bytes_per_block * self.block_per_segment
                || total_compressed >= self.max_bytes_per_file * self.block_per_segment)
    }

    #[inline]
    pub fn check_large_enough(&self, row_count: usize, block_size: usize) -> bool {
        row_count >= self.min_rows_per_block || block_size >= self.max_bytes_per_block
    }

    #[inline]
    pub fn check_for_compact(&self, row_count: usize, block_size: usize) -> bool {
        row_count < 2 * self.min_rows_per_block && block_size < 2 * self.max_bytes_per_block
    }

    #[inline]
    pub fn check_too_small(&self, row_count: usize, block_size: usize) -> bool {
        row_count < self.min_rows_per_block / 2 && block_size < self.max_bytes_per_block / 2
    }

    #[inline]
    pub fn calc_rows_per_block(
        &self,
        total_bytes: usize,
        total_rows: usize,
        total_compressed: usize,
    ) -> usize {
        if self.check_for_compact(total_rows, total_bytes) {
            return total_rows;
        }

        let block_num_by_rows = std::cmp::max(total_rows / self.min_rows_per_block, 1);
        let block_num_by_size = std::cmp::max(
            total_bytes / self.max_bytes_per_block,
            total_compressed / self.max_bytes_per_file,
        );
        if block_num_by_rows >= block_num_by_size {
            return self.max_rows_per_block;
        }

        let mut rows_per_block = total_rows.div_ceil(block_num_by_size);
        if rows_per_block < self.max_rows_per_block / 2 {
            // If block rows < 500_000, max_bytes_per_block set to 125M
            let block_num_by_size = (4 * block_num_by_size / 5).max(1);
            rows_per_block = total_rows.div_ceil(block_num_by_size);
        }
        rows_per_block
    }
}
