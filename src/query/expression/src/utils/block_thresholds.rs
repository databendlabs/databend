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
use databend_common_io::constants::DEFAULT_BLOCK_MAX_ROWS;
use databend_common_io::constants::DEFAULT_BLOCK_MIN_ROWS;

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct BlockThresholds {
    pub max_rows_per_block: usize,
    pub min_rows_per_block: usize,
    pub max_bytes_per_block: usize,
}

impl Default for BlockThresholds {
    fn default() -> BlockThresholds {
        BlockThresholds {
            max_rows_per_block: DEFAULT_BLOCK_MAX_ROWS,
            min_rows_per_block: DEFAULT_BLOCK_MIN_ROWS,
            max_bytes_per_block: DEFAULT_BLOCK_BUFFER_SIZE,
        }
    }
}

impl BlockThresholds {
    pub fn new(
        max_rows_per_block: usize,
        min_rows_per_block: usize,
        max_bytes_per_block: usize,
    ) -> Self {
        BlockThresholds {
            max_rows_per_block,
            min_rows_per_block,
            max_bytes_per_block,
        }
    }

    #[inline]
    pub fn check_perfect_block(&self, row_count: usize, block_size: usize) -> bool {
        row_count <= self.max_rows_per_block && self.check_large_enough(row_count, block_size)
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
        block_per_seg: Option<usize>,
    ) -> usize {
        if self.check_for_compact(total_rows, total_bytes) {
            return total_rows;
        }

        let block_num_by_size = std::cmp::max(total_bytes / self.max_bytes_per_block, 1);
        let block_num_by_rows = std::cmp::max(total_rows / self.min_rows_per_block, 1);
        if block_num_by_rows >= block_num_by_size {
            return self.max_rows_per_block;
        }

        let mut rows_per_block = total_rows.div_ceil(block_num_by_size);
        if let Some(block_per_seg) = block_per_seg {
            if block_num_by_size >= block_per_seg {
                return block_num_by_size;
            }
        }

        let max_bytes_per_block = match rows_per_block {
            v if v < self.max_rows_per_block / 10 => {
                // If block rows < 100_000, max_bytes_per_block set to 200M
                2 * self.max_bytes_per_block
            }
            v if v < self.max_rows_per_block / 2 => {
                // If block rows < 500_000, max_bytes_per_block set to 150M
                3 * self.max_bytes_per_block / 2
            }
            v if v < self.min_rows_per_block => {
                // If block rows < 800_000, max_bytes_per_block set to 125M
                5 * self.max_bytes_per_block / 4
            }
            _ => self.max_bytes_per_block,
        };

        if max_bytes_per_block > self.max_bytes_per_block {
            rows_per_block = std::cmp::max(
                total_rows / (std::cmp::max(total_bytes / max_bytes_per_block, 1)),
                1,
            );
        }
        rows_per_block
    }
}
