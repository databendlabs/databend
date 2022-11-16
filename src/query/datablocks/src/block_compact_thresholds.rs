// Copyright 2022 Datafuse Labs.
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

#[derive(Clone, Copy, Debug)]
pub struct BlockCompactThresholds {
    pub max_rows_per_block: usize,
    pub min_rows_per_block: usize,
    pub max_bytes_per_block: usize,
}

impl Default for BlockCompactThresholds {
    fn default() -> Self {
        Self {
            // DEFAULT_ROW_PER_BLOCK
            max_rows_per_block: 1000 * 1000,
            // 0.8 * DEFAULT_ROW_PER_BLOCK
            min_rows_per_block: 800 * 1000,
            // DEFAULT_BLOCK_SIZE_IN_MEM_SIZE_THRESHOLD
            max_bytes_per_block: 100 * 1024 * 1024,
        }
    }
}

impl BlockCompactThresholds {
    pub fn new(
        max_rows_per_block: usize,
        min_rows_per_block: usize,
        max_bytes_per_block: usize,
    ) -> Self {
        BlockCompactThresholds {
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
    pub fn check_for_recluster(&self, total_rows: usize, total_bytes: usize) -> bool {
        total_rows <= self.min_rows_per_block && total_bytes <= self.max_bytes_per_block
    }
}
