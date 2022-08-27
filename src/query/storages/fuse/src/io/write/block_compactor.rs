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

use common_pipeline_transforms::processors::transforms::BlockCompactor as Compactor;

#[derive(Clone, Default)]
pub struct BlockCompactor {
    max_rows_per_block: usize,
    min_rows_per_block: usize,
    max_bytes_per_block: usize,
}

impl BlockCompactor {
    pub fn new(
        max_rows_per_block: usize,
        min_rows_per_block: usize,
        max_bytes_per_block: usize,
    ) -> Self {
        BlockCompactor {
            max_rows_per_block,
            min_rows_per_block,
            max_bytes_per_block,
        }
    }

    pub fn check_perfect_block(&self, row_count: usize, block_size: usize) -> bool {
        if (row_count >= self.min_rows_per_block && row_count <= self.max_rows_per_block)
            || block_size >= self.max_bytes_per_block
        {
            return true;
        }
        false
    }

    pub fn check_for_recluster(&self, total_rows: usize, total_bytes: usize) -> bool {
        if total_rows <= self.min_rows_per_block && total_bytes <= self.max_bytes_per_block {
            return true;
        }
        false
    }

    pub fn to_compactor(&self) -> Compactor {
        Compactor::new(
            self.max_rows_per_block,
            self.min_rows_per_block,
            self.max_bytes_per_block,
        )
    }
}
