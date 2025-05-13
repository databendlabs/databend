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

use crate::pipelines::processors::transforms::DataProcessorStrategy;

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

    fn process_data_blocks(&self, data_blocks: Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        let blocks_num = data_blocks.len();
        if blocks_num < 2 {
            return Ok(data_blocks);
        }

        let mut accumulated_rows = 0;
        let mut accumulated_bytes = 0;
        let mut pending_blocks = Vec::with_capacity(blocks_num);
        let mut staged_blocks = Vec::with_capacity(blocks_num);
        let mut result = Vec::with_capacity(blocks_num);
        for block in data_blocks {
            accumulated_rows += block.num_rows();
            accumulated_bytes += block.estimate_block_size();
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

        Ok(result)
    }
}
