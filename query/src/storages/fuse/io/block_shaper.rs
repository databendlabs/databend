//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use common_datablocks::DataBlock;
use common_exception::Result;

pub struct BlockShaper {
    /// Max number of rows per data block
    max_row_per_block: usize,
    /// Number of rows accumulate in `accumulated_blocks`.
    accumulated_rows: usize,
    /// Small data blocks accumulated
    ///
    /// Invariant: accumulated_blocks.iter().map(|item| item.num_rows()).sum() < max_row_per_block
    accumulated_blocks: Vec<DataBlock>,
}

impl BlockShaper {
    pub fn new(max_row_per_block: usize) -> Self {
        Self {
            max_row_per_block,
            accumulated_rows: 0,
            accumulated_blocks: Vec::new(),
        }
    }

    /// Merge or split block according to the configurations
    pub fn shape(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        let num_rows = block.num_rows();

        // For cases like stmt `insert into .. select ... from ...`, the blocks that feeded
        // are likely to be properly sized, i.e. exeactly `max_row_per_block` rows per block,
        // In that cases, just return them.
        if num_rows == self.max_row_per_block {
            return Ok(vec![block]);
        }

        if num_rows + self.accumulated_rows < self.max_row_per_block {
            self.accumulated_rows += num_rows;
            self.accumulated_blocks.push(block);
            Ok(vec![])
        } else {
            let mut blocks = std::mem::take(&mut self.accumulated_blocks);
            blocks.push(block);
            let merged = DataBlock::concat_blocks(&blocks)?;
            let blocks = DataBlock::split_block_by_size(&merged, self.max_row_per_block)?;

            // NOTE: if the `blocks` returned by `split_block_by_size` is guaranteed to be
            // well-partitioned, then the current impl could be optimized further.
            let (result, remains) = blocks
                .into_iter()
                .partition(|item| item.num_rows() >= self.max_row_per_block);
            self.reset(remains);
            Ok(result)
        }
    }

    pub fn seal(self) -> Result<Option<DataBlock>> {
        // TODO DOC
        let remains = self.accumulated_blocks;
        Ok(if remains.is_empty() {
            None
        } else {
            Some(DataBlock::concat_blocks(&remains)?)
        })
    }

    fn reset(&mut self, remains: Vec<DataBlock>) {
        self.accumulated_rows = remains.iter().map(|item| item.num_rows()).sum();
        self.accumulated_blocks = remains;
    }
}
