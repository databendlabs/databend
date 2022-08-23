// Copyright 2021 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_exception::Result;

/// Takes elements of type S in, and spills elements of type T.
#[async_trait::async_trait]
pub trait Compactor<S, T> {
    /// Takes an element s of type S, convert it into [Some<T>] if possible;
    /// otherwise, returns [None]
    ///
    /// for example. given a DataBlock s, a setting of `max_rows_per_block`
    ///    - Some<Vec<DataBlock>> might be returned if s contains more rows than `max_rows_per_block`
    ///       in this case, s will been split into vector of (smaller) blocks
    ///    - or [None] might be returned if s is too small
    ///       in this case, s will be accumulated
    async fn compact(&mut self, s: S) -> Result<Option<T>>;

    /// Indicate that no more elements remains.
    ///
    /// Spills [Some<T>] if there were, otherwise [None]
    fn finish(self) -> Result<Option<T>>;
}

pub struct BlockCompactor {
    // TODO threshold of block size
    /// Max number of rows per data block
    max_rows_per_block: usize,
    /// Number of rows accumulate in `accumulated_blocks`.
    accumulated_rows: usize,
    /// Small data blocks accumulated
    ///
    /// Invariant: accumulated_blocks.iter().map(|item| item.num_rows()).sum() < max_rows_per_block
    accumulated_blocks: Vec<DataBlock>,
}

impl BlockCompactor {
    pub fn new(max_rows_per_block: usize) -> Self {
        Self {
            max_rows_per_block,
            accumulated_rows: 0,
            accumulated_blocks: Vec::new(),
        }
    }
    fn reset(&mut self, remains: Vec<DataBlock>) {
        self.accumulated_rows = remains.iter().map(|item| item.num_rows()).sum();
        self.accumulated_blocks = remains;
    }

    /// split or merge the DataBlock according to the configuration
    pub fn compact(&mut self, block: DataBlock) -> Result<Option<Vec<DataBlock>>> {
        let num_rows = block.num_rows();

        // For cases like stmt `insert into .. select ... from ...`, the blocks that feeded
        // are likely to be properly sized, i.e. exeactly `max_rows_per_block` rows per block,
        // In that cases, just return them.
        if num_rows == self.max_rows_per_block {
            return Ok(Some(vec![block]));
        }

        if num_rows + self.accumulated_rows < self.max_rows_per_block {
            self.accumulated_rows += num_rows;
            self.accumulated_blocks.push(block);
            Ok(None)
        } else {
            let mut blocks = std::mem::take(&mut self.accumulated_blocks);
            blocks.push(block);
            let merged = DataBlock::concat_blocks(&blocks)?;
            let blocks = DataBlock::split_block_by_size(&merged, self.max_rows_per_block)?;

            let (result, remains) = blocks
                .into_iter()
                .partition(|item| item.num_rows() >= self.max_rows_per_block);
            self.reset(remains);
            Ok(Some(result))
        }
    }

    /// Pack the remainders into a DataBlock
    pub fn finish(self) -> Result<Option<Vec<DataBlock>>> {
        let remains = self.accumulated_blocks;
        Ok(if remains.is_empty() {
            None
        } else {
            Some(vec![DataBlock::concat_blocks(&remains)?])
        })
    }
}

#[async_trait::async_trait]
impl Compactor<DataBlock, Vec<DataBlock>> for BlockCompactor {
    async fn compact(&mut self, block: DataBlock) -> Result<Option<Vec<DataBlock>>> {
        BlockCompactor::compact(self, block)
    }

    fn finish(self) -> Result<Option<Vec<DataBlock>>> {
        BlockCompactor::finish(self)
    }
}
