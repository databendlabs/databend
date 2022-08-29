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

use common_datablocks::DataBlock;
use common_exception::Result;

use super::Compactor;
use super::TransformCompact;

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
}

impl Compactor for BlockCompactor {
    fn name() -> &'static str {
        "BlockCompactTransform"
    }

    fn use_partial_compact() -> bool {
        true
    }

    fn compact_partial(&self, blocks: &mut Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        if blocks.is_empty() {
            return Ok(vec![]);
        }

        let size = blocks.len();
        let mut res = Vec::with_capacity(size);
        let block = blocks[size - 1].clone();

        // perfect block
        if (block.num_rows() >= self.min_rows_per_block
            && block.num_rows() <= self.max_rows_per_block)
            || block.memory_size() >= self.max_bytes_per_block
        {
            res.push(block);
            blocks.remove(size - 1);
        } else {
            let accumulated_rows: usize = blocks.iter_mut().map(|b| b.num_rows()).sum();
            let accumulated_bytes: usize = blocks.iter_mut().map(|b| b.memory_size()).sum();

            let merged = DataBlock::concat_blocks(blocks)?;
            blocks.clear();

            // we can't use slice here, it did not deallocate memory
            if accumulated_rows >= self.max_rows_per_block
                || accumulated_bytes >= self.max_bytes_per_block
            {
                res.push(merged);
            } else {
                blocks.push(merged);
            }
        }

        Ok(res)
    }

    fn compact_final(&self, blocks: &[DataBlock]) -> Result<Vec<DataBlock>> {
        let mut res = Vec::with_capacity(blocks.len());
        let mut temp_blocks = vec![];
        let mut accumulated_rows = 0;

        for block in blocks.iter() {
            // Perfect block, no need to compact
            if block.num_rows() >= self.min_rows_per_block
                && block.num_rows() <= self.max_rows_per_block
            {
                res.push(block.clone());
            } else {
                let block = if block.num_rows() > self.max_rows_per_block {
                    let b = block.slice(0, self.max_rows_per_block);
                    res.push(b);
                    block.slice(
                        self.max_rows_per_block,
                        block.num_rows() - self.max_rows_per_block,
                    )
                } else {
                    block.clone()
                };

                accumulated_rows += block.num_rows();
                temp_blocks.push(block);

                while accumulated_rows >= self.max_rows_per_block {
                    let block = DataBlock::concat_blocks(&temp_blocks)?;
                    res.push(block.slice(0, self.max_rows_per_block));
                    accumulated_rows -= self.max_rows_per_block;

                    temp_blocks.clear();
                    if accumulated_rows != 0 {
                        temp_blocks.push(block.slice(
                            self.max_rows_per_block,
                            block.num_rows() - self.max_rows_per_block,
                        ));
                    }
                }
            }
        }

        if accumulated_rows != 0 {
            let block = DataBlock::concat_blocks(&temp_blocks)?;
            res.push(block);
        }

        Ok(res)
    }
}

pub type TransformBlockCompact = TransformCompact<BlockCompactor>;
