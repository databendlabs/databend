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

pub struct BlockCompactCompactor {
    max_row_per_block: usize,
    min_row_per_block: usize,
}

impl BlockCompactCompactor {
    pub fn new(max_row_per_block: usize, min_row_per_block: usize) -> Self {
        BlockCompactCompactor {
            max_row_per_block,
            min_row_per_block,
        }
    }
}

impl Compactor for BlockCompactCompactor {
    fn name() -> &'static str {
        "BlockCompactTransform"
    }

    fn compact(&self, blocks: &Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        let mut res = Vec::with_capacity(blocks.len());
        let mut temp_blocks = vec![];
        let mut accumulated_rows = 0;

        for block in blocks.into_iter() {
            // Perfect block, no need to compact
            if block.num_rows() >= self.min_row_per_block
                && block.num_rows() <= self.max_row_per_block
            {
                res.push(block.clone());
            } else {
                let block = if block.num_rows() > self.max_row_per_block {
                    let b = block.slice(0, self.max_row_per_block);
                    res.push(b);
                    block.slice(
                        self.max_row_per_block,
                        block.num_rows() - self.max_row_per_block,
                    )
                } else {
                    block.clone()
                };

                accumulated_rows += block.num_rows();
                temp_blocks.push(block);

                while accumulated_rows >= self.min_row_per_block {
                    let block = DataBlock::concat_blocks(&temp_blocks)?;
                    res.push(block.slice(0, self.min_row_per_block));
                    accumulated_rows -= self.min_row_per_block;

                    temp_blocks.clear();
                    if accumulated_rows != 0 {
                        temp_blocks.push(block.slice(
                            self.max_row_per_block,
                            block.num_rows() - self.min_row_per_block,
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

pub type TransformBlockCompact = TransformCompact<BlockCompactCompactor>;
