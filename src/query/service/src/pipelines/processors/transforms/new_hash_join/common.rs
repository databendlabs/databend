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

use std::collections::VecDeque;

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::pipelines::processors::transforms::new_hash_join::JoinParams;

pub struct PartialBlock {
    avg_bytes: usize,
    block: DataBlock,
}

impl PartialBlock {
    pub fn new(block: DataBlock) -> PartialBlock {
        let avg_bytes = block.memory_size().div_ceil(block.num_rows());
        PartialBlock { avg_bytes, block }
    }
}

pub struct PartialBlocks {
    pub current_rows: usize,
    pub current_bytes: usize,
    pub blocks: VecDeque<PartialBlock>,
}

impl PartialBlocks {
    pub fn new() -> PartialBlocks {
        PartialBlocks {
            current_rows: 0,
            current_bytes: 0,
            blocks: VecDeque::new(),
        }
    }

    pub fn add_block(&mut self, block: DataBlock) {
        self.current_rows += block.num_rows();
        self.current_bytes += block.memory_size();
        self.blocks.push_back(PartialBlock::new(block));
    }

    pub fn compact_block(&mut self, rows: usize, bytes: usize) -> Result<DataBlock> {
        // Only split when rows exceed 2× the two-thirds point.
        if self.current_rows >= rows * 2 / 3 * 2 {
            return self.compact_block_inner(rows, bytes);
        }

        if self.current_bytes >= bytes * 2 / 3 * 2 {
            return self.compact_block_inner(rows, bytes);
        }

        let blocks = std::mem::take(&mut self.blocks)
            .into_iter()
            .map(|block| block.block)
            .collect::<Vec<_>>();

        DataBlock::concat(&blocks)
    }

    fn compact_block_inner(&mut self, rows: usize, bytes: usize) -> Result<DataBlock> {
        let mut blocks = vec![];

        let mut current_rows = 0;
        let mut current_bytes = 0;

        while let Some(mut block) = self.blocks.pop_front() {
            if current_rows + block.block.num_rows() >= rows {
                let compact_block = block.block.slice(0..rows - current_rows);
                let remain_block = block
                    .block
                    .slice(rows - current_rows..block.block.num_rows());

                blocks.push(compact_block);

                if !remain_block.is_empty() {
                    block.block = remain_block;
                    self.blocks.push_front(block);
                }

                break;
            }

            if current_bytes + block.block.memory_size() >= bytes {
                let compact_bytes = bytes - current_bytes;
                let estimated_rows = compact_bytes / block.avg_bytes;

                let compact_block = block.block.slice(0..estimated_rows);
                let remain_block = block.block.slice(estimated_rows..block.block.num_rows());

                blocks.push(compact_block);

                if !remain_block.is_empty() {
                    block.block = remain_block;
                    self.blocks.push_front(block);
                }

                break;
            }

            current_rows += block.block.num_rows();
            current_bytes += block.block.memory_size();
            blocks.push(block.block);
        }

        DataBlock::concat(&blocks)
    }
}

/// Compact small blocks into larger blocks that meet the max_rows and max_bytes requirements
pub fn compact_blocks(
    blocks: impl IntoIterator<Item = DataBlock>,
    rows: usize,
    bytes: usize,
) -> Result<Vec<DataBlock>> {
    let mut compacted_blocks = Vec::new();
    let mut current_blocks = Vec::new();
    let mut current_rows = 0;
    let mut current_bytes = 0;

    for block in blocks {
        let block_rows = block.num_rows();
        let block_bytes = block.memory_size();

        // Check if adding this block would exceed the limits
        if (current_rows + block_rows >= rows) || (current_bytes + block_bytes >= bytes) {
            // If we have accumulated blocks, compact them
            if !current_blocks.is_empty() {
                let compacted = DataBlock::concat(&current_blocks)?;
                // Free memory quickly.
                current_blocks.clear();

                if !compacted.is_empty() {
                    compacted_blocks.push(compacted);
                }

                current_rows = 0;
                current_bytes = 0;
            }

            // If the current block itself meets the requirements, add it directly
            if block_rows >= rows || block_bytes >= bytes {
                compacted_blocks.push(block);
            } else {
                // Otherwise, start a new accumulation with this block
                current_blocks.push(block);
                current_rows = block_rows;
                current_bytes = block_bytes;
            }
        } else {
            // Add block to current accumulation
            current_blocks.push(block);
            current_rows += block_rows;
            current_bytes += block_bytes;
        }
    }

    // Handle remaining blocks
    if !current_blocks.is_empty() {
        let compacted = DataBlock::concat(&current_blocks)?;
        // Free memory quickly.
        current_blocks.clear();

        if !compacted.is_empty() {
            compacted_blocks.push(compacted);
        }
    }

    Ok(compacted_blocks)
}

pub fn build_join_keys(block: &DataBlock, params: &JoinParams) {
    let evaluator = Evaluator::new(block, &params.func_ctx, &BUILTIN_FUNCTIONS);

    let build_keys = &params.build_keys;
    let mut keys_entries: Vec<BlockEntry> = build_keys
        .iter()
        .map(|expr| {
            Ok(evaluator
                .run(expr)?
                .convert_to_full_column(expr.data_type(), block.num_rows())
                .into())
        })
        .collect::<Result<_>>()?;

}
