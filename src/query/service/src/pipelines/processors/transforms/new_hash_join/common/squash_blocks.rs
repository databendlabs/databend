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
use databend_common_expression::DataBlock;

struct BlockWithInfo {
    avg_bytes: usize,
    block: DataBlock,
}

impl BlockWithInfo {
    pub fn new(block: DataBlock) -> BlockWithInfo {
        let avg_bytes = match block.num_rows() {
            0 => 0,
            _ => block.memory_size().div_ceil(block.num_rows()),
        };
        BlockWithInfo { avg_bytes, block }
    }
}

#[derive(Default)]
pub struct SquashBlocks {
    current_rows: usize,
    current_bytes: usize,
    squash_rows: usize,
    squash_bytes: usize,
    blocks: VecDeque<BlockWithInfo>,
}

impl SquashBlocks {
    pub fn new(squash_rows: usize, squash_bytes: usize) -> SquashBlocks {
        SquashBlocks {
            squash_rows,
            squash_bytes,
            current_rows: 0,
            current_bytes: 0,
            blocks: VecDeque::new(),
        }
    }

    pub fn add_block(&mut self, block: DataBlock) -> Result<Option<DataBlock>> {
        if block.is_empty() {
            return Ok(None);
        }

        let up_rows_bound = self.squash_rows * 2 / 3 * 2;
        let up_bytes_bound = self.squash_bytes * 2 / 3 * 2;

        if block.num_rows() >= self.squash_rows && block.num_rows() < up_rows_bound {
            return Ok(Some(block));
        }

        if block.memory_size() >= self.squash_bytes && block.memory_size() < up_bytes_bound {
            return Ok(Some(block));
        }

        self.current_rows += block.num_rows();
        self.current_bytes += block.memory_size();
        self.blocks.push_back(BlockWithInfo::new(block));

        if self.current_rows >= up_rows_bound || self.current_bytes >= up_bytes_bound {
            return Ok(Some(self.squash_blocks()?));
        }

        Ok(None)
    }

    pub fn finalize(&mut self) -> Result<Option<DataBlock>> {
        let mut blocks = Vec::with_capacity(self.blocks.len());
        for block in std::mem::take(&mut self.blocks) {
            blocks.push(block.block);
        }

        match blocks.is_empty() {
            true => Ok(None),
            false => Ok(Some(DataBlock::concat(&blocks)?)),
        }
    }

    fn squash_blocks(&mut self) -> Result<DataBlock> {
        let mut blocks = vec![];

        let mut current_rows = 0;
        let mut current_bytes = 0;

        while let Some(mut block) = self.blocks.pop_front() {
            if block.block.is_empty() {
                continue;
            }

            self.current_rows -= block.block.num_rows();
            self.current_bytes -= block.block.memory_size();

            let mut slice_rows = block.block.num_rows();

            slice_rows = std::cmp::min(slice_rows, self.squash_rows - current_rows);

            let max_bytes_rows = match block.avg_bytes {
                0 => block.block.num_rows(),
                _ => self.squash_bytes.saturating_sub(current_bytes) / block.avg_bytes,
            };

            slice_rows = std::cmp::min(max_bytes_rows, slice_rows);

            if slice_rows != block.block.num_rows() {
                let compact_block = block.block.slice(0..slice_rows);
                let remain_block = block.block.slice(slice_rows..block.block.num_rows());

                if !compact_block.is_empty() {
                    blocks.push(compact_block);
                }

                if !remain_block.is_empty() {
                    let mut columns = Vec::with_capacity(block.block.num_columns());

                    for block_entry in remain_block.take_columns() {
                        let column = block_entry.to_column();
                        drop(block_entry);
                        columns.push(column.maybe_gc());
                    }

                    block.block = DataBlock::new_from_columns(columns);
                    self.current_rows += block.block.num_rows();
                    self.current_bytes += block.block.memory_size();
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
#[allow(dead_code)]
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
