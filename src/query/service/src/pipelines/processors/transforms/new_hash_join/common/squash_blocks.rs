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

fn block_logical_bytes(block: &DataBlock) -> usize {
    block
        .columns()
        .iter()
        .map(|entry| entry.memory_size_with_options(true))
        .sum()
}

struct BlockWithInfo {
    avg_bytes: usize,
    bytes: usize,
    block: DataBlock,
}

impl BlockWithInfo {
    pub fn new(block: DataBlock) -> BlockWithInfo {
        let bytes = block_logical_bytes(&block);
        let avg_bytes = match block.num_rows() {
            0 => 0,
            rows => bytes.div_ceil(rows),
        };
        BlockWithInfo {
            avg_bytes,
            bytes,
            block,
        }
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

        let info = BlockWithInfo::new(block);

        if info.block.num_rows() >= self.squash_rows && info.block.num_rows() < up_rows_bound {
            return Ok(Some(info.block));
        }

        if info.bytes >= self.squash_bytes && info.bytes < up_bytes_bound {
            return Ok(Some(info.block));
        }

        self.current_rows += info.block.num_rows();
        self.current_bytes += info.bytes;
        self.blocks.push_back(info);

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

        while current_rows < self.squash_rows && current_bytes < self.squash_bytes {
            let Some(block) = self.blocks.pop_front() else {
                return DataBlock::concat(&blocks);
            };

            if block.block.is_empty() {
                continue;
            }

            self.current_rows -= block.block.num_rows();
            self.current_bytes -= block.bytes;

            let mut slice_rows = block.block.num_rows();
            slice_rows = std::cmp::min(slice_rows, self.squash_rows - current_rows);

            let max_bytes_rows = match block.avg_bytes {
                0 => block.block.num_rows(),
                _ => self.squash_bytes.saturating_sub(current_bytes) / block.avg_bytes,
            };

            slice_rows = std::cmp::min(max_bytes_rows, slice_rows);

            if slice_rows != block.block.num_rows() {
                // Both slices stay alive together in the join's build chunks, so avoid
                // calling `maybe_gc()` here (which would copy every slice without freeing
                // the shared backing buffer).
                let compact_block = block.block.slice(0..slice_rows);
                blocks.push(compact_block);

                let remain_block = block.block.slice(slice_rows..block.block.num_rows());
                if !remain_block.is_empty() {
                    let remain_info = BlockWithInfo::new(remain_block);
                    self.current_rows += remain_info.block.num_rows();
                    self.current_bytes += remain_info.bytes;
                    self.blocks.push_front(remain_info);
                }

                break;
            }

            current_rows += block.block.num_rows();
            current_bytes += block.bytes;
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

#[cfg(test)]
mod tests {
    use databend_common_expression::BlockEntry;
    use databend_common_expression::Column;
    use databend_common_expression::FromData;
    use databend_common_expression::types::ArrayColumn;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::NumberColumn;
    use databend_common_expression::types::StringType;

    use super::*;

    #[test]
    fn test_squash_blocks_split_keeps_sibling_slices_zero_copy() -> Result<()> {
        let total_rows = 6 * 4096;
        let ids = Int64Type::from_data((0..total_rows as i64).collect::<Vec<_>>());
        let strs = StringType::from_data(
            (0..total_rows)
                .map(|r| format!("row_{r:08}_00000000000000000000000000"))
                .collect::<Vec<_>>(),
        );
        let mut offsets = Vec::with_capacity(total_rows + 1);
        offsets.push(0u64);
        let mut vals = Vec::with_capacity(total_rows * 4);
        for r in 0..total_rows {
            for e in 0..4 {
                vals.push((r * 10 + e) as i64);
            }
            offsets.push(vals.len() as u64);
        }
        let arr = Column::Array(Box::new(ArrayColumn::new(
            Int64Type::from_data(vals),
            offsets.into(),
        )));

        let Column::Number(NumberColumn::Int64(orig_ids)) = &ids else {
            unreachable!()
        };
        let orig_ids_ptr = orig_ids.backing_ptr();
        let Column::String(orig_s) = &strs else {
            unreachable!()
        };
        let orig_str_ptr = orig_s.buffers_ptr();

        let block = DataBlock::new(
            vec![
                BlockEntry::Column(ids),
                BlockEntry::Column(strs),
                BlockEntry::Column(arr),
            ],
            total_rows,
        );

        let mut squasher = SquashBlocks::new(4096, 64 * 1024 * 1024);
        let mut out = Vec::new();
        if let Some(b) = squasher.add_block(block)? {
            out.push(b);
        }
        while let Some(b) = squasher.finalize()? {
            out.push(b);
        }

        let total_out_rows: usize = out.iter().map(DataBlock::num_rows).sum();
        assert_eq!(total_out_rows, total_rows);
        assert!(out.len() >= 2);

        // First emitted slice shares the parent buffers zero-copy instead of redundantly copying.
        let Column::Number(NumberColumn::Int64(first_ids)) =
            out[0].get_by_offset(0).as_column().unwrap()
        else {
            unreachable!()
        };
        let Column::String(first_s) = out[0].get_by_offset(1).as_column().unwrap() else {
            unreachable!()
        };
        assert_eq!(first_ids.backing_ptr(), orig_ids_ptr);
        assert_eq!(first_s.buffers_ptr(), orig_str_ptr);

        Ok(())
    }
}
