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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::hint::unlikely;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::DataBlock;
use databend_common_expression::local_block_meta_serde;

use crate::processors::BlockMetaTransform;
use crate::processors::UnknownMode;

pub enum BlockCompactMeta {
    Concat(Vec<DataBlock>),
    Split {
        blocks: Vec<DataBlock>,
        rows_per_block: usize,
    },
    NoChange(Vec<DataBlock>),
}

impl Debug for BlockCompactMeta {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("BlockCompactMeta").finish()
    }
}

local_block_meta_serde!(BlockCompactMeta);

#[typetag::serde(name = "block_compact")]
impl BlockMetaInfo for BlockCompactMeta {}

#[derive(Default)]
pub struct TransformCompactBlock {
    aborting: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl BlockMetaTransform<BlockCompactMeta> for TransformCompactBlock {
    const UNKNOWN_MODE: UnknownMode = UnknownMode::Pass;
    const NAME: &'static str = "TransformCompactBlock";

    fn transform(&mut self, meta: BlockCompactMeta) -> Result<Vec<DataBlock>> {
        if unlikely(self.aborting.load(Ordering::Relaxed)) {
            return Err(ErrorCode::aborting());
        }

        match meta {
            BlockCompactMeta::Concat(blocks) => Ok(vec![DataBlock::concat(&blocks)?]),
            BlockCompactMeta::Split {
                blocks,
                rows_per_block,
            } => Self::split_blocks(blocks, rows_per_block),
            BlockCompactMeta::NoChange(blocks) => Ok(blocks),
        }
    }

    fn interrupt(&self) {
        self.aborting.store(true, Ordering::Release);
    }
}

impl TransformCompactBlock {
    fn split_blocks(blocks: Vec<DataBlock>, rows_per_block: usize) -> Result<Vec<DataBlock>> {
        debug_assert!(!blocks.is_empty());
        if blocks.len() == 1 {
            return Ok(blocks[0].split_by_rows_if_needed_no_tail(rows_per_block));
        }

        let max_rows_per_block = (rows_per_block * 9).div_ceil(5);
        let mut total_rows: usize = blocks.iter().map(DataBlock::num_rows).sum();
        let mut blocks = blocks.into_iter();
        let mut current = blocks.next();
        let mut offset = 0;
        let mut output = Vec::new();

        // Mirror split_by_rows_if_needed_no_tail, but consume a sequence of blocks
        // while preserving their original order. Like the original helper, this
        // treats rows_per_block as a target and allows a slightly larger block to
        // avoid emitting a tiny tail block.
        while total_rows >= max_rows_per_block {
            let mut remain_rows = rows_per_block;
            let mut pieces = vec![];

            while remain_rows > 0 {
                let block = current.as_ref().ok_or_else(|| {
                    ErrorCode::Internal("not enough rows to split compact blocks")
                })?;
                let block_rows = block.num_rows() - offset;

                if block_rows <= remain_rows {
                    let block = current.take().unwrap();
                    remain_rows -= block_rows;
                    pieces.push(if offset == 0 {
                        block
                    } else {
                        block.slice(offset..block.num_rows())
                    });
                    current = blocks.next();
                    offset = 0;
                } else {
                    // Split the current block and keep the remainder for the next output block.
                    pieces.push(block.slice(offset..offset + remain_rows));
                    offset += remain_rows;
                    remain_rows = 0;
                }
            }

            output.push(DataBlock::concat(&pieces)?);
            total_rows -= rows_per_block;
        }

        if let Some(block) = current {
            // Emit the final tail block, which may be smaller than rows_per_block by design.
            let mut tail = Vec::new();
            tail.push(block.slice(offset..block.num_rows()));
            tail.extend(blocks);
            output.push(DataBlock::concat(&tail)?);
        }

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use databend_common_exception::Result;
    use databend_common_expression::FromData;
    use databend_common_expression::ScalarRef;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::number::NumberScalar;

    use super::*;

    fn block_with_range(start: i32, end: i32) -> DataBlock {
        DataBlock::new_from_columns(vec![Int32Type::from_data((start..end).collect::<Vec<_>>())])
    }

    fn block_values(block: &DataBlock) -> Vec<i32> {
        (0..block.num_rows())
            .map(|row| match block.get_by_offset(0).index(row).unwrap() {
                ScalarRef::Number(NumberScalar::Int32(value)) => value,
                value => panic!("unexpected scalar: {value:?}"),
            })
            .collect()
    }

    fn assert_split_matches_reference(blocks: Vec<DataBlock>, rows_per_block: usize) -> Result<()> {
        let actual = TransformCompactBlock::split_blocks(blocks.clone(), rows_per_block)?;
        let expected = DataBlock::concat(&blocks)?.split_by_rows_if_needed_no_tail(rows_per_block);

        assert_eq!(
            actual.iter().map(DataBlock::num_rows).collect::<Vec<_>>(),
            expected.iter().map(DataBlock::num_rows).collect::<Vec<_>>()
        );
        assert_eq!(
            actual.iter().map(block_values).collect::<Vec<_>>(),
            expected.iter().map(block_values).collect::<Vec<_>>()
        );
        Ok(())
    }

    #[test]
    fn test_split_blocks_matches_reference_across_block_boundaries() -> Result<()> {
        assert_split_matches_reference(
            vec![
                block_with_range(0, 2),
                block_with_range(2, 6),
                block_with_range(6, 10),
            ],
            3,
        )?;
        assert_split_matches_reference(
            vec![
                block_with_range(0, 1),
                block_with_range(1, 2),
                block_with_range(2, 3),
                block_with_range(3, 10),
            ],
            4,
        )?;
        assert_split_matches_reference(
            vec![
                block_with_range(0, 2),
                block_with_range(2, 4),
                block_with_range(4, 6),
                block_with_range(6, 8),
            ],
            5,
        )?;
        Ok(())
    }
}
