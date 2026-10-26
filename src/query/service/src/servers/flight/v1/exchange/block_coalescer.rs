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

//! Coalesces the small blocks a hash shuffle receiver gets from many senders.

use std::collections::VecDeque;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;

/// Target size of the blocks a receiver outputs.
#[derive(Clone, Copy, Debug)]
pub struct CoalesceThreshold {
    pub rows: usize,
    pub bytes: usize,
}

impl CoalesceThreshold {
    pub fn create(rows: usize, bytes: usize) -> CoalesceThreshold {
        CoalesceThreshold {
            rows: if rows == 0 { usize::MAX } else { rows },
            bytes: if bytes == 0 { usize::MAX } else { bytes },
        }
    }
}

/// Concatenates small blocks up to a threshold. Blocks of at least half the
/// threshold, and blocks carrying meta, are passed through without copying.
pub(super) struct BlockCoalescer {
    threshold: Option<CoalesceThreshold>,
    blocks: Vec<DataBlock>,
    rows: usize,
    bytes: usize,
    ready: VecDeque<DataBlock>,
}

impl BlockCoalescer {
    pub(super) fn create(threshold: Option<CoalesceThreshold>) -> BlockCoalescer {
        BlockCoalescer {
            threshold,
            blocks: vec![],
            rows: 0,
            bytes: 0,
            ready: VecDeque::new(),
        }
    }

    /// Rows and bytes buffered and not yet moved to the output queue.
    pub(super) fn buffered(&self) -> (usize, usize) {
        (self.rows, self.bytes)
    }

    pub(super) fn push(&mut self, block: DataBlock) -> Result<()> {
        let Some(threshold) = self.threshold else {
            self.ready.push_back(block);
            return Ok(());
        };

        if block.get_meta().is_some() || block.num_columns() == 0 {
            self.flush()?;
            self.ready.push_back(block);
            return Ok(());
        }

        if block.is_empty() {
            return Ok(());
        }

        let rows = block.num_rows();
        let bytes = block.memory_size();

        if rows.saturating_mul(2) >= threshold.rows || bytes.saturating_mul(2) >= threshold.bytes {
            self.flush()?;
            self.ready.push_back(block);
            return Ok(());
        }

        self.rows += rows;
        self.bytes += bytes;
        self.blocks.push(block);

        if self.rows >= threshold.rows || self.bytes >= threshold.bytes {
            self.flush()?;
        }

        Ok(())
    }

    /// Move the buffered blocks to the output queue. Returns whether any were moved.
    pub(super) fn flush(&mut self) -> Result<bool> {
        if self.blocks.is_empty() {
            return Ok(false);
        }

        let blocks = std::mem::take(&mut self.blocks);
        self.rows = 0;
        self.bytes = 0;

        self.ready.push_back(match blocks.len() {
            1 => blocks.into_iter().next().unwrap(),
            _ => DataBlock::concat(&blocks)?,
        });
        Ok(true)
    }

    pub(super) fn pop(&mut self) -> Option<DataBlock> {
        self.ready.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int32Type;

    use super::*;

    fn block(rows: usize) -> DataBlock {
        DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1i32; rows])])
    }

    fn drain(coalescer: &mut BlockCoalescer) -> Vec<usize> {
        std::iter::from_fn(|| coalescer.pop())
            .map(|x| x.num_rows())
            .collect()
    }

    #[test]
    fn test_coalescer_merges_small_blocks() -> Result<()> {
        let mut coalescer =
            BlockCoalescer::create(Some(CoalesceThreshold::create(100, usize::MAX)));

        for _ in 0..7 {
            coalescer.push(block(30))?;
        }
        assert_eq!(drain(&mut coalescer), vec![120]);

        assert!(coalescer.flush()?);
        assert_eq!(drain(&mut coalescer), vec![90]);
        assert!(!coalescer.flush()?);
        Ok(())
    }

    #[test]
    fn test_coalescer_passes_large_blocks_through() -> Result<()> {
        let mut coalescer =
            BlockCoalescer::create(Some(CoalesceThreshold::create(100, usize::MAX)));

        coalescer.push(block(10))?;
        coalescer.push(block(50))?;
        coalescer.push(block(0))?;
        coalescer.push(block(10))?;
        coalescer.flush()?;
        assert_eq!(drain(&mut coalescer), vec![10, 50, 10]);
        Ok(())
    }
}
