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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;
use tokio::time::Instant;

use super::Compactor;

pub struct BlockCompactorForCopy {
    thresholds: BlockThresholds,
    aborting: Arc<AtomicBool>,
    // call block.memory_size() only once.
    // we may no longer need it if we start using jsonb, otherwise it should be put in CompactorState
    accumulated_rows: usize,
    accumulated_bytes: usize,
}

impl BlockCompactorForCopy {
    pub fn new(thresholds: BlockThresholds) -> Self {
        BlockCompactorForCopy {
            thresholds,
            accumulated_rows: 0,
            accumulated_bytes: 0,
            aborting: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Compactor for BlockCompactorForCopy {
    fn name() -> &'static str {
        "BlockCompactTransform"
    }

    fn use_partial_compact(&self) -> bool {
        true
    }

    fn interrupt(&self) {
        self.aborting.store(true, Ordering::Release);
    }

    fn compact_partial(&mut self, blocks: &mut Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        if blocks.is_empty() {
            return Ok(vec![]);
        }

        let size = blocks.len();
        let mut res = Vec::with_capacity(size);
        let block = blocks[size - 1].clone();

        let num_rows = block.num_rows();
        let num_bytes = block.memory_size();

        if num_rows > self.thresholds.max_rows_per_block
            || num_bytes > self.thresholds.max_bytes_per_block * 2
        {
            // holding slices of blocks to merge later may lead to oom, so
            // 1. we expect blocks from file formats are not slice.
            // 2. if block is split here, cut evenly and emit them at once.
            let by_size = num_bytes / self.thresholds.max_bytes_per_block;
            let by_rows = num_rows / self.thresholds.min_rows_per_block;
            let rows_per_block = if by_size > by_rows {
                num_rows / by_size
            } else {
                self.thresholds.min_rows_per_block
            };
            res.extend(block.split_by_rows_if_needed_no_tail(rows_per_block));
            blocks.remove(size - 1);
        } else if self.thresholds.check_large_enough(num_rows, num_bytes) {
            // pass through the new data block just arrived
            res.push(block);
            blocks.remove(size - 1);
        } else {
            let accumulated_rows_new = self.accumulated_rows + num_rows;
            let accumulated_bytes_new = self.accumulated_bytes + num_bytes;

            if self
                .thresholds
                .check_large_enough(accumulated_rows_new, accumulated_bytes_new)
            {
                let start = Instant::now();
                let n = blocks.len();
                // avoid call concat_blocks for each new block
                let merged = DataBlock::concat(blocks)?;
                log::info!(
                    "concat {} blocks ({} rows, {} bytes ) using {} secs",
                    n,
                    self.accumulated_rows,
                    self.accumulated_bytes,
                    start.elapsed().as_secs_f32()
                );
                blocks.clear();
                self.accumulated_rows = 0;
                self.accumulated_bytes = 0;
                res.push(merged);
            } else {
                self.accumulated_rows = accumulated_rows_new;
                self.accumulated_bytes = accumulated_bytes_new;
            }
        }

        Ok(res)
    }

    fn compact_final(&mut self, blocks: Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        let mut res = vec![];
        if self.accumulated_rows != 0 {
            if self.aborting.load(Ordering::Relaxed) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }

            let block = DataBlock::concat(&blocks)?;
            res.push(block);
        }

        Ok(res)
    }
}
