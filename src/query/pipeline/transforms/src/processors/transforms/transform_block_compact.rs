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

use crate::processors::Compactor;

pub struct BlockCompactor {
    thresholds: BlockThresholds,
    aborting: Arc<AtomicBool>,
    // call block.memory_size() only once.
    // we may no longer need it if we start using jsonb, otherwise it should be put in CompactorState
    accumulated_rows: usize,
    accumulated_bytes: usize,
}

impl BlockCompactor {
    pub fn new(thresholds: BlockThresholds) -> Self {
        BlockCompactor {
            thresholds,
            accumulated_rows: 0,
            accumulated_bytes: 0,
            aborting: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Compactor for BlockCompactor {
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
        let num_rows = blocks[size - 1].num_rows();
        let num_bytes = blocks[size - 1].memory_size();

        if !self.thresholds.check_for_compact(num_rows, num_bytes) {
            // holding slices of blocks to merge later may lead to oom, so
            // 1. we expect blocks from file formats are not slice.
            // 2. if block is split here, cut evenly and emit them at once.
            let rows_per_block = self.thresholds.calc_rows_per_block(num_bytes, num_rows);
            let block = blocks.pop().unwrap();
            res.extend(block.split_by_rows_if_needed_no_tail(rows_per_block));
        } else if self.thresholds.check_large_enough(num_rows, num_bytes) {
            // pass through the new data block just arrived
            let block = blocks.pop().unwrap();
            res.push(block);
        } else {
            let accumulated_rows_new = self.accumulated_rows + num_rows;
            let accumulated_bytes_new = self.accumulated_bytes + num_bytes;

            if self
                .thresholds
                .check_large_enough(accumulated_rows_new, accumulated_bytes_new)
            {
                // avoid call concat_blocks for each new block
                let merged = DataBlock::concat(blocks)?;
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
