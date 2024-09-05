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

use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;

use super::Compactor;

pub struct BlockCompactorWithoutSplit {
    thresholds: BlockThresholds,
    aborting: Arc<AtomicBool>,

    temp_blocks: Vec<DataBlock>,
}

impl BlockCompactorWithoutSplit {
    pub fn new(thresholds: BlockThresholds) -> Self {
        BlockCompactorWithoutSplit {
            thresholds,
            aborting: Arc::new(AtomicBool::new(false)),
            temp_blocks: vec![],
        }
    }
}

impl Compactor for BlockCompactorWithoutSplit {
    fn name() -> &'static str {
        "BlockCompactorWithoutSplit"
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

        let (accumulated_rows, accumulated_bytes) = blocks.iter().fold((0, 0), |(r, b), block| {
            (r + block.num_rows(), b + block.memory_size())
        });

        if !self
            .thresholds
            .check_large_enough(accumulated_rows, accumulated_bytes)
        {
            return Ok(vec![]);
        }

        let mut res = Vec::with_capacity(2);
        if self.temp_blocks.len() > 1 {
            res.push(DataBlock::concat(&self.temp_blocks)?);
            self.temp_blocks.clear();
        } else if !self.temp_blocks.is_empty() {
            res.append(&mut self.temp_blocks);
        }

        if !self
            .thresholds
            .check_for_compact(accumulated_rows, accumulated_bytes)
            && blocks.len() > 1
        {
            self.temp_blocks.push(blocks.pop().unwrap());
        } else {
            self.temp_blocks = blocks.drain(..).collect();
        }

        if blocks.len() > 1 {
            res.push(DataBlock::concat(blocks)?);
            blocks.clear();
        } else if !blocks.is_empty() {
            res.append(blocks);
        }

        Ok(res)
    }

    fn compact_final(&mut self, blocks: Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        let mut res = Vec::with_capacity(2);
        let (accumulated_rows, accumulated_bytes) = self
            .temp_blocks
            .iter()
            .chain(blocks.iter())
            .fold((0, 0), |(r, b), block| {
                (r + block.num_rows(), b + block.memory_size())
            });

        if self
            .thresholds
            .check_for_compact(accumulated_rows, accumulated_bytes)
        {
            self.temp_blocks.extend(blocks);
            res.push(DataBlock::concat(&self.temp_blocks)?);
        } else {
            if self.temp_blocks.len() > 1 {
                res.push(DataBlock::concat(&self.temp_blocks)?);
            } else {
                res = std::mem::take(&mut self.temp_blocks);
            }

            if blocks.len() > 1 {
                res.push(DataBlock::concat(&blocks)?);
            } else {
                res.extend(blocks);
            }
        }

        Ok(res)
    }
}
